package mongo

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/metrics/dbtracker"
	"github.com/coroot/coroot-cluster-agent/schema"
	"github.com/coroot/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	writtenCollectionsN = 50
	maxCollectionsN     = 500
)

var mongoSystemDBs = map[string]bool{"admin": true, "config": true, "local": true}

type databaseInfo struct {
	Name       string  `bson:"name"`
	SizeOnDisk float64 `bson:"sizeOnDisk"`
}

type databaseTracker struct {
	*dbtracker.Tracker
	client         *mongo.Client
	maxTablesPerDB int
	trackSchema    bool
	trackSizes     bool
	logger         logger.Logger

	prevWrites map[string]float64
	indexDefs  map[schema.TableKey][]schema.Index
}

func newDatabaseTracker(maxTablesPerDB int, trackSchema, trackSizes bool, logger logger.Logger) *databaseTracker {
	dt := &databaseTracker{
		maxTablesPerDB: maxTablesPerDB,
		trackSchema:    trackSchema,
		trackSizes:     trackSizes,
		logger:         logger,
	}
	dt.Tracker = dbtracker.NewTracker("mongodb", trackSchema, trackSizes, dt.collectSnapshot, logger)
	return dt
}

func (dt *databaseTracker) collectSnapshot(ctx context.Context) (schema.Snapshot, map[string]*dbtracker.DBSizeSnapshot, error) {
	if dt.client == nil {
		return nil, nil, fmt.Errorf("no mongo client")
	}

	var listResult struct {
		Databases []databaseInfo `bson:"databases"`
	}
	res := dt.client.Database("admin").RunCommand(ctx, bson.D{{Key: "listDatabases", Value: 1}})
	if err := res.Decode(&listResult); err != nil {
		return nil, nil, fmt.Errorf("listDatabases: %w", err)
	}
	dbSizes := map[string]*dbtracker.DBSizeSnapshot{}
	var databases []databaseInfo
	for _, db := range listResult.Databases {
		if mongoSystemDBs[db.Name] {
			continue
		}
		dbSizes[db.Name] = &dbtracker.DBSizeSnapshot{DatabaseSize: db.SizeOnDisk}
		databases = append(databases, db)
	}

	if dt.trackSizes {
		for _, t := range dt.collectionSizes(ctx, databases) {
			if snap := dbSizes[t.DB]; snap != nil {
				snap.Tables = append(snap.Tables, t)
			}
		}
	}

	var snapshot schema.Snapshot
	if dt.trackSchema {
		var err error
		if snapshot, err = dt.indexes(ctx); err != nil {
			dt.logger.Warning("failed to get index definitions:", err)
		}
		stats := map[schema.TableKey]schema.TableStats{}
		for _, snap := range dbSizes {
			for _, t := range snap.Tables {
				ts := schema.TableStats{}
				ts.SetInt(schema.AttrRows, int64(t.Documents))
				ts.SetInt(schema.AttrDataSize, int64(t.StorageSize))
				if indexesSize := t.Size - t.StorageSize; indexesSize > 0 {
					ts.SetInt(schema.AttrIndexesSize, int64(indexesSize))
				}
				stats[t.TableKey] = ts
			}
		}
		for key, defs := range dt.indexDefs {
			ts := stats[key]
			if ts == nil {
				ts = schema.TableStats{}
				stats[key] = ts
			}
			ts.SetIndexes(defs)
		}
		dt.TableStats = stats
	}
	return snapshot, dbSizes, nil
}

func (dt *databaseTracker) collectionSizes(ctx context.Context, databases []databaseInfo) []dbtracker.TableSizeEntry {
	var tables []dbtracker.TableSizeEntry
	seen := map[schema.TableKey]bool{}
	collect := func(key schema.TableKey) {
		if seen[key] || len(seen) >= maxCollectionsN || ctx.Err() != nil {
			return
		}
		seen[key] = true
		stats, err := collStorage(ctx, dt.client.Database(key.DB), key.Table)
		if err != nil {
			dt.logger.Warning("collStats for", key.DB+"."+key.Table+":", err)
			return
		}
		tables = append(tables, dbtracker.TableSizeEntry{
			TableKey:    key,
			Size:        stats.TotalSize,
			StorageSize: stats.StorageSize,
			FreeStorage: stats.FreeStorageSize,
			Documents:   stats.Count,
		})
	}

	written, err := dt.writtenCollections(ctx)
	if err != nil {
		dt.logger.Warning("top:", err)
	}
	for _, key := range written {
		collect(key)
	}

	sort.Slice(databases, func(i, j int) bool { return databases[i].SizeOnDisk > databases[j].SizeOnDisk })
	for _, db := range databases {
		if len(seen) >= maxCollectionsN || ctx.Err() != nil {
			break
		}
		names, err := dt.client.Database(db.Name).ListCollectionNames(ctx, bson.D{{Key: "type", Value: "collection"}}, options.ListCollections().SetAuthorizedCollections(true))
		if err != nil {
			dt.logger.Warning("list collections for", db.Name+":", err)
			continue
		}
		if dt.maxTablesPerDB > 0 && len(names) > dt.maxTablesPerDB {
			dt.logger.Warningf("database %s has %d collections (limit %d), skipping", db.Name, len(names), dt.maxTablesPerDB)
			continue
		}
		for _, name := range names {
			if !strings.HasPrefix(name, "system.") {
				collect(schema.TableKey{DB: db.Name, Table: name})
			}
		}
	}
	return tables
}

func (dt *databaseTracker) writtenCollections(ctx context.Context) ([]schema.TableKey, error) {
	var top struct {
		Totals map[string]bson.Raw `bson:"totals"`
	}
	if err := dt.client.Database("admin").RunCommand(ctx, bson.D{{Key: "top", Value: 1}}).Decode(&top); err != nil {
		return nil, err
	}
	type counter struct {
		Count float64 `bson:"count"`
	}
	type written struct {
		key    schema.TableKey
		writes float64
	}
	curr := map[string]float64{}
	var res []written
	for ns, raw := range top.Totals {
		db, coll, _ := strings.Cut(ns, ".")
		var c struct {
			Insert, Update, Remove counter
		}
		if mongoSystemDBs[db] || strings.HasPrefix(coll, "system.") || bson.Unmarshal(raw, &c) != nil {
			continue
		}
		curr[ns] = c.Insert.Count + c.Update.Count + c.Remove.Count
		if prev, ok := dt.prevWrites[ns]; ok && curr[ns] > prev {
			res = append(res, written{key: schema.TableKey{DB: db, Table: coll}, writes: curr[ns] - prev})
		}
	}
	dt.prevWrites = curr

	var keys []schema.TableKey
	for _, w := range common.TopN(res, writtenCollectionsN, func(a, b written) bool { return a.writes > b.writes }) {
		keys = append(keys, w.key)
	}
	return keys, nil
}

type collStorageStatsRaw struct {
	Size            float64 `bson:"size"`
	StorageSize     float64 `bson:"storageSize"`
	FreeStorageSize float64 `bson:"freeStorageSize"`
	TotalSize       float64 `bson:"totalSize"`
	Count           float64 `bson:"count"`
}

func collStorage(ctx context.Context, database *mongo.Database, collName string) (*collStorageStatsRaw, error) {
	cursor, err := database.Collection(collName).Aggregate(ctx, bson.A{
		bson.D{{Key: "$collStats", Value: bson.D{{Key: "storageStats", Value: bson.D{}}}}},
		bson.D{{Key: "$project", Value: bson.D{
			{Key: "storageStats.size", Value: 1},
			{Key: "storageStats.storageSize", Value: 1},
			{Key: "storageStats.freeStorageSize", Value: 1},
			{Key: "storageStats.totalSize", Value: 1},
			{Key: "storageStats.count", Value: 1},
		}}},
	})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var doc struct {
		StorageStats collStorageStatsRaw `bson:"storageStats"`
	}
	if !cursor.Next(ctx) {
		if err = cursor.Err(); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("no $collStats result")
	}
	if err = cursor.Decode(&doc); err != nil {
		return nil, err
	}
	return &doc.StorageStats, nil
}

func (dt *databaseTracker) indexes(ctx context.Context) (schema.Snapshot, error) {
	cursor, err := dt.client.Database("admin").Aggregate(ctx, bson.A{
		bson.D{{Key: "$listCatalog", Value: bson.D{}}},
		bson.D{{Key: "$match", Value: bson.D{{Key: "type", Value: "collection"}}}},
		bson.D{{Key: "$project", Value: bson.D{
			{Key: "db", Value: 1},
			{Key: "name", Value: 1},
			{Key: "md.indexes.ready", Value: 1},
			{Key: "md.indexes.spec.name", Value: 1},
			{Key: "md.indexes.spec.key", Value: 1},
			{Key: "md.indexes.spec.unique", Value: 1},
		}}},
	})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	snapshot := schema.Snapshot{}
	dt.indexDefs = map[schema.TableKey][]schema.Index{}
	for cursor.Next(ctx) {
		var coll struct {
			DB   string `bson:"db"`
			Name string `bson:"name"`
			MD   struct {
				Indexes []struct {
					Ready bool `bson:"ready"`
					Spec  struct {
						Name   string        `bson:"name"`
						Key    bson.D        `bson:"key"`
						Unique bson.RawValue `bson:"unique"`
					} `bson:"spec"`
				} `bson:"indexes"`
			} `bson:"md"`
		}
		if err = cursor.Decode(&coll); err != nil {
			return nil, err
		}
		if mongoSystemDBs[coll.DB] || strings.HasPrefix(coll.Name, "system.") {
			continue
		}
		var lines []string
		var defs []schema.Index
		for _, idx := range coll.MD.Indexes {
			if !idx.Ready {
				continue
			}
			fields := make([]string, 0, len(idx.Spec.Key))
			def := schema.Index{Name: idx.Spec.Name, Primary: idx.Spec.Name == "_id_", Unique: rawValueIsTrue(idx.Spec.Unique)}
			for _, e := range idx.Spec.Key {
				fields = append(fields, fmt.Sprintf("%q:%v", e.Key, e.Value))
				def.Columns = append(def.Columns, fmt.Sprintf("%s:%v", e.Key, e.Value))
			}
			sort.Strings(fields)
			lines = append(lines, fmt.Sprintf("INDEX %s: {%s}\n", idx.Spec.Name, strings.Join(fields, ",")))
			defs = append(defs, def)
		}
		sort.Strings(lines)
		key := schema.TableKey{DB: coll.DB, Table: coll.Name}
		snapshot[key] = strings.Join(lines, "")
		sort.Slice(defs, func(i, j int) bool { return defs[i].Name < defs[j].Name })
		dt.indexDefs[key] = defs
	}
	return snapshot, cursor.Err()
}

func rawValueIsTrue(v bson.RawValue) bool {
	switch v.Type {
	case bson.TypeBoolean:
		return v.Boolean()
	case bson.TypeInt32:
		return v.Int32() != 0
	case bson.TypeInt64:
		return v.Int64() != 0
	case bson.TypeDouble:
		return v.Double() != 0
	}
	return false
}
