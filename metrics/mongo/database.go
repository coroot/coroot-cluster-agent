package mongo

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/coroot/coroot-cluster-agent/metrics/dbtracker"
	"github.com/coroot/coroot-cluster-agent/schema"
	"github.com/coroot/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

var mongoSystemDBs = map[string]bool{"admin": true, "config": true, "local": true}

type databaseTracker struct {
	*dbtracker.Tracker
	client         *mongo.Client
	maxTablesPerDB int
	trackSchema    bool
	trackSizes     bool
	logger         logger.Logger
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
	client := dt.client
	if client == nil {
		return nil, nil, fmt.Errorf("no mongo client")
	}

	var listResult struct {
		Databases []struct {
			Name       string  `bson:"name"`
			SizeOnDisk float64 `bson:"sizeOnDisk"`
		} `bson:"databases"`
	}
	res := client.Database("admin").RunCommand(ctx, bson.D{{Key: "listDatabases", Value: 1}})
	if err := res.Decode(&listResult); err != nil {
		return nil, nil, fmt.Errorf("listDatabases: %w", err)
	}

	snapshot := schema.Snapshot{}
	dbSizes := map[string]*dbtracker.DBSizeSnapshot{}

	for _, db := range listResult.Databases {
		if mongoSystemDBs[db.Name] {
			continue
		}
		dbSizes[db.Name] = &dbtracker.DBSizeSnapshot{DatabaseSize: db.SizeOnDisk}

		database := client.Database(db.Name)
		collNames, err := database.ListCollectionNames(ctx, bson.D{{Key: "type", Value: "collection"}})
		if err != nil {
			dt.logger.Warning("list collections for", db.Name+":", err)
			continue
		}

		if dt.maxTablesPerDB > 0 && len(collNames) > dt.maxTablesPerDB {
			dt.logger.Warningf("database %s has %d collections (limit %d), skipping", db.Name, len(collNames), dt.maxTablesPerDB)
			continue
		}

		var tables []dbtracker.TableSizeEntry

		for _, collName := range collNames {
			if dt.trackSizes {
				size, err := collTotalSize(ctx, database, collName)
				if err != nil {
					dt.logger.Warning("collStats for", db.Name+"."+collName+":", err)
				} else {
					tables = append(tables, dbtracker.TableSizeEntry{
						TableKey: schema.TableKey{DB: db.Name, Table: collName},
						Size:     size,
					})
				}
			}

			if dt.trackSchema {
				text, err := indexSnapshot(ctx, database.Collection(collName))
				if err != nil {
					dt.logger.Warning("list indexes for", db.Name+"."+collName+":", err)
					continue
				}
				snapshot[schema.TableKey{DB: db.Name, Table: collName}] = text
			}
		}

		if dt.trackSizes {
			dbSizes[db.Name].Tables = tables
		}
	}

	return snapshot, dbSizes, nil
}

func collTotalSize(ctx context.Context, database *mongo.Database, collName string) (float64, error) {
	var stats struct {
		TotalSize float64 `bson:"totalSize"`
	}
	res := database.RunCommand(ctx, bson.D{{Key: "collStats", Value: collName}})
	if err := res.Decode(&stats); err != nil {
		return 0, err
	}
	return stats.TotalSize, nil
}

func indexSnapshot(ctx context.Context, coll *mongo.Collection) (string, error) {
	cursor, err := coll.Indexes().List(ctx)
	if err != nil {
		return "", err
	}
	defer cursor.Close(ctx)

	type indexEntry struct {
		Name string
		Keys string
	}
	var entries []indexEntry
	for cursor.Next(ctx) {
		var idx struct {
			Name string `bson:"name"`
			Key  bson.M `bson:"key"`
		}
		if err := cursor.Decode(&idx); err != nil {
			return "", err
		}
		fields := make([]string, 0, len(idx.Key))
		for k := range idx.Key {
			fields = append(fields, k)
		}
		sort.Strings(fields)
		var keyBuf strings.Builder
		keyBuf.WriteByte('{')
		for i, f := range fields {
			if i > 0 {
				keyBuf.WriteByte(',')
			}
			fmt.Fprintf(&keyBuf, "%q:%v", f, idx.Key[f])
		}
		keyBuf.WriteByte('}')
		entries = append(entries, indexEntry{Name: idx.Name, Keys: keyBuf.String()})
	}
	if err := cursor.Err(); err != nil {
		return "", err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })

	var buf strings.Builder
	for _, e := range entries {
		fmt.Fprintf(&buf, "INDEX %s: %s\n", e.Name, e.Keys)
	}
	return buf.String(), nil
}
