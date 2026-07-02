package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	"github.com/coroot/coroot-cluster-agent/metrics/dbtracker"
	"github.com/coroot/coroot-cluster-agent/schema"
	"github.com/coroot/logger"
)

type databaseTracker struct {
	*dbtracker.Tracker
	db               *sql.DB
	baseDSN          string
	maxTablesPerDB   int
	trackSchema      bool
	trackSizes       bool
	trackBloat       bool
	excludeDatabases map[string]bool
	logger           logger.Logger

	bloat          map[string]*dbBloat
	tableStats     map[string][]tableStatEntry
	vacuumProgress map[string][]vacuumProgressEntry
}

func newDatabaseTracker(db *sql.DB, baseDSN string, maxTablesPerDB int, trackSchema, trackSizes, trackBloat bool, excludeDatabases []string, logger logger.Logger) *databaseTracker {
	exclude := make(map[string]bool, len(excludeDatabases))
	for _, dbName := range excludeDatabases {
		exclude[dbName] = true
	}
	dt := &databaseTracker{
		db:               db,
		baseDSN:          baseDSN,
		maxTablesPerDB:   maxTablesPerDB,
		trackSchema:      trackSchema,
		trackSizes:       trackSizes,
		trackBloat:       trackBloat,
		excludeDatabases: exclude,
		logger:           logger,
	}
	dt.Tracker = dbtracker.NewTracker("postgresql", trackSchema, trackSizes, dt.collectSnapshot, logger)
	return dt
}

func (dt *databaseTracker) collectSnapshot(ctx context.Context) (schema.Snapshot, map[string]*dbtracker.DBSizeSnapshot, error) {
	databases, dbSizes, err := listDatabasesWithSizes(ctx, dt.db, dt.excludeDatabases)
	if err != nil {
		return nil, nil, fmt.Errorf("list databases: %w", err)
	}

	snapshot := schema.Snapshot{}
	bloat := map[string]*dbBloat{}
	tableStats := map[string][]tableStatEntry{}
	vacuumProgress := map[string][]vacuumProgressEntry{}
	for _, dbName := range databases {
		dsn := replaceDatabaseInDSN(dt.baseDSN, dbName)
		tables, b, stats, vac, err := dt.collectDatabase(ctx, dsn, dbName, snapshot)
		if err != nil {
			dt.logger.Warning("database tracking for", dbName+":", err)
			continue
		}
		if b != nil {
			bloat[dbName] = b
		}
		if len(stats) > 0 {
			tableStats[dbName] = stats
		}
		if len(vac) > 0 {
			vacuumProgress[dbName] = vac
		}
		if dt.trackSizes {
			if snap, ok := dbSizes[dbName]; ok {
				snap.Tables = tables
			} else {
				dbSizes[dbName] = &dbtracker.DBSizeSnapshot{Tables: tables}
			}
		}
	}
	dt.bloat = bloat
	dt.tableStats = tableStats
	dt.vacuumProgress = vacuumProgress
	return snapshot, dbSizes, nil
}

func (dt *databaseTracker) collectDatabase(ctx context.Context, dsn, dbName string, snapshot schema.Snapshot) ([]dbtracker.TableSizeEntry, *dbBloat, []tableStatEntry, []vacuumProgressEntry, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	var tableCount int
	if err := db.QueryRowContext(ctx, `
SELECT count(*)
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
    AND n.nspname NOT IN ('pg_catalog', 'information_schema')`).Scan(&tableCount); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("count tables: %w", err)
	}

	var bloat *dbBloat
	if dt.trackBloat {
		bloat = collectBloat(ctx, db, dt.logger)
	}
	// vacuum progress must be resolved here (per-database) — pg_stat_progress_vacuum.relid
	// only maps to a table name via pg_class in the database the vacuum runs in
	vacuumProgress := collectVacuumProgress(ctx, db, dt.logger)

	if dt.maxTablesPerDB > 0 && tableCount > dt.maxTablesPerDB {
		dt.logger.Warningf("database %s has %d tables (limit %d), skipping", dbName, tableCount, dt.maxTablesPerDB)
		return nil, bloat, nil, vacuumProgress, nil
	}

	if dt.trackSchema {
		columns, err := queryColumns(ctx, db)
		if err != nil {
			return nil, bloat, nil, vacuumProgress, fmt.Errorf("query columns: %w", err)
		}

		constraints, err := queryConstraints(ctx, db)
		if err != nil {
			return nil, bloat, nil, vacuumProgress, fmt.Errorf("query constraints: %w", err)
		}

		indexes, err := queryIndexes(ctx, db)
		if err != nil {
			return nil, bloat, nil, vacuumProgress, fmt.Errorf("query indexes: %w", err)
		}

		tables := map[schema.TableKey]bool{}
		for key := range columns {
			tables[key] = true
		}

		for key := range tables {
			ddl := buildDDL(key.Table, columns[key], constraints[key], indexes[key])
			snapshot[schema.TableKey{DB: dbName, Schema: key.Schema, Table: key.Table}] = ddl
		}
	}

	if !dt.trackSizes {
		return nil, bloat, nil, vacuumProgress, nil
	}

	tableStats := collectTableStats(ctx, db, dt.logger)

	tableSizes, err := queryTableSizes(ctx, db, dbName)
	if err != nil {
		dt.logger.Warning("query table sizes for", dbName+":", err)
	}
	return tableSizes, bloat, tableStats, vacuumProgress, nil
}

type columnInfo struct {
	Name     string
	DataType string
	Nullable bool
	Default  sql.NullString
}

type constraintInfo struct {
	Name       string
	Type       string
	Definition string
}

type indexInfo struct {
	Name string
	DDL  string
}

func queryTableSizes(ctx context.Context, db *sql.DB, dbName string) ([]dbtracker.TableSizeEntry, error) {
	rows, err := db.QueryContext(ctx, `
SELECT n.nspname, c.relname, pg_total_relation_size(c.oid)
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
    AND n.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY pg_total_relation_size(c.oid) DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []dbtracker.TableSizeEntry
	for rows.Next() {
		e := dbtracker.TableSizeEntry{TableKey: schema.TableKey{DB: dbName}}
		if err := rows.Scan(&e.Schema, &e.Table, &e.Size); err != nil {
			return nil, err
		}
		result = append(result, e)
	}
	return result, rows.Err()
}

func listDatabasesWithSizes(ctx context.Context, db *sql.DB, exclude map[string]bool) ([]string, map[string]*dbtracker.DBSizeSnapshot, error) {
	rows, err := db.QueryContext(ctx, `
SELECT datname, pg_database_size(datname) FROM pg_database WHERE NOT datistemplate AND datallowconn ORDER BY datname`)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var dbs []string
	sizes := map[string]*dbtracker.DBSizeSnapshot{}
	for rows.Next() {
		var name string
		var size float64
		if err := rows.Scan(&name, &size); err != nil {
			return nil, nil, err
		}
		if exclude[name] {
			continue
		}
		sizes[name] = &dbtracker.DBSizeSnapshot{DatabaseSize: size}
		dbs = append(dbs, name)
	}
	return dbs, sizes, rows.Err()
}

func queryColumns(ctx context.Context, db *sql.DB) (map[schema.TableKey][]columnInfo, error) {
	rows, err := db.QueryContext(ctx, `
SELECT
    n.nspname, c.relname, a.attname,
    pg_catalog.format_type(a.atttypid, a.atttypmod),
    NOT a.attnotnull,
    pg_get_expr(d.adbin, d.adrelid)
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
WHERE c.relkind = 'r'
    AND n.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY n.nspname, c.relname, a.attnum`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := map[schema.TableKey][]columnInfo{}
	for rows.Next() {
		var col columnInfo
		var key schema.TableKey
		if err := rows.Scan(&key.Schema, &key.Table, &col.Name, &col.DataType, &col.Nullable, &col.Default); err != nil {
			return nil, err
		}
		result[key] = append(result[key], col)
	}
	return result, rows.Err()
}

func queryConstraints(ctx context.Context, db *sql.DB) (map[schema.TableKey][]constraintInfo, error) {
	rows, err := db.QueryContext(ctx, `
SELECT
    n.nspname, c.relname, con.conname, con.contype,
    pg_get_constraintdef(con.oid)
FROM pg_constraint con
JOIN pg_class c ON c.oid = con.conrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY n.nspname, c.relname, con.contype, con.conname`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := map[schema.TableKey][]constraintInfo{}
	for rows.Next() {
		var key schema.TableKey
		var con constraintInfo
		if err := rows.Scan(&key.Schema, &key.Table, &con.Name, &con.Type, &con.Definition); err != nil {
			return nil, err
		}
		result[key] = append(result[key], con)
	}
	return result, rows.Err()
}

func queryIndexes(ctx context.Context, db *sql.DB) (map[schema.TableKey][]indexInfo, error) {
	rows, err := db.QueryContext(ctx, `
SELECT
    schemaname, tablename, indexname, indexdef
FROM pg_indexes
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
ORDER BY schemaname, tablename, indexname`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := map[schema.TableKey][]indexInfo{}
	for rows.Next() {
		var key schema.TableKey
		var idx indexInfo
		if err := rows.Scan(&key.Schema, &key.Table, &idx.Name, &idx.DDL); err != nil {
			return nil, err
		}
		result[key] = append(result[key], idx)
	}
	return result, rows.Err()
}

func buildDDL(tableName string, cols []columnInfo, cons []constraintInfo, idxs []indexInfo) string {
	var buf strings.Builder
	buf.WriteString("CREATE TABLE ")
	buf.WriteString(tableName)
	buf.WriteString(" (\n")

	// Columns
	for i, col := range cols {
		buf.WriteString("    ")
		buf.WriteString(col.Name)
		buf.WriteString(" ")
		buf.WriteString(col.DataType)
		if !col.Nullable {
			buf.WriteString(" NOT NULL")
		}
		if col.Default.Valid {
			buf.WriteString(" DEFAULT ")
			buf.WriteString(col.Default.String)
		}
		if i < len(cols)-1 || len(cons) > 0 {
			buf.WriteString(",")
		}
		buf.WriteString("\n")
	}

	// Constraints
	// Build a set of constraint-backing index names to exclude from standalone index output.
	pkIndexes := map[string]bool{}
	uqIndexes := map[string]bool{}
	for _, con := range cons {
		if con.Type == "p" {
			pkIndexes[con.Name] = true
		} else if con.Type == "u" {
			uqIndexes[con.Name] = true
		}
	}

	for i, con := range cons {
		buf.WriteString("    CONSTRAINT ")
		buf.WriteString(con.Name)
		buf.WriteString(" ")
		buf.WriteString(con.Definition)
		if i < len(cons)-1 {
			buf.WriteString(",")
		}
		buf.WriteString("\n")
	}

	buf.WriteString(");\n")

	// Indexes (excluding those backing PRIMARY KEY and UNIQUE constraints)
	for _, idx := range idxs {
		if pkIndexes[idx.Name] || uqIndexes[idx.Name] {
			continue
		}
		buf.WriteString("\n")
		buf.WriteString(idx.DDL)
		buf.WriteString(";\n")
	}

	return buf.String()
}

func replaceDatabaseInDSN(dsn, dbName string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return dsn
	}
	u.Path = "/" + dbName
	return u.String()
}
