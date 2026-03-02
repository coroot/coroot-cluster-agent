package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/coroot/coroot-cluster-agent/schema"
	"github.com/coroot/logger"
)

const schemaTrackMinInterval = 60 * time.Second

type changeEmitter interface {
	Emit(change schema.Change, dbSystem, targetAddr string)
}

type schemaTracker struct {
	baseDSN        string
	maxTablesPerDB int
	logger         logger.Logger
	prev           schema.Snapshot
	lastTracked    time.Time
}

func newSchemaTracker(baseDSN string, maxTablesPerDB int, logger logger.Logger) *schemaTracker {
	return &schemaTracker{
		baseDSN:        baseDSN,
		maxTablesPerDB: maxTablesPerDB,
		logger:         logger,
	}
}

func (st *schemaTracker) Track(ctx context.Context, mainDB *sql.DB, emitter changeEmitter, targetAddr string) {
	if time.Since(st.lastTracked) < schemaTrackMinInterval {
		return
	}
	st.lastTracked = time.Now()

	curr, err := st.collectSnapshot(ctx, mainDB)
	if err != nil {
		st.logger.Warning("schema tracking:", err)
		return
	}

	for _, c := range schema.Diff(st.prev, curr) {
		emitter.Emit(c, "postgresql", targetAddr)
	}
	st.prev = curr
}

func (st *schemaTracker) collectSnapshot(ctx context.Context, mainDB *sql.DB) (schema.Snapshot, error) {
	databases, err := listDatabases(ctx, mainDB)
	if err != nil {
		return nil, fmt.Errorf("list databases: %w", err)
	}

	snapshot := schema.Snapshot{}
	for _, dbName := range databases {
		dsn := replaceDatabaseInDSN(st.baseDSN, dbName)
		if err := st.collectDatabaseSchema(ctx, dsn, dbName, snapshot); err != nil {
			st.logger.Warning("schema tracking for", dbName+":", err)
		}
	}
	return snapshot, nil
}

func (st *schemaTracker) collectDatabaseSchema(ctx context.Context, dsn, dbName string, snapshot schema.Snapshot) error {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
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
		return fmt.Errorf("count tables: %w", err)
	}
	if st.maxTablesPerDB > 0 && tableCount > st.maxTablesPerDB {
		st.logger.Warningf("database %s has %d tables (limit %d), skipping schema tracking", dbName, tableCount, st.maxTablesPerDB)
		return nil
	}

	columns, err := queryColumns(ctx, db)
	if err != nil {
		return fmt.Errorf("query columns: %w", err)
	}

	constraints, err := queryConstraints(ctx, db)
	if err != nil {
		return fmt.Errorf("query constraints: %w", err)
	}

	indexes, err := queryIndexes(ctx, db)
	if err != nil {
		return fmt.Errorf("query indexes: %w", err)
	}

	tables := map[string]bool{}
	for key := range columns {
		tables[key] = true
	}

	for tableName := range tables {
		ddl := buildDDL(tableName, columns[tableName], constraints[tableName], indexes[tableName])
		snapshot[dbName+"/"+tableName] = ddl
	}
	return nil
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

func listDatabases(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT datname FROM pg_database WHERE NOT datistemplate AND datallowconn ORDER BY datname`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var dbs []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		dbs = append(dbs, name)
	}
	return dbs, rows.Err()
}

func queryColumns(ctx context.Context, db *sql.DB) (map[string][]columnInfo, error) {
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

	result := map[string][]columnInfo{}
	for rows.Next() {
		var schemaName, tableName string
		var col columnInfo
		if err := rows.Scan(&schemaName, &tableName, &col.Name, &col.DataType, &col.Nullable, &col.Default); err != nil {
			return nil, err
		}
		key := schemaName + "." + tableName
		result[key] = append(result[key], col)
	}
	return result, rows.Err()
}

func queryConstraints(ctx context.Context, db *sql.DB) (map[string][]constraintInfo, error) {
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

	result := map[string][]constraintInfo{}
	for rows.Next() {
		var schemaName, tableName string
		var con constraintInfo
		if err := rows.Scan(&schemaName, &tableName, &con.Name, &con.Type, &con.Definition); err != nil {
			return nil, err
		}
		key := schemaName + "." + tableName
		result[key] = append(result[key], con)
	}
	return result, rows.Err()
}

func queryIndexes(ctx context.Context, db *sql.DB) (map[string][]indexInfo, error) {
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

	// Collect PK index names to skip (they're covered by constraints).
	result := map[string][]indexInfo{}
	for rows.Next() {
		var schemaName, tableName, idxName, idxDef string
		if err := rows.Scan(&schemaName, &tableName, &idxName, &idxDef); err != nil {
			return nil, err
		}
		key := schemaName + "." + tableName
		result[key] = append(result[key], indexInfo{Name: idxName, DDL: idxDef})
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
