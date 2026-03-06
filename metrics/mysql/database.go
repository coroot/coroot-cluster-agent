package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/coroot/coroot-cluster-agent/metrics/dbtracker"
	"github.com/coroot/coroot-cluster-agent/schema"
	"github.com/coroot/logger"
)

type databaseTracker struct {
	*dbtracker.Tracker
	db               *sql.DB
	maxTablesPerDB   int
	trackSchema      bool
	trackSizes       bool
	excludeDatabases map[string]bool
	logger           logger.Logger
}

func newDatabaseTracker(db *sql.DB, maxTablesPerDB int, trackSchema, trackSizes bool, excludeDatabases []string, logger logger.Logger) *databaseTracker {
	exclude := make(map[string]bool, len(excludeDatabases))
	for _, dbName := range excludeDatabases {
		exclude[dbName] = true
	}
	dt := &databaseTracker{
		db:               db,
		maxTablesPerDB:   maxTablesPerDB,
		trackSchema:      trackSchema,
		trackSizes:       trackSizes,
		excludeDatabases: exclude,
		logger:           logger,
	}
	dt.Tracker = dbtracker.NewTracker("mysql", trackSchema, trackSizes, dt.collectSnapshot, logger)
	return dt
}

func (dt *databaseTracker) collectSnapshot(ctx context.Context) (schema.Snapshot, map[string]*dbtracker.DBSizeSnapshot, error) {
	tables, err := dt.queryTablesWithSizes(ctx, dt.db)
	if err != nil {
		return nil, nil, fmt.Errorf("query tables: %w", err)
	}

	type dbInfo struct {
		tableNames []string
		tables     []dbtracker.TableSizeEntry
		totalSize  float64
	}
	byDB := map[string]*dbInfo{}
	for _, t := range tables {
		info := byDB[t.DB]
		if info == nil {
			info = &dbInfo{}
			byDB[t.DB] = info
		}
		info.tableNames = append(info.tableNames, t.Table)
		info.tables = append(info.tables, t)
		info.totalSize += t.Size
	}

	dbSizes := map[string]*dbtracker.DBSizeSnapshot{}
	var validDBs []string
	for dbName, info := range byDB {
		if dt.maxTablesPerDB > 0 && len(info.tableNames) > dt.maxTablesPerDB {
			dt.logger.Warningf("database %s has %d tables (limit %d), skipping", dbName, len(info.tableNames), dt.maxTablesPerDB)
			continue
		}
		dbSizes[dbName] = &dbtracker.DBSizeSnapshot{DatabaseSize: info.totalSize}
		validDBs = append(validDBs, dbName)
	}

	snapshot := schema.Snapshot{}

	if dt.trackSchema && len(validDBs) > 0 {
		columns, err := dt.queryColumns(ctx, dt.db)
		if err != nil {
			return nil, nil, fmt.Errorf("query columns: %w", err)
		}
		indexes, err := dt.queryIndexes(ctx, dt.db)
		if err != nil {
			return nil, nil, fmt.Errorf("query indexes: %w", err)
		}
		foreignKeys, err := dt.queryForeignKeys(ctx, dt.db)
		if err != nil {
			return nil, nil, fmt.Errorf("query foreign keys: %w", err)
		}

		for _, dbName := range validDBs {
			info := byDB[dbName]
			for _, tableName := range info.tableNames {
				key := schema.TableKey{DB: dbName, Table: tableName}
				ddl := buildDDL(tableName, columns[key], indexes[key], foreignKeys[key])
				snapshot[key] = ddl
			}
		}
	}

	if dt.trackSizes {
		for _, dbName := range validDBs {
			info := byDB[dbName]
			dbSizes[dbName].Tables = info.tables
		}
	}

	return snapshot, dbSizes, nil
}

func (dt *databaseTracker) queryTablesWithSizes(ctx context.Context, db *sql.DB) ([]dbtracker.TableSizeEntry, error) {
	rows, err := db.QueryContext(ctx, `
SELECT table_schema, table_name, COALESCE(data_length + index_length, 0)
FROM information_schema.tables
WHERE table_type = 'BASE TABLE'
ORDER BY table_schema, table_name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []dbtracker.TableSizeEntry
	for rows.Next() {
		var dbName, tableName string
		var size float64
		if err := rows.Scan(&dbName, &tableName, &size); err != nil {
			return nil, err
		}
		if dt.excludeDatabases[dbName] {
			continue
		}
		result = append(result, dbtracker.TableSizeEntry{
			TableKey: schema.TableKey{DB: dbName, Table: tableName},
			Size:     size,
		})
	}
	return result, rows.Err()
}

type columnInfo struct {
	Name     string
	Type     string
	Nullable bool
	Default  sql.NullString
	Extra    string
}

type indexInfo struct {
	Name      string
	Unique    bool
	Columns   string
	IsPrimary bool
}

type foreignKeyInfo struct {
	Name       string
	Columns    string
	RefSchema  string
	RefTable   string
	RefColumns string
}

func (dt *databaseTracker) queryColumns(ctx context.Context, db *sql.DB) (map[schema.TableKey][]columnInfo, error) {
	rows, err := db.QueryContext(ctx, `
SELECT table_schema, table_name, column_name, column_type, is_nullable, column_default, extra
FROM information_schema.columns
ORDER BY table_schema, table_name, ordinal_position`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := map[schema.TableKey][]columnInfo{}
	for rows.Next() {
		var key schema.TableKey
		var colName, colType, nullable, extra string
		var colDefault sql.NullString
		if err := rows.Scan(&key.DB, &key.Table, &colName, &colType, &nullable, &colDefault, &extra); err != nil {
			return nil, err
		}
		if dt.excludeDatabases[key.DB] {
			continue
		}
		result[key] = append(result[key], columnInfo{
			Name:     colName,
			Type:     colType,
			Nullable: nullable == "YES",
			Default:  colDefault,
			Extra:    extra,
		})
	}
	return result, rows.Err()
}

func (dt *databaseTracker) queryIndexes(ctx context.Context, db *sql.DB) (map[schema.TableKey][]indexInfo, error) {
	rows, err := db.QueryContext(ctx, `
SELECT table_schema, table_name, index_name, non_unique,
       GROUP_CONCAT(column_name ORDER BY seq_in_index)
FROM information_schema.statistics
GROUP BY table_schema, table_name, index_name, non_unique
ORDER BY table_schema, table_name, index_name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := map[schema.TableKey][]indexInfo{}
	for rows.Next() {
		var key schema.TableKey
		var idxName, columns string
		var nonUnique int
		if err := rows.Scan(&key.DB, &key.Table, &idxName, &nonUnique, &columns); err != nil {
			return nil, err
		}
		if dt.excludeDatabases[key.DB] {
			continue
		}
		result[key] = append(result[key], indexInfo{
			Name:      idxName,
			Unique:    nonUnique == 0,
			Columns:   columns,
			IsPrimary: idxName == "PRIMARY",
		})
	}
	return result, rows.Err()
}

func (dt *databaseTracker) queryForeignKeys(ctx context.Context, db *sql.DB) (map[schema.TableKey][]foreignKeyInfo, error) {
	rows, err := db.QueryContext(ctx, `
SELECT constraint_schema, table_name, constraint_name,
       GROUP_CONCAT(column_name ORDER BY ordinal_position),
       referenced_table_schema, referenced_table_name,
       GROUP_CONCAT(referenced_column_name ORDER BY ordinal_position)
FROM information_schema.key_column_usage
WHERE referenced_table_name IS NOT NULL
GROUP BY constraint_schema, table_name, constraint_name,
         referenced_table_schema, referenced_table_name
ORDER BY constraint_schema, table_name, constraint_name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := map[schema.TableKey][]foreignKeyInfo{}
	for rows.Next() {
		var key schema.TableKey
		var constraintName, columns, refSchema, refTable, refColumns string
		if err := rows.Scan(&key.DB, &key.Table, &constraintName, &columns, &refSchema, &refTable, &refColumns); err != nil {
			return nil, err
		}
		if dt.excludeDatabases[key.DB] {
			continue
		}
		result[key] = append(result[key], foreignKeyInfo{
			Name:       constraintName,
			Columns:    columns,
			RefSchema:  refSchema,
			RefTable:   refTable,
			RefColumns: refColumns,
		})
	}
	return result, rows.Err()
}

func buildDDL(tableName string, cols []columnInfo, idxs []indexInfo, fks []foreignKeyInfo) string {
	var buf strings.Builder
	buf.WriteString("CREATE TABLE ")
	buf.WriteString(tableName)
	buf.WriteString(" (\n")

	var nonPKIndexes []indexInfo
	for _, idx := range idxs {
		if !idx.IsPrimary {
			nonPKIndexes = append(nonPKIndexes, idx)
		}
	}
	var pk *indexInfo
	for i := range idxs {
		if idxs[i].IsPrimary {
			pk = &idxs[i]
			break
		}
	}

	totalTrailing := 0
	if pk != nil {
		totalTrailing++
	}
	totalTrailing += len(nonPKIndexes) + len(fks)

	// Columns
	for i, col := range cols {
		buf.WriteString("    ")
		buf.WriteString(col.Name)
		buf.WriteString(" ")
		buf.WriteString(col.Type)
		if !col.Nullable {
			buf.WriteString(" NOT NULL")
		}
		if col.Default.Valid {
			buf.WriteString(" DEFAULT ")
			buf.WriteString(col.Default.String)
		}
		if col.Extra != "" {
			buf.WriteString(" ")
			buf.WriteString(col.Extra)
		}
		if i < len(cols)-1 || totalTrailing > 0 {
			buf.WriteString(",")
		}
		buf.WriteString("\n")
	}

	trailing := 0

	// PRIMARY KEY
	if pk != nil {
		trailing++
		buf.WriteString("    PRIMARY KEY (")
		buf.WriteString(pk.Columns)
		buf.WriteString(")")
		if trailing < totalTrailing {
			buf.WriteString(",")
		}
		buf.WriteString("\n")
	}

	// Non-PK indexes
	for _, idx := range nonPKIndexes {
		trailing++
		buf.WriteString("    ")
		if idx.Unique {
			buf.WriteString("UNIQUE KEY ")
		} else {
			buf.WriteString("KEY ")
		}
		buf.WriteString(idx.Name)
		buf.WriteString(" (")
		buf.WriteString(idx.Columns)
		buf.WriteString(")")
		if trailing < totalTrailing {
			buf.WriteString(",")
		}
		buf.WriteString("\n")
	}

	// Foreign keys
	for _, fk := range fks {
		trailing++
		buf.WriteString("    CONSTRAINT ")
		buf.WriteString(fk.Name)
		buf.WriteString(" FOREIGN KEY (")
		buf.WriteString(fk.Columns)
		buf.WriteString(") REFERENCES ")
		buf.WriteString(fk.RefSchema)
		buf.WriteString(".")
		buf.WriteString(fk.RefTable)
		buf.WriteString("(")
		buf.WriteString(fk.RefColumns)
		buf.WriteString(")")
		if trailing < totalTrailing {
			buf.WriteString(",")
		}
		buf.WriteString("\n")
	}

	buf.WriteString(");\n")
	return buf.String()
}
