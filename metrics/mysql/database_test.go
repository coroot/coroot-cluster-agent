package mysql

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_buildDDL(t *testing.T) {
	cols := []columnInfo{
		{Name: "id", Type: "int", Nullable: false, Default: sql.NullString{}, Extra: "auto_increment"},
		{Name: "name", Type: "varchar(255)", Nullable: true, Default: sql.NullString{String: "'unnamed'", Valid: true}},
		{Name: "email", Type: "varchar(255)", Nullable: false, Default: sql.NullString{}},
		{Name: "org_id", Type: "int", Nullable: false, Default: sql.NullString{}},
	}
	idxs := []indexInfo{
		{Name: "PRIMARY", Unique: true, Columns: "id", IsPrimary: true},
		{Name: "name_idx", Unique: true, Columns: "name"},
		{Name: "fk_idx", Unique: false, Columns: "org_id"},
	}
	fks := []foreignKeyInfo{
		{Name: "fk_org", Columns: "org_id", RefSchema: "mydb", RefTable: "orgs", RefColumns: "id"},
	}

	got := buildDDL("mydb.users", cols, idxs, fks)
	expected := `CREATE TABLE mydb.users (
    id int NOT NULL auto_increment,
    name varchar(255) DEFAULT 'unnamed',
    email varchar(255) NOT NULL,
    org_id int NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY name_idx (name),
    KEY fk_idx (org_id),
    CONSTRAINT fk_org FOREIGN KEY (org_id) REFERENCES mydb.orgs(id)
);
`
	assert.Equal(t, expected, got)
}

func Test_buildDDL_NoIndexesNoFKs(t *testing.T) {
	cols := []columnInfo{
		{Name: "id", Type: "int", Nullable: false},
		{Name: "value", Type: "text", Nullable: true},
	}

	got := buildDDL("mydb.simple", cols, nil, nil)
	expected := `CREATE TABLE mydb.simple (
    id int NOT NULL,
    value text
);
`
	assert.Equal(t, expected, got)
}

func Test_buildDDL_PKOnly(t *testing.T) {
	cols := []columnInfo{
		{Name: "id", Type: "bigint", Nullable: false, Extra: "auto_increment"},
		{Name: "data", Type: "json", Nullable: true},
	}
	idxs := []indexInfo{
		{Name: "PRIMARY", Unique: true, Columns: "id", IsPrimary: true},
	}

	got := buildDDL("app.events", cols, idxs, nil)
	expected := `CREATE TABLE app.events (
    id bigint NOT NULL auto_increment,
    data json,
    PRIMARY KEY (id)
);
`
	assert.Equal(t, expected, got)
}
