package postgres

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_replaceDatabaseInDSN(t *testing.T) {
	tests := []struct {
		dsn    string
		dbName string
		want   string
	}{
		{
			dsn:    "postgresql://user:pass@localhost:5432/postgres?sslmode=disable",
			dbName: "mydb",
			want:   "postgresql://user:pass@localhost:5432/mydb?sslmode=disable",
		},
		{
			dsn:    "postgresql://user:pass@host:5432/postgres?connect_timeout=1&statement_timeout=9000",
			dbName: "other",
			want:   "postgresql://user:pass@host:5432/other?connect_timeout=1&statement_timeout=9000",
		},
		{
			dsn:    "postgresql://user@localhost/template1",
			dbName: "app",
			want:   "postgresql://user@localhost/app",
		},
	}
	for _, tt := range tests {
		t.Run(tt.dbName, func(t *testing.T) {
			got := replaceDatabaseInDSN(tt.dsn, tt.dbName)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_buildDDL(t *testing.T) {
	cols := []columnInfo{
		{Name: "id", DataType: "integer", Nullable: false, Default: sql.NullString{}},
		{Name: "name", DataType: "character varying(255)", Nullable: true, Default: sql.NullString{String: "'unnamed'", Valid: true}},
		{Name: "email", DataType: "text", Nullable: false, Default: sql.NullString{}},
	}
	cons := []constraintInfo{
		{Name: "users_pkey", Type: "p", Definition: "PRIMARY KEY (id)"},
		{Name: "users_email_key", Type: "u", Definition: "UNIQUE (email)"},
	}
	idxs := []indexInfo{
		{Name: "users_pkey", DDL: "CREATE UNIQUE INDEX users_pkey ON public.users USING btree (id)"},
		{Name: "users_email_key", DDL: "CREATE UNIQUE INDEX users_email_key ON public.users USING btree (email)"},
		{Name: "idx_users_name", DDL: "CREATE INDEX idx_users_name ON public.users USING btree (name)"},
	}

	got := buildDDL("public.users", cols, cons, idxs)
	expected := `CREATE TABLE public.users (
    id integer NOT NULL,
    name character varying(255) DEFAULT 'unnamed',
    email text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id),
    CONSTRAINT users_email_key UNIQUE (email)
);

CREATE INDEX idx_users_name ON public.users USING btree (name);
`
	assert.Equal(t, expected, got)
}

func Test_buildDDL_NoConstraintsNoIndexes(t *testing.T) {
	cols := []columnInfo{
		{Name: "id", DataType: "integer", Nullable: false},
		{Name: "value", DataType: "text", Nullable: true},
	}

	got := buildDDL("public.simple", cols, nil, nil)
	expected := `CREATE TABLE public.simple (
    id integer NOT NULL,
    value text
);
`
	assert.Equal(t, expected, got)
}
