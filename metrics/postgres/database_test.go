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

	got := buildDDL("users", cols, cons, idxs)
	expected := `CREATE TABLE users (
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

func Test_settingsToText(t *testing.T) {
	settings := []Setting{
		{Name: "max_connections", RawValue: "100", Source: "configuration file", IsMetric: true},
		{Name: "shared_buffers", RawValue: "16384", Source: "configuration file", IsMetric: true},
		{Name: "timezone", RawValue: "UTC", Source: "default"},
		{Name: "log_min_duration_statement", RawValue: "1000", Source: "command line", IsMetric: true},
		{Name: "transaction_read_only", RawValue: "off", Source: "override"},
		{Name: "work_mem", RawValue: "8192", Source: "configuration file", IsMetric: true},
	}
	got := settingsToText(settings)
	expected := "max_connections = 100\nshared_buffers = 16384\ntimezone = UTC\nlog_min_duration_statement = 1000\nwork_mem = 8192\n"
	assert.Equal(t, expected, got)
}

func Test_buildDDL_NoConstraintsNoIndexes(t *testing.T) {
	cols := []columnInfo{
		{Name: "id", DataType: "integer", Nullable: false},
		{Name: "value", DataType: "text", Nullable: true},
	}

	got := buildDDL("simple", cols, nil, nil)
	expected := `CREATE TABLE simple (
    id integer NOT NULL,
    value text
);
`
	assert.Equal(t, expected, got)
}
