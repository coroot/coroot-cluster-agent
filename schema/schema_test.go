package schema

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiff_FirstRun(t *testing.T) {
	curr := Snapshot{TableKey{DB: "mydb", Schema: "public", Table: "users"}: "CREATE TABLE users (\n    id integer NOT NULL\n);\n"}
	changes := Diff(nil, curr)
	assert.Nil(t, changes)
}

func TestDiff_NoChanges(t *testing.T) {
	ddl := "CREATE TABLE users (\n    id integer NOT NULL\n);\n"
	key := TableKey{DB: "mydb", Schema: "public", Table: "users"}
	prev := Snapshot{key: ddl}
	curr := Snapshot{key: ddl}
	changes := Diff(prev, curr)
	assert.Empty(t, changes)
}

func TestDiff_TableChanged(t *testing.T) {
	key := TableKey{DB: "mydb", Schema: "public", Table: "users"}
	prev := Snapshot{key: "CREATE TABLE users (\n    id integer NOT NULL\n);\n"}
	curr := Snapshot{key: "CREATE TABLE users (\n    id integer NOT NULL,\n    name text\n);\n"}
	changes := Diff(prev, curr)
	require.Len(t, changes, 1)
	assert.Equal(t, "mydb", changes[0].Database)
	assert.Equal(t, "public", changes[0].Schema)
	assert.Equal(t, "users", changes[0].Object)
	assert.Equal(t, ChangeTypeChanged, changes[0].Type)
	assert.Contains(t, changes[0].Diff, "-    id integer NOT NULL")
	assert.Contains(t, changes[0].Diff, "+    id integer NOT NULL,")
	assert.Contains(t, changes[0].Diff, "+    name text")
}

func TestDiff_NoSchema(t *testing.T) {
	key := TableKey{DB: "mydb", Table: "orders"}
	prev := Snapshot{key: "CREATE TABLE orders (\n    id integer NOT NULL\n);\n"}
	curr := Snapshot{key: "CREATE TABLE orders (\n    id integer NOT NULL,\n    total decimal\n);\n"}
	changes := Diff(prev, curr)
	require.Len(t, changes, 1)
	assert.Equal(t, "mydb", changes[0].Database)
	assert.Equal(t, "", changes[0].Schema)
	assert.Equal(t, "orders", changes[0].Object)
}

func TestDiff_MultipleChanges(t *testing.T) {
	prev := Snapshot{
		TableKey{DB: "mydb", Schema: "public", Table: "users"}:  "CREATE TABLE users (\n    id integer NOT NULL\n);\n",
		TableKey{DB: "mydb", Schema: "public", Table: "orders"}: "CREATE TABLE orders (\n    id integer NOT NULL\n);\n",
	}
	curr := Snapshot{
		TableKey{DB: "mydb", Schema: "public", Table: "users"}:    "CREATE TABLE users (\n    id integer NOT NULL,\n    email text\n);\n",
		TableKey{DB: "mydb", Schema: "public", Table: "products"}: "CREATE TABLE products (\n    id integer NOT NULL\n);\n",
	}
	changes := Diff(prev, curr)
	// Only the changed table (users), not created (products) or dropped (orders)
	require.Len(t, changes, 1)
	assert.Equal(t, "users", changes[0].Object)
	assert.Equal(t, ChangeTypeChanged, changes[0].Type)
}

func TestDiff_DiffFormat(t *testing.T) {
	key := TableKey{DB: "db", Schema: "public", Table: "t"}
	prev := Snapshot{key: "line1\nline2\nline3\n"}
	curr := Snapshot{key: "line1\nmodified\nline3\n"}
	changes := Diff(prev, curr)
	require.Len(t, changes, 1)
	diff := changes[0].Diff
	assert.True(t, strings.HasPrefix(diff, "--- db/public/t\n+++ db/public/t\n"))
	assert.Contains(t, diff, "@@")
	assert.Contains(t, diff, "-line2")
	assert.Contains(t, diff, "+modified")
}
