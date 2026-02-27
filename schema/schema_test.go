package schema

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiff_FirstRun(t *testing.T) {
	curr := Snapshot{"mydb/public.users": "CREATE TABLE public.users (\n    id integer NOT NULL\n);\n"}
	changes := Diff(nil, curr)
	assert.Nil(t, changes)
}

func TestDiff_NoChanges(t *testing.T) {
	ddl := "CREATE TABLE public.users (\n    id integer NOT NULL\n);\n"
	prev := Snapshot{"mydb/public.users": ddl}
	curr := Snapshot{"mydb/public.users": ddl}
	changes := Diff(prev, curr)
	assert.Empty(t, changes)
}

func TestDiff_TableCreated(t *testing.T) {
	prev := Snapshot{}
	curr := Snapshot{"mydb/public.orders": "CREATE TABLE public.orders (\n    id integer NOT NULL\n);\n"}
	changes := Diff(prev, curr)
	require.Len(t, changes, 1)
	assert.Equal(t, "mydb", changes[0].Database)
	assert.Equal(t, "public.orders", changes[0].Table)
	assert.True(t, changes[0].IsCreate)
	assert.False(t, changes[0].IsDrop)
	assert.Contains(t, changes[0].Diff, "+CREATE TABLE public.orders")
}

func TestDiff_TableDropped(t *testing.T) {
	prev := Snapshot{"mydb/public.orders": "CREATE TABLE public.orders (\n    id integer NOT NULL\n);\n"}
	curr := Snapshot{}
	changes := Diff(prev, curr)
	require.Len(t, changes, 1)
	assert.Equal(t, "mydb", changes[0].Database)
	assert.Equal(t, "public.orders", changes[0].Table)
	assert.True(t, changes[0].IsDrop)
	assert.False(t, changes[0].IsCreate)
	assert.Contains(t, changes[0].Diff, "-CREATE TABLE public.orders")
}

func TestDiff_TableChanged(t *testing.T) {
	prev := Snapshot{"mydb/public.users": "CREATE TABLE public.users (\n    id integer NOT NULL\n);\n"}
	curr := Snapshot{"mydb/public.users": "CREATE TABLE public.users (\n    id integer NOT NULL,\n    name text\n);\n"}
	changes := Diff(prev, curr)
	require.Len(t, changes, 1)
	assert.Equal(t, "mydb", changes[0].Database)
	assert.Equal(t, "public.users", changes[0].Table)
	assert.False(t, changes[0].IsCreate)
	assert.False(t, changes[0].IsDrop)
	assert.Contains(t, changes[0].Diff, "-    id integer NOT NULL")
	assert.Contains(t, changes[0].Diff, "+    id integer NOT NULL,")
	assert.Contains(t, changes[0].Diff, "+    name text")
}

func TestDiff_MultipleChanges(t *testing.T) {
	prev := Snapshot{
		"mydb/public.users":  "CREATE TABLE public.users (\n    id integer NOT NULL\n);\n",
		"mydb/public.orders": "CREATE TABLE public.orders (\n    id integer NOT NULL\n);\n",
	}
	curr := Snapshot{
		"mydb/public.users":    "CREATE TABLE public.users (\n    id integer NOT NULL,\n    email text\n);\n",
		"mydb/public.products": "CREATE TABLE public.products (\n    id integer NOT NULL\n);\n",
	}
	changes := Diff(prev, curr)
	require.Len(t, changes, 3)

	// Sorted by key: orders (dropped), products (created), users (changed)
	assert.Equal(t, "public.orders", changes[0].Table)
	assert.True(t, changes[0].IsDrop)

	assert.Equal(t, "public.products", changes[1].Table)
	assert.True(t, changes[1].IsCreate)

	assert.Equal(t, "public.users", changes[2].Table)
	assert.False(t, changes[2].IsCreate)
	assert.False(t, changes[2].IsDrop)
}

func TestDiff_DiffFormat(t *testing.T) {
	prev := Snapshot{"db/public.t": "line1\nline2\nline3\n"}
	curr := Snapshot{"db/public.t": "line1\nmodified\nline3\n"}
	changes := Diff(prev, curr)
	require.Len(t, changes, 1)
	diff := changes[0].Diff
	assert.True(t, strings.HasPrefix(diff, "--- db/public.t\n+++ db/public.t\n"))
	assert.Contains(t, diff, "@@")
	assert.Contains(t, diff, "-line2")
	assert.Contains(t, diff, "+modified")
}
