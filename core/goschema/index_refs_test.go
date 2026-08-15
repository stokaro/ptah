package goschema_test

import (
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
)

func TestResolveIndexTableNames(t *testing.T) {
	c := qt.New(t)
	tables := []goschema.Table{
		{StructName: "User", Schema: "public", Name: "users"},
		{StructName: "AuditUser", Schema: "audit", Name: "users"},
		{StructName: "Post", Schema: "content", Name: "posts"},
	}
	indexes := []goschema.Index{
		{StructName: "User", TableName: "posts"},
		{StructName: "User"},
		{TableName: "posts"},
		{TableName: "users"},
		{StructName: "Missing"},
		{StructName: "User", TableName: "content.posts"},
		{TableName: "publci.users"},
	}

	got := goschema.ResolveIndexTableNames(indexes, tables)

	c.Assert(got, qt.DeepEquals, []string{
		"content.posts",
		"public.users",
		"content.posts",
		"",
		"",
		"content.posts",
		"",
	})

	t.Run("indexes-only input retains explicit owner", func(t *testing.T) {
		c := qt.New(t)
		got := goschema.ResolveIndexTableNames(
			[]goschema.Index{{TableName: "external.users"}},
			nil,
		)
		c.Assert(got, qt.DeepEquals, []string{"external.users"})
	})
}

func TestResolveIndexTableNames_AmbiguousStructAssociations(t *testing.T) {
	c := qt.New(t)
	tables := []goschema.Table{
		{StructName: "User", Schema: "public", Name: "users"},
		{StructName: "User", Schema: "audit", Name: "users"},
	}
	indexes := []goschema.Index{
		{StructName: "User"},
		{StructName: "User", TableName: "users"},
		{StructName: "User", TableName: "public.users"},
		{StructName: "User", TableName: "audit.users"},
	}

	got := goschema.ResolveIndexTableNames(indexes, tables)

	c.Assert(got, qt.DeepEquals, []string{
		"",
		"",
		"public.users",
		"audit.users",
	})
}

func TestDeduplicate_PreservesSameIndexNameOnDifferentTables(t *testing.T) {
	c := qt.New(t)
	first := goschema.Index{
		StructName: "User",
		Name:       "idx_email",
		Fields:     []string{"email"},
	}
	second := goschema.Index{
		StructName: "AuditUser",
		Name:       "idx_email",
		Fields:     []string{"email"},
	}
	db := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Schema: "public", Name: "users"},
			{StructName: "AuditUser", Schema: "audit", Name: "users"},
		},
		Indexes: []goschema.Index{first, second, first, second},
	}

	goschema.Deduplicate(db)

	c.Assert(db.Indexes, qt.DeepEquals, []goschema.Index{first, second})
}

func TestDeduplicate_DoesNotCollideOnDotsInIdentityParts(t *testing.T) {
	c := qt.New(t)
	first := goschema.Index{
		StructName: "First",
		Name:       "b.c",
	}
	second := goschema.Index{
		StructName: "Second",
		Name:       "c",
	}
	db := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "First", Name: "a"},
			{StructName: "Second", Schema: "a", Name: "b"},
		},
		Indexes: []goschema.Index{first, second},
	}

	goschema.Deduplicate(db)

	c.Assert(db.Indexes, qt.DeepEquals, []goschema.Index{first, second})
}

func BenchmarkResolveIndexTableNames_LargeSchema(b *testing.B) {
	c := qt.New(b)
	const indexCount = 10_000

	tables := make([]goschema.Table, 0, indexCount)
	indexes := make([]goschema.Index, 0, indexCount)
	for index := range indexCount {
		name := "table_" + strconv.Itoa(index)
		tables = append(tables, goschema.Table{StructName: name, Name: name})
		indexes = append(indexes, goschema.Index{StructName: name, Name: "idx_value"})
	}

	b.ReportAllocs()
	b.ResetTimer()
	var owners []string
	for range b.N {
		owners = goschema.ResolveIndexTableNames(indexes, tables)
	}
	b.StopTimer()
	c.Assert(owners, qt.HasLen, indexCount)
}
