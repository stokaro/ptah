package indexscope_test

import (
	"slices"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/indexscope"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

func TestNewResolver_HappyPath(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "User", Schema: "app", Name: "users"},
		},
		Indexes: []schemamodel.Index{
			{StructName: "User", Name: "idx_email", Fields: []string{"email"}},
		},
	}
	diff := &difftypes.SchemaDiff{
		IndexesAdded: []difftypes.IndexRef{
			{Name: "idx_email", TableName: "app.users"},
		},
	}

	resolver, err := indexscope.NewResolver("postgres", withDeclaredIndexes(diff, desired))
	c.Assert(err, qt.IsNil)
	index, err := resolver.Resolve(
		difftypes.IndexRef{Name: "idx_email", TableName: "app.users"},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(index.StructName, qt.Equals, "User")
}

func TestNewResolver_TargetIdentityCollisionRejected(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Indexes: []schemamodel.Index{
			{Name: "idx_shared", TableName: "orders"},
			{Name: "idx_shared", TableName: "orders"},
		},
	}
	diff := &difftypes.SchemaDiff{
		IndexesAdded: []difftypes.IndexRef{
			{Name: "idx_shared", TableName: "orders"},
		},
	}

	resolver, err := indexscope.NewResolver("mysql", withDeclaredIndexes(diff, desired))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(resolver, qt.IsNil)
}

func TestNewResolver_TargetIdentityCollisionRejectedWithoutAdditions(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Indexes: []schemamodel.Index{
			{Name: "idx_shared", TableName: "app.orders"},
			{Name: "idx_shared", TableName: "app.users"},
		},
	}

	resolver, err := indexscope.NewResolver(
		"postgres",
		withDeclaredIndexes(&difftypes.SchemaDiff{}, desired),
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(resolver, qt.IsNil)
}

func TestNewResolver_DefaultAndNamedSchemaIndexesAreIndependent(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "User", Name: "users"},
			{StructName: "AuditUser", Schema: "audit", Name: "users"},
		},
		Indexes: []schemamodel.Index{
			{StructName: "User", Name: "idx_email"},
			{StructName: "AuditUser", Name: "idx_email"},
		},
	}

	resolver, err := indexscope.NewResolver("postgres", withDeclaredIndexes(&difftypes.SchemaDiff{}, desired))

	c.Assert(err, qt.IsNil)
	defaultIndex, err := resolver.Resolve(difftypes.IndexRef{Name: "idx_email", TableName: "users"})
	c.Assert(err, qt.IsNil)
	c.Assert(defaultIndex.StructName, qt.Equals, "User")
	auditIndex, err := resolver.Resolve(difftypes.IndexRef{Name: "idx_email", TableName: "audit.users"})
	c.Assert(err, qt.IsNil)
	c.Assert(auditIndex.StructName, qt.Equals, "AuditUser")
}

func TestNewResolver_CaseInsensitiveTargetIdentityCollisionRejected(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: "mysql"},
		{name: "mariadb", dialect: "mariadb"},
		{name: "sqlite", dialect: "sqlite"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{
				Indexes: []schemamodel.Index{
					{Name: "IDX_Shared", TableName: "users"},
					{Name: "idx_shared", TableName: "users"},
				},
			}
			diff := &difftypes.SchemaDiff{
				IndexesAdded: []difftypes.IndexRef{
					{Name: "IDX_Shared", TableName: "users"},
				},
			}

			resolver, err := indexscope.NewResolver(test.dialect, withDeclaredIndexes(diff, desired))

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(resolver, qt.IsNil)
		})
	}
}

func TestNewResolver_CaseInsensitiveDiffCollisionRejected(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: "mysql"},
		{name: "mariadb", dialect: "mariadb"},
		{name: "sqlite", dialect: "sqlite"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{
				IndexesAdded: []difftypes.IndexRef{
					{Name: "IDX_Shared", TableName: "users"},
					{Name: "idx_shared", TableName: "users"},
				},
			}

			resolver, err := indexscope.NewResolver(test.dialect, diff)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(resolver, qt.IsNil)
		})
	}
}

func TestNewResolverWithSemantics_IncompleteCatalogSnapshotRejected(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForSQLServerCatalog("SQL_Latin1_General_CP1_CI_AS").
		WithResolvedNames([]identifier.ResolvedName{
			{Name: "dbo", Key: "dbo"},
			{Name: "users", Key: "users"},
		})
	diff := &difftypes.SchemaDiff{
		IndexesAdded: []difftypes.IndexRef{
			{Name: "idx_email", TableName: "dbo.users"},
		},
	}
	desired := &schemamodel.Database{
		Indexes: []schemamodel.Index{
			{Name: "idx_email", TableName: "dbo.users"},
		},
	}

	resolver, err := indexscope.NewResolverWithSemantics(
		"sqlserver",
		semantics,
		withDeclaredIndexes(diff, desired),
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(
		err,
		qt.ErrorMatches,
		`.*added index reference dbo\.users\.idx_email at position 0 is not covered by catalog identifier semantics`,
	)
	c.Assert(resolver, qt.IsNil)
}

func TestNewResolver_TargetTableResolution(t *testing.T) {
	tests := []struct {
		name   string
		tables []schemamodel.Table
		index  schemamodel.Index
		ref    difftypes.IndexRef
	}{
		{
			name: "struct association",
			tables: []schemamodel.Table{
				{StructName: "User", Schema: "app", Name: "users"},
			},
			index: schemamodel.Index{StructName: "User", Name: "idx_email"},
			ref:   difftypes.IndexRef{Name: "idx_email", TableName: "app.users"},
		},
		{
			name: "explicit struct table association",
			tables: []schemamodel.Table{
				{StructName: "User", Schema: "app", Name: "users"},
			},
			index: schemamodel.Index{StructName: "User", Name: "idx_email", TableName: "users"},
			ref:   difftypes.IndexRef{Name: "idx_email", TableName: "app.users"},
		},
		{
			name: "explicit unqualified struct table association",
			tables: []schemamodel.Table{
				{StructName: "User", Name: "users"},
			},
			index: schemamodel.Index{StructName: "User", Name: "idx_email", TableName: "users"},
			ref:   difftypes.IndexRef{Name: "idx_email", TableName: "users"},
		},
		{
			name: "unique plain table association",
			tables: []schemamodel.Table{
				{StructName: "User", Schema: "app", Name: "users"},
			},
			index: schemamodel.Index{Name: "idx_email", TableName: "users"},
			ref:   difftypes.IndexRef{Name: "idx_email", TableName: "app.users"},
		},
		{
			name:  "explicit table without table metadata",
			index: schemamodel.Index{Name: "idx_email", TableName: "users"},
			ref:   difftypes.IndexRef{Name: "idx_email", TableName: "users"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{
				Tables:  test.tables,
				Indexes: []schemamodel.Index{test.index},
			}
			diff := &difftypes.SchemaDiff{IndexesAdded: []difftypes.IndexRef{test.ref}}

			resolver, err := indexscope.NewResolver("mysql", withDeclaredIndexes(diff, desired))
			c.Assert(err, qt.IsNil)
			c.Assert(
				schemamodel.ResolveIndexTableNames(
					[]schemamodel.Index{test.index},
					test.tables,
				)[0],
				qt.Equals,
				test.ref.TableName,
			)
			got, err := resolver.Resolve(test.ref)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.index)
		})
	}
}

func TestNewResolver_UnknownQualifiedTableRejected(t *testing.T) {
	c := qt.New(t)
	index := schemamodel.Index{Name: "idx_email", TableName: "app.users"}
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Audit", Schema: "logs", Name: "events"},
		},
		Indexes: []schemamodel.Index{index},
	}
	diff := &difftypes.SchemaDiff{
		IndexesAdded: []difftypes.IndexRef{
			{Name: "idx_email", TableName: "app.users"},
		},
	}

	resolver, err := indexscope.NewResolver("mysql", withDeclaredIndexes(diff, desired))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(resolver, qt.IsNil)
}

func TestNewResolver_AmbiguousPlainTableRejected(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "AppUser", Schema: "app", Name: "users"},
			{StructName: "AuditUser", Schema: "audit", Name: "users"},
		},
		Indexes: []schemamodel.Index{
			{Name: "idx_email", TableName: "users"},
		},
	}
	diff := &difftypes.SchemaDiff{
		IndexesAdded: []difftypes.IndexRef{
			{Name: "idx_email", TableName: "app.users"},
		},
	}

	resolver, err := indexscope.NewResolver("mysql", withDeclaredIndexes(diff, desired))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(resolver, qt.IsNil)
}

func TestNewResolver_MalformedRemovalRejectedWithoutTarget(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		IndexesRemoved: []difftypes.IndexRef{{Name: "idx_users_email"}},
	}

	resolver, err := indexscope.NewResolver("postgres", diff)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(resolver, qt.IsNil)
}

func TestNewResolver_AdditionRequiresTargetIndex(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		IndexesAdded: []difftypes.IndexRef{
			{Name: "idx_users_email", TableName: "users"},
		},
	}

	resolver, err := indexscope.NewResolver("postgres", diff)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(resolver, qt.IsNil)
}

func TestIdentityKey_DialectCaseSemantics(t *testing.T) {
	c := qt.New(t)
	ref := difftypes.IndexRef{Name: "IDX_Users_Email", TableName: "Tenant.Users"}
	tests := []struct {
		name    string
		dialect string
		want    difftypes.IndexRef
	}{
		{
			name:    "mysql folds only index name",
			dialect: "mysql",
			want:    difftypes.IndexRef{Name: "idx_users_email", TableName: "Tenant.Users"},
		},
		{
			name:    "mariadb folds only index name",
			dialect: "mariadb",
			want:    difftypes.IndexRef{Name: "idx_users_email", TableName: "Tenant.Users"},
		},
		{
			name:    "sqlite folds schema table and index",
			dialect: "sqlite",
			want:    difftypes.IndexRef{Name: "idx_users_email", TableName: "tenant.users"},
		},
		{
			name:    "postgres preserves quoted identity",
			dialect: "postgres",
			want:    ref,
		},
		{
			name:    "sqlserver preserves collation-dependent identity",
			dialect: "sqlserver",
			want:    ref,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := indexscope.IdentityKey(test.dialect, ref)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}

	c.Assert(ref, qt.DeepEquals, difftypes.IndexRef{
		Name:      "IDX_Users_Email",
		TableName: "Tenant.Users",
	})
}

func TestConflictSet_DialectMatching(t *testing.T) {
	candidate := difftypes.IndexRef{Name: "IDX_Shared", TableName: "Tenant.Users"}
	tests := []struct {
		name    string
		dialect string
		ref     difftypes.IndexRef
		want    bool
	}{
		{
			name:    "mysql index names are case insensitive per table",
			dialect: "mysql",
			ref:     difftypes.IndexRef{Name: "idx_shared", TableName: "Tenant.Users"},
			want:    true,
		},
		{
			name:    "mariadb index names are case insensitive per table",
			dialect: "mariadb",
			ref:     difftypes.IndexRef{Name: "idx_shared", TableName: "Tenant.Users"},
			want:    true,
		},
		{
			name:    "mysql keeps different tables independent",
			dialect: "mysql",
			ref:     difftypes.IndexRef{Name: "idx_shared", TableName: "Tenant.Orders"},
			want:    false,
		},
		{
			name:    "sqlite schema and index names are case insensitive",
			dialect: "sqlite",
			ref:     difftypes.IndexRef{Name: "idx_shared", TableName: "tenant.orders"},
			want:    true,
		},
		{
			name:    "postgres quoted names remain case sensitive",
			dialect: "postgres",
			ref:     difftypes.IndexRef{Name: "idx_shared", TableName: "Tenant.Orders"},
			want:    false,
		},
		{
			name:    "postgres same schema conflicts across tables",
			dialect: "postgres",
			ref:     difftypes.IndexRef{Name: "IDX_Shared", TableName: "Tenant.Orders"},
			want:    true,
		},
		{
			name:    "postgres different schemas are independent",
			dialect: "postgres",
			ref:     difftypes.IndexRef{Name: "IDX_Shared", TableName: "Audit.Orders"},
			want:    false,
		},
		{
			name:    "spanner same schema conflicts across tables",
			dialect: "spanner",
			ref:     difftypes.IndexRef{Name: "IDX_Shared", TableName: "Tenant.Orders"},
			want:    true,
		},
		{
			name:    "spanner different schemas are independent",
			dialect: "spanner",
			ref:     difftypes.IndexRef{Name: "IDX_Shared", TableName: "Audit.Orders"},
			want:    false,
		},
		{
			name:    "raw dotted index name remains one identifier",
			dialect: "postgres",
			ref:     difftypes.IndexRef{Name: "Tenant.IDX_Shared", TableName: "Orders"},
			want:    false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			set := indexscope.NewConflictSet(test.dialect, []difftypes.IndexRef{candidate})
			c.Assert(set.Contains(test.ref), qt.Equals, test.want)
		})
	}
}

func TestConflictSet_NonASCIICaseSemantics(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		stored  string
		lookup  string
		want    bool
	}{
		{
			name:    "mysql preserves non-ASCII case",
			dialect: "mysql",
			stored:  "Ä",
			lookup:  "ä",
			want:    false,
		},
		{
			name:    "mariadb folds Unicode case",
			dialect: "mariadb",
			stored:  "Ä",
			lookup:  "ä",
			want:    true,
		},
		{
			name:    "mariadb folds dotted capital I",
			dialect: "mariadb",
			stored:  "İ",
			lookup:  "i",
			want:    true,
		},
		{
			name:    "mariadb preserves accents",
			dialect: "mariadb",
			stored:  "a",
			lookup:  "ä",
			want:    false,
		},
		{
			name:    "sqlite preserves non-ASCII case",
			dialect: "sqlite",
			stored:  "Ä",
			lookup:  "ä",
			want:    false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			set := indexscope.NewConflictSet(test.dialect, []difftypes.IndexRef{
				{Name: test.stored, TableName: "users"},
			})

			c.Assert(
				set.Contains(difftypes.IndexRef{Name: test.lookup, TableName: "users"}),
				qt.Equals,
				test.want,
			)
		})
	}
}

func TestConflictSet_DefaultSchemaIsIndependentFromNamedSchemas(t *testing.T) {
	c := qt.New(t)
	refs := []difftypes.IndexRef{
		{Name: "idx_shared", TableName: "orders"},
		{Name: "idx_shared", TableName: "app.users"},
		{Name: "idx_shared", TableName: "logs.users"},
		{Name: "idx_other", TableName: "app.users"},
	}
	set := indexscope.NewConflictSet("postgres", refs)

	got := slices.Collect(set.Matches(difftypes.IndexRef{
		Name:      "idx_shared",
		TableName: "orders",
	}))

	c.Assert(got, qt.DeepEquals, refs[:1])
	c.Assert(
		slices.Collect(set.Matches(difftypes.IndexRef{
			Name:      "idx_shared",
			TableName: "app.orders",
		})),
		qt.DeepEquals,
		refs[1:2],
	)
	c.Assert(
		slices.Collect(set.Matches(difftypes.IndexRef{
			Name:      "idx_shared",
			TableName: "public.users",
		})),
		qt.DeepEquals,
		refs[:1],
	)
}

func TestIdentityKeyWithSemantics_SQLServerCatalogResolution(t *testing.T) {
	ref := difftypes.IndexRef{Name: "IDX_Email", TableName: "Users"}
	tests := []struct {
		name      string
		semantics identifier.Semantics
		want      difftypes.IndexRef
	}{
		{
			name: "case insensitive",
			semantics: resolvedCatalogSemantics(
				"CI",
				[]string{"dbo"},
				[]string{"users", "Users"},
				[]string{"idx_email", "IDX_Email"},
			),
			want: difftypes.IndexRef{Name: "IDX_Email", TableName: "dbo.Users"},
		},
		{
			name: "case sensitive",
			semantics: resolvedCatalogSemantics(
				"CS",
				[]string{"dbo"},
				[]string{"Users"},
				[]string{"users"},
				[]string{"IDX_Email"},
				[]string{"idx_email"},
			),
			want: difftypes.IndexRef{Name: "IDX_Email", TableName: "dbo.Users"},
		},
		{
			name:      "unknown remains exact identity",
			semantics: identifier.ForDialect("sqlserver"),
			want:      difftypes.IndexRef{Name: "IDX_Email", TableName: "dbo.Users"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := indexscope.IdentityKeyWithSemantics(test.semantics, ref)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

func TestConflictSetWithSemantics_SQLServerCatalogResolution(t *testing.T) {
	candidate := difftypes.IndexRef{Name: "IDX_Email", TableName: "Users"}
	lookup := difftypes.IndexRef{Name: "idx_email", TableName: "dbo.users"}
	tests := []struct {
		name      string
		semantics identifier.Semantics
		want      bool
	}{
		{
			name: "case insensitive conflicts",
			semantics: resolvedCatalogSemantics(
				"CI",
				[]string{"dbo"},
				[]string{"users", "Users"},
				[]string{"idx_email", "IDX_Email"},
			),
			want: true,
		},
		{
			name: "case sensitive is independent",
			semantics: resolvedCatalogSemantics(
				"CS",
				[]string{"dbo"},
				[]string{"Users"},
				[]string{"users"},
				[]string{"IDX_Email"},
				[]string{"idx_email"},
			),
			want: false,
		},
		{
			name:      "unknown conservatively conflicts",
			semantics: identifier.ForDialect("sqlserver"),
			want:      true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			set := indexscope.NewConflictSetWithSemantics(test.semantics, []difftypes.IndexRef{candidate})
			c.Assert(set.Contains(lookup), qt.Equals, test.want)
		})
	}
}

func TestNewResolverWithSemantics_SQLServerUnknownRejectsDistinctNames(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{Indexes: []schemamodel.Index{
		{Name: "resume", TableName: "dbo.users"},
		{Name: "r\u00e9sum\u00e9", TableName: "dbo.users"},
	}}
	diff := &difftypes.SchemaDiff{IndexesAdded: []difftypes.IndexRef{
		{Name: "resume", TableName: "dbo.users"},
		{Name: "r\u00e9sum\u00e9", TableName: "dbo.users"},
	}}

	_, err := indexscope.NewResolverWithSemantics(
		"sqlserver",
		identifier.ForDialect("sqlserver"),
		withDeclaredIndexes(diff, desired),
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(
		err.Error(),
		qt.Contains,
		"added indexes dbo.users.resume and dbo.users.r\u00e9sum\u00e9 conflict",
	)
}

func TestNewResolverWithSemantics_SQLServerCaseSensitiveAcceptsVariants(t *testing.T) {
	c := qt.New(t)
	semantics := resolvedCatalogSemantics(
		"CS",
		[]string{"dbo"},
		[]string{"users"},
		[]string{"idx_email"},
		[]string{"IDX_Email"},
	)
	desired := &schemamodel.Database{Indexes: []schemamodel.Index{
		{Name: "idx_email", TableName: "dbo.users"},
		{Name: "IDX_Email", TableName: "dbo.users"},
	}}
	diff := &difftypes.SchemaDiff{IndexesAdded: []difftypes.IndexRef{
		{Name: "idx_email", TableName: "dbo.users"},
		{Name: "IDX_Email", TableName: "dbo.users"},
	}}

	resolver, err := indexscope.NewResolverWithSemantics("sqlserver", semantics, withDeclaredIndexes(diff, desired))

	c.Assert(err, qt.IsNil)
	lower, err := resolver.Resolve(difftypes.IndexRef{Name: "idx_email", TableName: "users"})
	c.Assert(err, qt.IsNil)
	c.Assert(lower.Name, qt.Equals, "idx_email")
	upper, err := resolver.Resolve(difftypes.IndexRef{Name: "IDX_Email", TableName: "dbo.users"})
	c.Assert(err, qt.IsNil)
	c.Assert(upper.Name, qt.Equals, "IDX_Email")
}

func resolvedCatalogSemantics(
	collation string,
	groups ...[]string,
) identifier.Semantics {
	resolved := make([]identifier.ResolvedName, 0)
	for _, group := range groups {
		key := slices.Min(group)
		for _, name := range group {
			resolved = append(resolved, identifier.ResolvedName{
				Name: name,
				Key:  key,
			})
		}
	}
	return identifier.ForSQLServerCatalog(collation).WithResolvedNames(resolved)
}

func BenchmarkNewResolver_LargeDuplicateNameSchema(b *testing.B) {
	c := qt.New(b)
	const indexCount = 10_000

	tables := make([]schemamodel.Table, 0, indexCount)
	indexes := make([]schemamodel.Index, 0, indexCount)
	additions := make([]difftypes.IndexRef, 0, indexCount)
	for index := range indexCount {
		tableName := "table_" + strconv.Itoa(index)
		tables = append(tables, schemamodel.Table{Name: tableName, StructName: tableName})
		indexes = append(indexes, schemamodel.Index{
			StructName: tableName,
			Name:       "idx_shared",
			Fields:     []string{"value"},
		})
		additions = append(additions, difftypes.IndexRef{
			Name:      "idx_shared",
			TableName: tableName,
		})
	}
	desired := &schemamodel.Database{Tables: tables, Indexes: indexes}
	diff := &difftypes.SchemaDiff{IndexesAdded: additions}

	b.ReportAllocs()
	b.ResetTimer()
	var resolver *indexscope.Resolver
	var err error
	for range b.N {
		resolver, err = indexscope.NewResolver("mysql", withDeclaredIndexes(diff, desired))
	}
	b.StopTimer()
	c.Assert(err, qt.IsNil)
	c.Assert(resolver, qt.IsNotNil)
}

// withDeclaredIndexes fills the declared index/owner pairs a comparison
// carries, so a fixture states the schema once rather than the pairs
// (stokaro/ptah#2315).
func withDeclaredIndexes(diff *difftypes.SchemaDiff, desired *schemamodel.Database) *difftypes.SchemaDiff {
	if diff == nil {
		return diff
	}
	completed := *diff
	if len(completed.DeclaredIndexes) == 0 {
		completed.DeclaredIndexes = difftypes.IndexDeclarationsOf(desired)
	}
	return &completed
}
