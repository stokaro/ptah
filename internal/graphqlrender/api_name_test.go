package graphqlrender_test

import (
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/graphqlrender"
)

func apiNameFixture(fields ...schemamodel.Field) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Invoice", Name: "invoices"}},
		Fields: fields,
	}
}

// The declared API name is what the SDL publishes, and the column name is not
// published alongside it (stokaro/ptah#905).
func TestRenderPublishesTheDeclaredAPIName(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(apiNameFixture(
		schemamodel.Field{StructName: "Invoice", Name: "id", Type: "BIGSERIAL", Primary: true},
		schemamodel.Field{
			StructName: "Invoice", Name: "billing_amount_minor",
			APIName: "amount", Type: "INTEGER",
		},
	), graphqlrender.Options{})
	c.Assert(err, qt.IsNil)

	sdl := string(res.Data)
	c.Assert(sdl, qt.Contains, "amount: Int!")
	c.Assert(sdl, qt.Not(qt.Contains), "billing_amount_minor")
}

// Sanitization runs on the API name exactly as it runs on a column name: an
// alias is an arbitrary annotation string too, and a GraphQL field name that is
// not a legal identifier fails to build.
//
// The diagnostic keeps naming the COLUMN. A warning about a name the reader
// cannot find in their schema source is a warning they cannot act on.
func TestRenderSanitizesAnIllegalAPIName(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(apiNameFixture(
		schemamodel.Field{StructName: "Invoice", Name: "id", Type: "BIGSERIAL", Primary: true},
		schemamodel.Field{
			StructName: "Invoice", Name: "billing_amount_minor",
			APIName: "amount-minor", Type: "INTEGER",
		},
	), graphqlrender.Options{})
	c.Assert(err, qt.IsNil)

	sdl := string(res.Data)
	c.Assert(sdl, qt.Contains, "amount_minor")
	c.Assert(sdl, qt.Not(qt.Contains), "amount-minor")
	c.Assert(diagnosticPaths(res), qt.Contains, "type Invoice.billing_amount_minor")
}

// diagnosticPaths collects the paths a render reported, so an assertion can name
// the one it expects instead of walking the slice in the test body.
func diagnosticPaths(res graphqlrender.Result) []string {
	paths := make([]string, 0, len(res.Diagnostics))
	for _, d := range res.Diagnostics {
		paths = append(paths, d.Path)
	}
	return paths
}

// A declared collision is refused rather than warned about. The existing
// warn-and-omit path is for names that only collide AFTER GraphQL sanitization,
// which is a naming-rules artifact; two columns explicitly published under one
// name is an authoring mistake, and dropping one of them silently is what the
// refusal exists to prevent.
func TestRenderRefusesAnAPINameCollision(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(apiNameFixture(
		schemamodel.Field{StructName: "Invoice", Name: "amount", Type: "INTEGER"},
		schemamodel.Field{
			StructName: "Invoice", Name: "billing_amount_minor",
			APIName: "amount", Type: "INTEGER",
		},
	), graphqlrender.Options{})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `exports two columns as "amount"`)
	c.Assert(res.Data, qt.HasLen, 0, qt.Commentf("nothing may be written on the refusing path"))
}

func TestRenderRefusesDeclaredFieldNameThatCollidesAfterNormalization(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(apiNameFixture(
		schemamodel.Field{StructName: "Invoice", Name: "amount-minor", Type: "INTEGER"},
		schemamodel.Field{
			StructName: "Invoice", Name: "billing_amount_minor", Type: "INTEGER",
			APINames: schemamodel.TargetNames{GraphQL: "amount_minor"},
		},
	), graphqlrender.Options{})

	c.Assert(err, qt.ErrorMatches,
		`columns "amount-minor" and "billing_amount_minor" on table "invoices" both produce GraphQL field name "amount_minor"; give one of them a distinct graphql_name`)
	c.Assert(res.Data, qt.HasLen, 0)
}

func TestRenderRefusesSharedFieldNameThatCollidesWithDerivedName(t *testing.T) {
	derived := schemamodel.Field{StructName: "Invoice", Name: "amount_minor", Type: "INTEGER"}
	authored := schemamodel.Field{
		StructName: "Invoice", Name: "billing_amount_minor", APIName: "amount-minor", Type: "INTEGER",
	}
	tests := []struct {
		name   string
		fields []schemamodel.Field
	}{
		{name: "derived first", fields: []schemamodel.Field{derived, authored}},
		{name: "authored first", fields: []schemamodel.Field{authored, derived}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			res, err := graphqlrender.Render(apiNameFixture(test.fields...), graphqlrender.Options{})
			c.Assert(err, qt.ErrorMatches,
				`columns "(amount_minor|billing_amount_minor)" and "(amount_minor|billing_amount_minor)" on table "invoices" both produce GraphQL field name "amount_minor"; give one of them a distinct graphql_name`)
			c.Assert(res.Data, qt.HasLen, 0)
		})
	}
}

// The GraphQL type name is derived from the table's API name, so a published
// `Invoice` survives the table underneath being renamed.
func TestRenderDerivesTheTypeNameFromTheTableAPIName(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(&schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Invoice", Name: "billing_invoices", APIName: "invoices"}},
		Fields: []schemamodel.Field{
			{StructName: "Invoice", Name: "id", Type: "BIGSERIAL", Primary: true},
		},
	}, graphqlrender.Options{})
	c.Assert(err, qt.IsNil)

	sdl := string(res.Data)
	// Singularized and PascalCased from the API name, exactly as it would be
	// from a table name.
	c.Assert(sdl, qt.Contains, "type Invoice {")
	c.Assert(sdl, qt.Not(qt.Contains), "BillingInvoice")
}

func TestRenderRefusesATableAPINameCollision(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(&schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Invoice", Name: "invoices"},
			{StructName: "Billing", Name: "billing_invoices", APIName: "invoices"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Invoice", Name: "id", Type: "BIGSERIAL", Primary: true},
			{StructName: "Billing", Name: "id", Type: "BIGSERIAL", Primary: true},
		},
	}, graphqlrender.Options{})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `two tables export as "invoices"`)
	c.Assert(res.Data, qt.HasLen, 0)
}

func TestRenderRefusesDeclaredTableNamesThatNormalizeToOneType(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(&schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Records", Name: "invoice_records", APINames: schemamodel.TargetNames{GraphQL: "invoice_records"}},
			{StructName: "Record", Name: "invoice_record", APINames: schemamodel.TargetNames{GraphQL: "invoice_record"}},
		},
		Fields: []schemamodel.Field{
			{StructName: "Records", Name: "id", Type: "BIGINT"},
			{StructName: "Record", Name: "id", Type: "BIGINT"},
		},
	}, graphqlrender.Options{})

	c.Assert(err, qt.ErrorMatches,
		`tables "invoice_records" and "invoice_record" both produce GraphQL type name "InvoiceRecord"; give one of them a distinct graphql_name`)
	c.Assert(res.Data, qt.HasLen, 0)
}

func TestRenderRefusesSharedTableNamesThatNormalizeToOneType(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(&schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Records", Name: "first", APIName: "invoice_records"},
			{StructName: "Record", Name: "second", APIName: "invoice_record"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Records", Name: "id", Type: "BIGINT"},
			{StructName: "Record", Name: "id", Type: "BIGINT"},
		},
	}, graphqlrender.Options{})

	c.Assert(err, qt.ErrorMatches,
		`tables "first" and "second" both produce GraphQL type name "InvoiceRecord"; give one of them a distinct graphql_name`)
	c.Assert(res.Data, qt.HasLen, 0)
}

func TestRenderRefusesSharedTableNameThatCollidesWithDerivedNameInEitherOrder(t *testing.T) {
	derived := schemamodel.Table{StructName: "Records", Name: "invoice_records"}
	authored := schemamodel.Table{StructName: "Alias", Name: "archive", APIName: "invoice_record"}
	tests := []struct {
		name   string
		tables []schemamodel.Table
	}{
		{name: "derived first", tables: []schemamodel.Table{derived, authored}},
		{name: "authored first", tables: []schemamodel.Table{authored, derived}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			res, err := graphqlrender.Render(&schemamodel.Database{
				Tables: test.tables,
				Fields: []schemamodel.Field{
					{StructName: "Records", Name: "id", Type: "BIGINT"},
					{StructName: "Alias", Name: "id", Type: "BIGINT"},
				},
			}, graphqlrender.Options{})
			c.Assert(err, qt.ErrorMatches,
				`tables "(invoice_records|archive)" and "(invoice_records|archive)" both produce GraphQL type name "InvoiceRecord"; give one of them a distinct graphql_name`)
			c.Assert(res.Data, qt.HasLen, 0)
		})
	}
}

func TestRenderRefusesSharedTableNamesThatShadowReservedTypes(t *testing.T) {
	tests := []struct {
		apiName string
		want    string
	}{
		{apiName: "query", want: "Query"},
		{apiName: "page_infos", want: "PageInfo"},
		{apiName: "string", want: "String"},
	}
	for _, test := range tests {
		t.Run(test.want, func(t *testing.T) {
			c := qt.New(t)
			res, err := graphqlrender.Render(&schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "Thing", Name: "things", APIName: test.apiName}},
				Fields: []schemamodel.Field{{StructName: "Thing", Name: "id", Type: "BIGINT"}},
			}, graphqlrender.Options{})
			c.Assert(err, qt.ErrorMatches, fmt.Sprintf(
				`table "things" declares api_name %q, which produces reserved GraphQL type name %q; choose a different graphql_name`,
				test.apiName,
				test.want,
			))
			c.Assert(res.Data, qt.HasLen, 0)
		})
	}
}

func TestRenderKeepsDistinctNamesForSameTableNameInTwoSchemas(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(&schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "AUser", Schema: "a", Name: "users", APINames: schemamodel.TargetNames{GraphQL: "a_users"}},
			{StructName: "BUser", Schema: "b", Name: "users", APINames: schemamodel.TargetNames{GraphQL: "b_users"}},
		},
		Fields: []schemamodel.Field{
			{StructName: "AUser", Name: "id", Type: "BIGINT"},
			{StructName: "BUser", Name: "id", Type: "BIGINT"},
		},
	}, graphqlrender.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(string(res.Data), qt.Contains, "type AUser {")
	c.Assert(string(res.Data), qt.Contains, "type BUser {")
}

func TestRenderUsesResolvedTableNamesForListQueries(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(&schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "AUser", Schema: "a", Name: "users", APINames: schemamodel.TargetNames{GraphQL: "a_users"}},
			{StructName: "BUser", Schema: "b", Name: "users", APINames: schemamodel.TargetNames{GraphQL: "b_users"}},
		},
		Fields: []schemamodel.Field{
			{StructName: "AUser", Name: "id", Type: "BIGINT"},
			{StructName: "BUser", Name: "id", Type: "BIGINT"},
		},
	}, graphqlrender.Options{Operations: graphqlrender.Operations{List: true}})

	c.Assert(err, qt.IsNil)
	c.Assert(string(res.Data), qt.Contains, "aUsers(first: Int, after: String): AUserConnection")
	c.Assert(string(res.Data), qt.Contains, "bUsers(first: Int, after: String): BUserConnection")
}

func TestRenderRefusesGeneratedOperationTypeCollisionsBeforeOutput(t *testing.T) {
	tests := []struct {
		name       string
		collision  string
		operations graphqlrender.Operations
	}{
		{name: "edge", collision: "user_edges", operations: graphqlrender.Operations{List: true}},
		{name: "connection", collision: "user_connections", operations: graphqlrender.Operations{List: true}},
		{name: "create input", collision: "user_create_inputs", operations: graphqlrender.Operations{CreateInput: true}},
		{name: "update input", collision: "user_update_inputs", operations: graphqlrender.Operations{UpdateInput: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			res, err := graphqlrender.Render(&schemamodel.Database{
				Tables: []schemamodel.Table{
					{StructName: "User", Name: "users", APINames: schemamodel.TargetNames{GraphQL: "users"}},
					{StructName: "Collision", Name: "collision", APINames: schemamodel.TargetNames{GraphQL: test.collision}},
				},
				Fields: []schemamodel.Field{
					{StructName: "User", Name: "name", Type: "TEXT"},
					{StructName: "Collision", Name: "id", Type: "BIGINT"},
				},
			}, graphqlrender.Options{Operations: test.operations})

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, "collides with another type")
			c.Assert(res.Data, qt.HasLen, 0)
		})
	}
}

func TestRenderRefusesGeneratedOperationTypeCollisionsWithOneAuthoredOwner(t *testing.T) {
	tests := []struct {
		name       string
		collision  string
		operations graphqlrender.Operations
	}{
		{name: "edge", collision: "user_edges", operations: graphqlrender.Operations{List: true}},
		{name: "connection", collision: "user_connections", operations: graphqlrender.Operations{List: true}},
		{name: "create input", collision: "user_create_inputs", operations: graphqlrender.Operations{CreateInput: true}},
		{name: "update input", collision: "user_update_inputs", operations: graphqlrender.Operations{UpdateInput: true}},
	}
	for _, test := range tests {
		owners := []struct {
			name      string
			user      schemamodel.Table
			collision schemamodel.Table
		}{
			{
				name: "generated type authored",
				user: schemamodel.Table{
					StructName: "User", Name: "users",
					APINames: schemamodel.TargetNames{GraphQL: "users"},
				},
				collision: schemamodel.Table{StructName: "Collision", Name: test.collision},
			},
			{
				name: "colliding type authored",
				user: schemamodel.Table{StructName: "User", Name: "users"},
				collision: schemamodel.Table{
					StructName: "Collision", Name: test.collision,
					APINames: schemamodel.TargetNames{GraphQL: test.collision},
				},
			},
		}
		for _, owner := range owners {
			t.Run(test.name+"/"+owner.name, func(t *testing.T) {
				c := qt.New(t)
				res, err := graphqlrender.Render(&schemamodel.Database{
					Tables: []schemamodel.Table{owner.user, owner.collision},
					Fields: []schemamodel.Field{
						{StructName: "User", Name: "name", Type: "TEXT"},
						{StructName: "Collision", Name: "id", Type: "BIGINT"},
					},
				}, graphqlrender.Options{Operations: test.operations})

				c.Assert(err, qt.IsNotNil)
				c.Assert(err.Error(), qt.Contains, "collides with another type")
				c.Assert(res.Data, qt.HasLen, 0)
			})
		}
	}
}

func TestRenderPreservesDerivedOnlyOperationCollisionBehavior(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(&schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "User", Name: "users"},
			{StructName: "UserEdge", Name: "user_edges"},
		},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "BIGINT"},
			{StructName: "UserEdge", Name: "id", Type: "BIGINT"},
		},
	}, graphqlrender.Options{Operations: graphqlrender.Operations{List: true}})

	c.Assert(err, qt.IsNil)
	c.Assert(string(res.Data), qt.Contains, "type UserEdge2 {")
}

func TestRenderRefusesEnumTypeCollisionBeforeOutput(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(&schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "State", Name: "states", APINames: schemamodel.TargetNames{GraphQL: "invoice_statuses"}},
			{StructName: "Invoice", Name: "invoices"},
		},
		Fields: []schemamodel.Field{
			{StructName: "State", Name: "id", Type: "BIGINT"},
			{StructName: "Invoice", Name: "status", Type: "invoice_status"},
		},
		Enums: []schemamodel.Enum{{Name: "invoice_status", Values: []string{"OPEN", "PAID"}}},
	}, graphqlrender.Options{})

	c.Assert(err, qt.ErrorMatches,
		`column "status" on table "invoices" produces GraphQL enum type name "InvoiceStatus", which collides with another type; choose distinct API names or api_type`)
	c.Assert(res.Data, qt.HasLen, 0)
}

func TestRenderRefusesAuthoredEnumThatCollidesWithDerivedObject(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(&schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "State", Name: "invoice_statuses"},
			{StructName: "Invoice", Name: "invoices"},
		},
		Fields: []schemamodel.Field{
			{StructName: "State", Name: "id", Type: "BIGINT"},
			{StructName: "Invoice", Name: "status", Type: "TEXT", APIType: "invoice_status"},
		},
		Enums: []schemamodel.Enum{{Name: "invoice_status", Values: []string{"OPEN", "PAID"}}},
	}, graphqlrender.Options{})

	c.Assert(err, qt.ErrorMatches,
		`column "status" on table "invoices" produces GraphQL enum type name "InvoiceStatus", which collides with another type; choose distinct API names or api_type`)
	c.Assert(res.Data, qt.HasLen, 0)
}

func TestRenderRefusesAuthoredEnumThatShadowsAReservedType(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(&schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Invoice", Name: "invoices"}},
		Fields: []schemamodel.Field{{
			StructName: "Invoice", Name: "status", Type: "TEXT", APIType: "query",
		}},
		Enums: []schemamodel.Enum{{Name: "query", Values: []string{"OPEN", "PAID"}}},
	}, graphqlrender.Options{})

	c.Assert(err, qt.ErrorMatches,
		`column "status" on table "invoices" produces GraphQL enum type name "Query", which collides with another type; choose distinct API names or api_type`)
	c.Assert(res.Data, qt.HasLen, 0)
}

func TestRenderPreservesDerivedOnlyEnumCollisionBehavior(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(&schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "State", Name: "invoice_statuses"},
			{StructName: "Invoice", Name: "invoices"},
		},
		Fields: []schemamodel.Field{
			{StructName: "State", Name: "id", Type: "BIGINT"},
			{StructName: "Invoice", Name: "status", Type: "invoice_status"},
		},
		Enums: []schemamodel.Enum{{Name: "invoice_status", Values: []string{"OPEN", "PAID"}}},
	}, graphqlrender.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(string(res.Data), qt.Contains, "enum InvoiceStatus2 {")
}

func TestRenderPreservesDerivedOnlyDuplicateQueryBehavior(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(&schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "user"}},
		Fields: []schemamodel.Field{{StructName: "User", Name: "id", Type: "BIGINT", Primary: true}},
	}, graphqlrender.Options{Operations: graphqlrender.Operations{List: true, ByID: true}})

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Count(string(res.Data), "  user("), qt.Equals, 1)
	c.Assert(res.Diagnostics, qt.HasLen, 1)
	c.Assert(res.Diagnostics[0].Message, qt.Equals, "duplicate query field name; omitted")
}

func TestRenderRefusesAuthoredDuplicateQueryFieldBeforeOutput(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(&schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "User", Name: "user",
			APINames: schemamodel.TargetNames{GraphQL: "user"},
		}},
		Fields: []schemamodel.Field{{StructName: "User", Name: "id", Type: "BIGINT", Primary: true}},
	}, graphqlrender.Options{Operations: graphqlrender.Operations{List: true, ByID: true}})

	c.Assert(err, qt.ErrorMatches,
		`table "user" produces GraphQL Query field name "user", which collides with another table; choose a distinct graphql_name`)
	c.Assert(res.Data, qt.HasLen, 0)
}

func TestRenderRefusesAuthoredFieldThatShadowsARelation(t *testing.T) {
	foreignKey := schemamodel.Field{
		StructName: "Book", Name: "author_id", Type: "BIGINT", Foreign: "authors(id)",
	}
	authored := schemamodel.Field{
		StructName: "Book", Name: "display_name", Type: "TEXT",
		APINames: schemamodel.TargetNames{GraphQL: "author"},
	}
	authorID := schemamodel.Field{StructName: "Author", Name: "id", Type: "BIGINT"}
	tests := []struct {
		name   string
		fields []schemamodel.Field
	}{
		{name: "foreign key first", fields: []schemamodel.Field{foreignKey, authored, authorID}},
		{name: "authored field first", fields: []schemamodel.Field{authored, foreignKey, authorID}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			res, err := graphqlrender.Render(&schemamodel.Database{
				Tables: []schemamodel.Table{
					{StructName: "Book", Name: "books"},
					{StructName: "Author", Name: "authors"},
				},
				Fields: test.fields,
			}, graphqlrender.Options{})

			c.Assert(err, qt.ErrorMatches,
				`foreign-key column "author_id" on table "books" produces GraphQL relation field name "author", which collides with another field; choose a distinct graphql_name`)
			c.Assert(res.Data, qt.HasLen, 0)
		})
	}
}

func TestRenderRefusesAuthoredForeignKeyThatShadowsItsRelation(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(&schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Book", Name: "books"},
			{StructName: "Author", Name: "authors"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Book", Name: "author_id", APIName: "author", Type: "BIGINT", Foreign: "authors(id)"},
			{StructName: "Author", Name: "id", Type: "BIGINT"},
		},
	}, graphqlrender.Options{})

	c.Assert(err, qt.ErrorMatches,
		`foreign-key column "author_id" on table "books" produces GraphQL relation field name "author", which collides with another field; choose a distinct graphql_name`)
	c.Assert(res.Data, qt.HasLen, 0)
}

func TestRenderRefusesAuthoredRelationThatShadowsADerivedField(t *testing.T) {
	derived := schemamodel.Field{StructName: "Book", Name: "author", Type: "TEXT"}
	authorID := schemamodel.Field{StructName: "Author", Name: "id", Type: "BIGINT"}
	sharedName := schemamodel.Field{
		StructName: "Book", Name: "author_id", APIName: "author_id",
		Type: "BIGINT", Foreign: "authors(id)",
	}
	targetName := schemamodel.Field{
		StructName: "Book", Name: "author_id", Type: "BIGINT", Foreign: "authors(id)",
		APINames: schemamodel.TargetNames{GraphQL: "author_id"},
	}
	tests := []struct {
		name   string
		fields []schemamodel.Field
	}{
		{name: "shared name/derived first", fields: []schemamodel.Field{derived, sharedName, authorID}},
		{name: "shared name/relation first", fields: []schemamodel.Field{sharedName, derived, authorID}},
		{name: "target name/derived first", fields: []schemamodel.Field{derived, targetName, authorID}},
		{name: "target name/relation first", fields: []schemamodel.Field{targetName, derived, authorID}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			res, err := graphqlrender.Render(&schemamodel.Database{
				Tables: []schemamodel.Table{
					{StructName: "Book", Name: "books"},
					{StructName: "Author", Name: "authors"},
				},
				Fields: test.fields,
			}, graphqlrender.Options{})

			c.Assert(err, qt.ErrorMatches,
				`foreign-key column "author_id" on table "books" produces GraphQL relation field name "author", which collides with another field; choose a distinct graphql_name`)
			c.Assert(res.Data, qt.HasLen, 0)
		})
	}
}

func TestRenderPreservesDerivedOnlyRelationCollisionBehavior(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(&schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Book", Name: "books"},
			{StructName: "Author", Name: "authors"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Book", Name: "author_id", Type: "BIGINT", Foreign: "authors(id)"},
			{StructName: "Book", Name: "authorId", Type: "BIGINT", Foreign: "authors(id)"},
			{StructName: "Author", Name: "id", Type: "BIGINT"},
		},
	}, graphqlrender.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Count(string(res.Data), "  author: Author!"), qt.Equals, 1)
}

func TestRenderKeepsRelationNameAcrossStorageRename(t *testing.T) {
	c := qt.New(t)

	render := func(storageName string) graphqlrender.Result {
		res, err := graphqlrender.Render(&schemamodel.Database{
			Tables: []schemamodel.Table{
				{StructName: "Book", Name: "books"},
				{StructName: "Author", Name: "authors"},
			},
			Fields: []schemamodel.Field{
				{StructName: "Book", Name: storageName, APIName: "owner_id", Type: "BIGINT", Foreign: "authors(id)"},
				{StructName: "Author", Name: "id", Type: "BIGINT"},
			},
		}, graphqlrender.Options{})
		c.Assert(err, qt.IsNil)
		return res
	}

	before := render("author_id")
	after := render("writer_id")
	c.Assert(string(before.Data), qt.Contains, "  owner_id: Int!")
	c.Assert(string(before.Data), qt.Contains, "  owner: Author!")
	c.Assert(after.Data, qt.DeepEquals, before.Data)
}

func TestRenderRefusesSharedFieldNameThatShadowsGraphQLIntrospection(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(apiNameFixture(
		schemamodel.Field{StructName: "Invoice", Name: "kind", APIName: "__typename", Type: "TEXT"},
	), graphqlrender.Options{})

	c.Assert(err, qt.ErrorMatches,
		`column "kind" on table "invoices" declares api_name "__typename", which produces reserved GraphQL field name "__typename"; choose a different graphql_name`)
	c.Assert(res.Data, qt.HasLen, 0)
}

func TestRenderValidatesWriteOnlyFieldsForFinalNameCollisions(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(apiNameFixture(
		schemamodel.Field{
			StructName: "Invoice", Name: "first", APIName: "amount-minor", Type: "INTEGER",
			APIExpose: "read",
		},
		schemamodel.Field{
			StructName: "Invoice", Name: "second", APIName: "amount_minor", Type: "INTEGER",
			APIExpose: "write",
		},
	), graphqlrender.Options{FieldPolicy: "allowlist"})

	c.Assert(err, qt.ErrorMatches,
		`columns "first" and "second" on table "invoices" both produce GraphQL field name "amount_minor"; give one of them a distinct graphql_name`)
	c.Assert(res.Data, qt.HasLen, 0)
}

func TestRenderValidatesWriteOnlyTargetName(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(apiNameFixture(
		schemamodel.Field{
			StructName: "Invoice", Name: "amount", Type: "INTEGER",
			APINames: schemamodel.TargetNames{GraphQL: "amount minor"}, APIExpose: "write",
		},
	), graphqlrender.Options{FieldPolicy: "allowlist"})

	c.Assert(err, qt.ErrorMatches,
		`column "amount" on table "invoices" declares graphql_name "amount minor", which is not a valid GraphQL field name`)
	c.Assert(res.Data, qt.HasLen, 0)
}

func TestRenderValidatesWriteOnlyAPIType(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(apiNameFixture(
		schemamodel.Field{
			StructName: "Invoice", Name: "amount", Type: "INTEGER",
			APIType: "money_ish", APIExpose: "write",
		},
	), graphqlrender.Options{FieldPolicy: "allowlist"})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `declares api_type "money_ish"`)
	c.Assert(err.Error(), qt.Contains, "GraphQL projection does not recognize")
	c.Assert(res.Data, qt.HasLen, 0)
}

func TestRenderRefusesDeclaredTableNameThatShadowsAReservedType(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(&schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "Queries", Name: "queries",
			APINames: schemamodel.TargetNames{GraphQL: "query"},
		}},
		Fields: []schemamodel.Field{{StructName: "Queries", Name: "id", Type: "BIGINT"}},
	}, graphqlrender.Options{})

	c.Assert(err, qt.ErrorMatches,
		`table "queries" declares graphql_name "query", which produces reserved GraphQL type name "Query"; choose a different graphql_name`)
	c.Assert(res.Data, qt.HasLen, 0)
}

// The documented limitation this exists for: DECIMAL becomes Float, a
// double-precision scalar that cannot carry an exact decimal. Publishing the
// column as text keeps the digits, and costs one declaration for all three
// export targets rather than one vocabulary per target.
func TestRenderUsesTheDeclaredAPIType(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(apiNameFixture(
		schemamodel.Field{StructName: "Invoice", Name: "id", Type: "BIGSERIAL", Primary: true},
		schemamodel.Field{StructName: "Invoice", Name: "plain", Type: "DECIMAL(12,2)"},
		schemamodel.Field{StructName: "Invoice", Name: "exact", Type: "DECIMAL(12,2)", APIType: "TEXT"},
	), graphqlrender.Options{})
	c.Assert(err, qt.IsNil)

	sdl := string(res.Data)
	c.Assert(sdl, qt.Contains, "plain: Float!")
	c.Assert(sdl, qt.Contains, "exact: String!")
}

// An override the projection cannot honor is refused, where an unrecognized
// COLUMN type is only warned about. One is a fact about the schema; the other
// is a declaration that would silently do nothing.
func TestRenderRefusesAnUnknownAPIType(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(apiNameFixture(
		schemamodel.Field{StructName: "Invoice", Name: "id", Type: "BIGSERIAL", Primary: true},
		schemamodel.Field{StructName: "Invoice", Name: "amount", Type: "DECIMAL(12,2)", APIType: "money_ish"},
	), graphqlrender.Options{})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `declares api_type "money_ish"`)
	c.Assert(err.Error(), qt.Contains, "GraphQL projection does not recognize")
	c.Assert(res.Data, qt.HasLen, 0)
}

// The control that keeps the refusal from becoming a blanket rejection of
// unmapped types: a column whose OWN type is unrecognized still exports.
func TestRenderStillExportsAnUnknownColumnType(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(apiNameFixture(
		schemamodel.Field{StructName: "Invoice", Name: "id", Type: "BIGSERIAL", Primary: true},
		schemamodel.Field{StructName: "Invoice", Name: "quirk", Type: "money_ish"},
	), graphqlrender.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(string(res.Data), qt.Contains, "quirk: String!")
}

// The override reaches enum resolution in both directions, and the scalar
// mapping alone would have refused the second case -- the one worth having.
func TestRenderProjectsEnumsBothWays(t *testing.T) {
	c := qt.New(t)

	db := apiNameFixture(
		schemamodel.Field{StructName: "Invoice", Name: "id", Type: "BIGSERIAL", Primary: true},
		schemamodel.Field{StructName: "Invoice", Name: "flattened", Type: "invoice_state", APIType: "TEXT"},
		schemamodel.Field{StructName: "Invoice", Name: "promoted", Type: "VARCHAR(32)", APIType: "invoice_state"},
	)
	db.Enums = []schemamodel.Enum{{Name: "invoice_state", Values: []string{"draft", "sent"}}}

	res, err := graphqlrender.Render(db, graphqlrender.Options{})
	c.Assert(err, qt.IsNil)

	sdl := string(res.Data)
	c.Assert(sdl, qt.Contains, "flattened: String!")
	c.Assert(sdl, qt.Contains, "promoted: InvoiceState!")
}

func TestRenderUsesPublishedIdentitiesForInlineEnumType(t *testing.T) {
	c := qt.New(t)

	render := func(storageName string) graphqlrender.Result {
		res, err := graphqlrender.Render(apiNameFixture(
			schemamodel.Field{
				StructName: "Invoice", Name: storageName, Type: "VARCHAR(16)",
				APINames: schemamodel.TargetNames{GraphQL: "status"},
				Enum:     []string{"DRAFT", "SENT"},
			},
		), graphqlrender.Options{})
		c.Assert(err, qt.IsNil)
		return res
	}

	before := render("billing_status")
	after := render("stored_status")
	c.Assert(string(before.Data), qt.Contains, "enum InvoiceStatus {")
	c.Assert(string(before.Data), qt.Contains, "status: InvoiceStatus!")
	c.Assert(after.Data, qt.DeepEquals, before.Data)
}

// Inline enum values answer before the type, so an override that did not clear
// them would do nothing at all, and say nothing about it.
func TestRenderOverridesInlineEnumValues(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(apiNameFixture(
		schemamodel.Field{StructName: "Invoice", Name: "id", Type: "BIGSERIAL", Primary: true},
		schemamodel.Field{
			StructName: "Invoice", Name: "state", Type: "VARCHAR(16)",
			Enum: []string{"draft", "sent"}, APIType: "TEXT",
		},
	), graphqlrender.Options{})
	c.Assert(err, qt.IsNil)

	sdl := string(res.Data)
	c.Assert(sdl, qt.Contains, "state: String!")
	c.Assert(sdl, qt.Not(qt.Contains), "enum ")
}

// One column, three published names. This is the case a shared alias cannot
// cover: GraphQL sanitizes a name its own rules reject, and an author who wants
// a deliberate name there should not have to change what the other two publish.
func TestRenderPrefersTheGraphQLName(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(apiNameFixture(
		schemamodel.Field{StructName: "Invoice", Name: "id", Type: "BIGSERIAL", Primary: true},
		schemamodel.Field{
			StructName: "Invoice", Name: "billing_amount_minor", Type: "INTEGER",
			APIName:  "amount",
			APINames: schemamodel.TargetNames{GraphQL: "amountMinor"},
		},
	), graphqlrender.Options{})
	c.Assert(err, qt.IsNil)

	sdl := string(res.Data)
	c.Assert(sdl, qt.Contains, "amountMinor: Int!")
	c.Assert(sdl, qt.Not(qt.Contains), "amount:")
	c.Assert(sdl, qt.Not(qt.Contains), "billing_amount_minor")
}

// The table-level half, and the control beside it: a name declared for another
// target is not read here.
func TestRenderIgnoresAnotherTargetsName(t *testing.T) {
	c := qt.New(t)

	db := apiNameFixture(
		schemamodel.Field{StructName: "Invoice", Name: "id", Type: "BIGSERIAL", Primary: true},
	)
	db.Tables[0].APIName = "invoices"
	db.Tables[0].APINames = schemamodel.TargetNames{Protobuf: "invoice_records"}

	res, err := graphqlrender.Render(db, graphqlrender.Options{})
	c.Assert(err, qt.IsNil)

	sdl := string(res.Data)
	c.Assert(sdl, qt.Contains, "type Invoice {")
	c.Assert(sdl, qt.Not(qt.Contains), "InvoiceRecord")
}

// A field-level target-specific name is an exact contract declaration, not
// another persistence name to normalize. Table names remain stems because the
// exporter singularizes and PascalCases every object type.
func TestRenderRefusesAnIllegalGraphQLName(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(apiNameFixture(
		schemamodel.Field{StructName: "Invoice", Name: "id", Type: "BIGSERIAL", Primary: true},
		schemamodel.Field{
			StructName: "Invoice", Name: "billing_amount_minor", Type: "INTEGER",
			APIName:  "amount",
			APINames: schemamodel.TargetNames{GraphQL: "amount minor"},
		},
	), graphqlrender.Options{})
	c.Assert(err, qt.ErrorMatches,
		`column "billing_amount_minor" on table "invoices" declares graphql_name "amount minor", which is not a valid GraphQL field name`)

	c.Assert(res.Data, qt.HasLen, 0)
	c.Assert(res.Diagnostics, qt.HasLen, 0)
}
