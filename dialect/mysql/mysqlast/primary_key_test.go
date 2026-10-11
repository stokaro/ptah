package mysqlast_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mysql/mysqlast"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/internal/modelast"
)

// keyHint is a primary key's block size as the MySQL owner's facet.
func keyHint(size uint64) schemaext.Facets {
	return must.Must(schemaext.NewFacets(&mysqlschema.DesiredIndexBlockSize{KeyBlockSize: size}))
}

// loweredKey is the PRIMARY KEY node the common lowering writes for table.
func loweredKey(table schemamodel.Table, fields []schemamodel.Field) *ast.ConstraintNode {
	create := modelast.FromTable(table, fields, nil, "mysql")
	return create.Constraints[slices.IndexFunc(create.Constraints, func(node *ast.ConstraintNode) bool {
		return node.Type == ast.PrimaryKeyConstraint
	})]
}

// PrimaryKeyFromDeclaration reads a declared key as the common planner lowers
// it, so the owner's replacement writes the definition the common one would.
// The rows cover a key declared on the table, with columns or with parts, and
// a key declared as a PRIMARY KEY constraint, each with the owner's hint.
func TestPrimaryKeyFromDeclaration_AgreesWithTheCommonLowering(t *testing.T) {
	fields := []schemamodel.Field{{StructName: "T", Name: "id", Type: "INT"}, {StructName: "T", Name: "name", Type: "VARCHAR(20)"}}
	columns := schemamodel.Table{StructName: "T", Name: "t", PrimaryKey: []string{"id", "name"}, PrimaryKeyMethod: "HASH",
		PrimaryKeyComment: "lookup", PrimaryKeyFacets: keyHint(8)}
	parts := schemamodel.Table{StructName: "T", Name: "t", PrimaryKey: []string{"id", "name"}, PrimaryKeyFacets: keyHint(4),
		PrimaryKeyParts: []schemamodel.PrimaryKeyPart{{Name: "id"}, {Name: "name", Prefix: "7", Desc: true}}}
	constraint := schemamodel.Constraint{StructName: "T", Name: "pk_t", Type: "PRIMARY KEY", Columns: []string{"id"},
		UsingMethod: "HASH", Comment: "lookup", Facets: keyHint(2)}
	for _, test := range []struct {
		name        string
		declaration schemacapture.TableDeclaration
		node        *ast.ConstraintNode
	}{
		{"columns on the table", schemacapture.TableDeclaration{Table: columns, Fields: fields}, loweredKey(columns, fields)},
		{"parts on the table", schemacapture.TableDeclaration{Table: parts, Fields: fields}, loweredKey(parts, fields)},
		{"a PRIMARY KEY constraint", schemacapture.TableDeclaration{Table: schemamodel.Table{StructName: "T", Name: "t"}, Fields: fields,
			Constraints: []schemamodel.Constraint{constraint}}, modelast.FromConstraint(constraint)},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			key, err := mysqlast.PrimaryKeyFromDeclaration(test.declaration)

			c.Assert(err, qt.IsNil)
			c.Assert(key, qt.DeepEquals, must.Must(mysqlast.PrimaryKeyFromNode(test.node)))
			c.Assert(key.Primary, qt.IsTrue)
			c.Assert(key.Name, qt.Equals, mysqlast.PrimaryKeyName)
			c.Assert(key.Validate(), qt.IsNil)
		})
	}
}

// A key declared on the fields alone is the key of those fields, in their
// order: the column spelling the common lowering writes for it.
func TestPrimaryKeyFromDeclaration_FieldsKey(t *testing.T) {
	c := qt.New(t)
	declaration := schemacapture.TableDeclaration{Table: schemamodel.Table{StructName: "T", Name: "t"}, Fields: []schemamodel.Field{
		{StructName: "T", Name: "tenant", Type: "INT", Primary: true}, {StructName: "T", Name: "a", Type: "INT"},
		{StructName: "T", Name: "id", Type: "INT", Primary: true},
	}}

	key, err := mysqlast.PrimaryKeyFromDeclaration(declaration)

	c.Assert(err, qt.IsNil)
	c.Assert(key, qt.DeepEquals, mysqlast.Index{Name: mysqlast.PrimaryKeyName, Primary: true,
		Parts: []mysqlast.IndexPart{{Column: "tenant"}, {Column: "id"}}})
}

func TestPrimaryKeyFromDeclaration_FailurePath(t *testing.T) {
	c := qt.New(t)

	key, err := mysqlast.PrimaryKeyFromDeclaration(schemacapture.TableDeclaration{Table: schemamodel.Table{StructName: "T", Name: "t"}})

	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(err, qt.ErrorMatches, `.*table "t" declares no primary key`)
	c.Assert(key, qt.DeepEquals, mysqlast.Index{})
}

// A node that is not a primary key, and a facet of another kind on one, are
// refused rather than written without it.
func TestPrimaryKeyFromNode_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		node    *ast.ConstraintNode
		wantErr string
	}{
		{"no node", nil, `.*no primary key to read a MySQL definition from`},
		{"a UNIQUE constraint", &ast.ConstraintNode{Type: ast.UniqueConstraint, Columns: []string{"a"}}, `.*no primary key to read a MySQL definition from`},
		{"another facet", &ast.ConstraintNode{Type: ast.PrimaryKeyConstraint, Columns: []string{"a"},
			Facets: must.Must(schemaext.NewFacets(&mysqlschema.DesiredTable{Engine: "InnoDB"}))}, `.*MySQL primary key facet "ptah.run/mysql/table" is not supported`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			key, err := mysqlast.PrimaryKeyFromNode(test.node)

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(key, qt.DeepEquals, mysqlast.Index{})
		})
	}
}

// A primary key has no name of its own, uniqueness, kind, parser,
// visibility or expression part, and the codec refuses one claiming any.
func TestIndex_PrimaryKey_FailurePath(t *testing.T) {
	part := []mysqlast.IndexPart{{Column: "id"}}
	for _, test := range []struct {
		name string
		key  mysqlast.Index
	}{
		{"another name", mysqlast.Index{Name: "k", Primary: true, Parts: part}},
		{"unique", mysqlast.Index{Name: "PRIMARY", Primary: true, Unique: true, Parts: part}},
		{"a FULLTEXT kind", mysqlast.Index{Name: "PRIMARY", Primary: true, Type: "FULLTEXT", Parts: part}},
		{"a parser", mysqlast.Index{Name: "PRIMARY", Primary: true, Parser: "ngram", Parts: part}},
		{"invisible", mysqlast.Index{Name: "PRIMARY", Primary: true, Invisible: true, Parts: part}},
		{"an expression", mysqlast.Index{Name: "PRIMARY", Primary: true, Parts: []mysqlast.IndexPart{{Expression: "id + 1"}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.key.Validate(), qt.ErrorIs, schemaext.ErrInvalidValue)
			encoded, err := mysqlast.Codecs()[0].Encode(&mysqlast.ReplaceIndex{Index: test.key})
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(encoded, qt.IsNil)
		})
	}
}

// The codec writes a primary key's own flag and reads it back.
func TestCodecs_PrimaryKey(t *testing.T) {
	c := qt.New(t)
	value := &mysqlast.ReplaceIndex{Index: mysqlast.Index{Name: "PRIMARY", Primary: true, Type: "HASH", KeyBlockSize: 8, Comment: "c",
		Parts: []mysqlast.IndexPart{{Column: "id"}, {Column: "name", Prefix: "7"}}}, TableCopy: true}
	codec := mysqlast.Codecs()[0]

	encoded, err := codec.Encode(value)

	c.Assert(err, qt.IsNil)
	c.Assert(string(encoded), qt.Equals, `{"index":{"name":"PRIMARY","primary":true,"type":"HASH","parts":[{"column":"id"},{"column":"name","prefix":"7"}],`+
		`"key_block_size":8,"comment":"c"},"table_copy":true}`)
	c.Assert(must.Must(codec.Decode(encoded)), qt.DeepEquals, schemaext.Payload(value))
	c.Assert(value.SchemaChange(), qt.Equals, ast.ExtensionChange{Action: ast.ExtensionModify, Name: "PRIMARY"})
	c.Assert(value.Effect().Reason, qt.Contains, "ALGORITHM=COPY rebuilds the whole table to rebuild the primary key")
	value.TableCopy = false
	c.Assert(value.Effect().Reason, qt.Contains, "the primary key is dropped and built again")
}
