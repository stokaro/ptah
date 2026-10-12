package pgextension_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgextension"
)

func TestOperation_HappyPath(t *testing.T) {
	behavioral := schemaext.Effect{Impact: schemaext.Behavioral, Reason: "changes the version or the schema of an extension and of the objects it owns"}
	tests := []struct {
		name      string
		operation *pgextension.Operation
		effect    schemaext.Effect
		report    ast.ExtensionChange
	}{
		{
			name:      "a creation",
			operation: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.Create, Schema: "ext", Version: "1.6", IfNotExists: true},
			effect:    schemaext.Effect{Impact: schemaext.Additive, Reason: "installs an extension"},
			report:    ast.ExtensionChange{Action: ast.ExtensionAdd, Name: "pg_trgm"},
		},
		{
			name:      "an update",
			operation: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.UpdateVersion, Version: "1.6"},
			effect:    behavioral, report: ast.ExtensionChange{Action: ast.ExtensionModify, Name: "pg_trgm"},
		},
		{
			name:      "a move",
			operation: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.SetSchema, Schema: "ext"},
			effect:    behavioral, report: ast.ExtensionChange{Action: ast.ExtensionModify, Name: "pg_trgm"},
		},
		{
			name:      "a drop",
			operation: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.Drop},
			effect:    schemaext.Effect{Impact: schemaext.Destructive, Reason: "DROP EXTENSION removes database objects owned by the extension"},
			report:    ast.ExtensionChange{Action: ast.ExtensionDrop, Name: "pg_trgm"},
		},
		{
			name:      "a guarded drop with its dependents",
			operation: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.Drop, IfExists: true, Cascade: true},
			effect: schemaext.Effect{Impact: schemaext.Destructive,
				Reason: "DROP EXTENSION ... CASCADE removes database objects owned by the extension and every object that depends on them"},
			report: ast.ExtensionChange{Action: ast.ExtensionDrop, Name: "pg_trgm"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.operation.Validate(), qt.IsNil)
			c.Assert(test.operation.Effect(), qt.Equals, test.effect)
			c.Assert(test.operation.SchemaChange(), qt.Equals, test.report)
			c.Assert(test.operation.Subject(), qt.DeepEquals, pgextension.Ref("pg_trgm"))

			codec := pgextension.OperationCodecs()[0]
			decoded := must.Must(codec.Decode(must.Must(codec.Encode(test.operation))))
			c.Assert(decoded, qt.DeepEquals, test.operation)
		})
	}
}

func TestOperation_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		operation *pgextension.Operation
		want      string
	}{
		{name: "no name", operation: &pgextension.Operation{Action: pgextension.Drop}, want: `.*identified by its name alone.*`},
		{name: "no action", operation: &pgextension.Operation{Name: "pg_trgm"}, want: `.*unknown extension action ""`},
		{name: "an update without a version", operation: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.UpdateVersion},
			want: `.*an extension update names its version and nothing else`},
		{name: "an update that moves", operation: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.UpdateVersion, Version: "1.6", Schema: "ext"},
			want: `.*an extension update names its version and nothing else`},
		{name: "a move without a schema", operation: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.SetSchema},
			want: `.*an extension move names its schema and nothing else`},
		{name: "a drop guarded like a creation", operation: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.Drop, IfNotExists: true},
			want: `.*an extension drop names its extension and nothing else`},
		{name: "a creation guarded like a drop", operation: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.Create, IfExists: true},
			want: `.*only an extension drop names IF EXISTS or CASCADE`},
		{name: "an update that cascades", operation: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.UpdateVersion, Version: "1.6", Cascade: true},
			want: `.*only an extension drop names IF EXISTS or CASCADE`},
		{name: "a move guarded like a drop", operation: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.SetSchema, Schema: "ext", IfExists: true},
			want: `.*only an extension drop names IF EXISTS or CASCADE`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := test.operation.Validate()

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(test.operation.Effect(), qt.Equals, schemaext.Effect{})
		})
	}

	t.Run("a nil operation", func(t *testing.T) {
		c := qt.New(t)

		c.Assert((*pgextension.Operation)(nil).Validate(), qt.ErrorMatches, `.*extension operation is nil`)
	})

	t.Run("a wire without its action", func(t *testing.T) {
		c := qt.New(t)

		decoded, err := pgextension.OperationCodecs()[0].Decode(json.RawMessage(`{"name":"pg_trgm","schema":"","version":"","if_not_exists":false,"if_exists":false,"cascade":false}`))

		c.Assert(err, qt.ErrorAs, new(*schemaext.InvalidModelError))
		c.Assert(decoded, qt.IsNil)
	})
}

func TestCommentOperation_HappyPath(t *testing.T) {
	c := qt.New(t)
	operation := &pgextension.CommentOperation{Name: "pg_trgm", Comment: "trigrams"}

	kind, name := operation.OmissionSubject()

	c.Assert(operation.Validate(), qt.IsNil)
	c.Assert(operation.RequiredCapability(), qt.Equals, capability.ExtensionComments)
	c.Assert([]string{kind, name}, qt.DeepEquals, []string{"extension comment", "pg_trgm"})
	c.Assert(operation.Effect(), qt.Equals, schemaext.Effect{Impact: schemaext.Additive, Reason: "changes only the comment of an extension"})
	c.Assert(operation.SchemaChange(), qt.Equals, ast.ExtensionChange{Action: ast.ExtensionModify, Name: "pg_trgm"})
	codec := pgextension.OperationCodecs()[1]
	c.Assert(must.Must(codec.Decode(must.Must(codec.Encode(operation)))), qt.DeepEquals, operation)
}

func TestCommentOperation_FailurePath(t *testing.T) {
	c := qt.New(t)
	operation := &pgextension.CommentOperation{Name: "pg_trgm", Comment: "a\x00b"}

	c.Assert(operation.Validate(), qt.ErrorMatches, `.*extension comment contains a NUL byte`)
	c.Assert(operation.Effect(), qt.Equals, schemaext.Effect{})
	c.Assert((*pgextension.CommentOperation)(nil).Validate(), qt.ErrorMatches, `.*extension comment operation is nil`)
}
