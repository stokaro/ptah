package schemaexport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/schemaexport"
)

// exposureDB is the shape stokaro/ptah#904 is about: a server-owned key, an
// ordinary column, a credential that may be sent and never returned, an
// internal column that belongs in no contract, and one column that declares
// nothing at all.
func exposureDB() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users"}},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "INTEGER", Primary: true, APIExpose: "read"},
			{StructName: "User", Name: "email", Type: "TEXT", APIExpose: "read-write"},
			{StructName: "User", Name: "password_hash", Type: "TEXT", APIExpose: "write"},
			{StructName: "User", Name: "internal_state", Type: "TEXT", APIExpose: "none"},
			{StructName: "User", Name: "undeclared", Type: "TEXT"},
		},
	}
}

func fieldNames(fields []goschema.Field) []string {
	names := make([]string, 0, len(fields))
	for _, field := range fields {
		names = append(names, field.Name)
	}
	return names
}

// TestExposedFields pins the whole decision table: which columns reach which
// contract under which policy.
//
// The two policies are the point. Under "all" an undeclared column is exported,
// which is what every schema written before this existed relies on. Under
// "allowlist" it is not, which is what makes an additive migration unable to
// widen a contract on its own.
func TestExposedFields(t *testing.T) {
	tests := []struct {
		name   string
		shape  schemaexport.Shape
		policy schemaexport.FieldPolicy
		want   []string
	}{
		{
			name:   "read under all keeps the undeclared column",
			shape:  schemaexport.ShapeRead,
			policy: schemaexport.FieldPolicyAll,
			want:   []string{"id", "email", "undeclared"},
		},
		{
			name:   "write under all keeps the undeclared column",
			shape:  schemaexport.ShapeWrite,
			policy: schemaexport.FieldPolicyAll,
			want:   []string{"email", "password_hash", "undeclared"},
		},
		{
			name:   "read under allowlist drops it",
			shape:  schemaexport.ShapeRead,
			policy: schemaexport.FieldPolicyAllowlist,
			want:   []string{"id", "email"},
		},
		{
			name:   "write under allowlist drops it",
			shape:  schemaexport.ShapeWrite,
			policy: schemaexport.FieldPolicyAllowlist,
			want:   []string{"email", "password_hash"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := exposureDB()

			fields, _, err := schemaexport.ExposedFields(db, db.Tables[0], test.shape, test.policy)

			c.Assert(err, qt.IsNil)
			c.Assert(fieldNames(fields), qt.DeepEquals, test.want)
		})
	}
}

// TestExposedFieldsNeverPublishesNone is the control the table above cannot be:
// a model that answered "everything" to every row would satisfy several of
// them, and the one column that must never appear is the one declared none.
func TestExposedFieldsNeverPublishesNone(t *testing.T) {
	tests := []struct {
		name   string
		shape  schemaexport.Shape
		policy schemaexport.FieldPolicy
	}{
		{name: "read under all", shape: schemaexport.ShapeRead, policy: schemaexport.FieldPolicyAll},
		{name: "write under all", shape: schemaexport.ShapeWrite, policy: schemaexport.FieldPolicyAll},
		{name: "read under allowlist", shape: schemaexport.ShapeRead, policy: schemaexport.FieldPolicyAllowlist},
		{name: "write under allowlist", shape: schemaexport.ShapeWrite, policy: schemaexport.FieldPolicyAllowlist},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := exposureDB()

			fields, _, err := schemaexport.ExposedFields(db, db.Tables[0], test.shape, test.policy)

			c.Assert(err, qt.IsNil)
			c.Assert(fieldNames(fields), qt.Not(qt.Contains), "internal_state")
		})
	}
}

// TestExposedFieldsReportsWhatItWithheld pins that a withheld column is named.
//
// A policy that hides something without saying so is indistinguishable from a
// schema that never had the column, which is the failure #904 asks the
// diagnostics to prevent.
func TestExposedFieldsReportsWhatItWithheld(t *testing.T) {
	c := qt.New(t)
	db := exposureDB()

	_, diagnostics, err := schemaexport.ExposedFields(
		db, db.Tables[0], schemaexport.ShapeRead, schemaexport.FieldPolicyAllowlist)

	c.Assert(err, qt.IsNil)
	paths := make([]string, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		paths = append(paths, diagnostic.Path)
	}
	c.Assert(paths, qt.DeepEquals, []string{
		"users.password_hash", "users.internal_state", "users.undeclared",
	})
	c.Assert(diagnostics[2].Message, qt.Contains, "declares no api_expose")
	c.Assert(diagnostics[2].Message, qt.Contains, "allowlist")
}

// TestParseExposureRefusesAnUnknownValue pins that a misspelled declaration is
// refused rather than read as absent.
//
// Reading it as absent is the dangerous direction: under the default policy an
// unparsed api_expose="nome" would publish the column the author wrote the
// declaration to hide, silently.
func TestParseExposureRefusesAnUnknownValue(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{name: "empty is absent, not an error", value: ""},
		{name: "none", value: "none"},
		{name: "read", value: "read"},
		{name: "write", value: "write"},
		{name: "read-write", value: "read-write"},
		{name: "a misspelling", value: "nome", wantErr: true},
		{name: "a plausible synonym", value: "readwrite", wantErr: true},
		{name: "a boolean", value: "false", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := schemaexport.ParseExposure(test.value)

			c.Assert(err != nil, qt.Equals, test.wantErr, qt.Commentf("err=%v", err))
		})
	}
}

// TestExposedFieldsRefusesAnUnknownDeclaration pins that the refusal reaches the
// caller with the table and column named, rather than being swallowed per field.
func TestExposedFieldsRefusesAnUnknownDeclaration(t *testing.T) {
	c := qt.New(t)
	db := exposureDB()
	db.Fields[1].APIExpose = "sometimes"

	_, _, err := schemaexport.ExposedFields(
		db, db.Tables[0], schemaexport.ShapeRead, schemaexport.FieldPolicyAll)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `table "users" column "email"`)
	c.Assert(err.Error(), qt.Contains, "sometimes")
}

// TestParseFieldPolicy pins the flag's accepted values, including the empty one
// that keeps the historical behavior.
func TestParseFieldPolicy(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    schemaexport.FieldPolicy
		wantErr bool
	}{
		{name: "empty defaults to all", value: "", want: schemaexport.FieldPolicyAll},
		{name: "all", value: "all", want: schemaexport.FieldPolicyAll},
		{name: "allowlist", value: "allowlist", want: schemaexport.FieldPolicyAllowlist},
		{name: "an unknown policy", value: "strict", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := schemaexport.ParseFieldPolicy(test.value)

			c.Assert(err != nil, qt.Equals, test.wantErr, qt.Commentf("err=%v", err))
			c.Assert(got, qt.Equals, test.want)
		})
	}
}
