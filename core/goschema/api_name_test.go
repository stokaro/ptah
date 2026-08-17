package goschema_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
)

// The exported API name is declared on the field annotation, and this is the
// reachability proof for it: the attribute registry rejects an unknown key, so
// a value that parses here is one an author can actually write
// (stokaro/ptah#905).
func TestParseFileReadsTheAPIName(t *testing.T) {
	tests := []struct {
		name        string
		annotation  string
		wantColumn  string
		wantAPIName string
	}{
		{
			name:        "a declared API name is carried beside the column name",
			annotation:  `name="billing_amount_minor" api_name="amount" type="INTEGER"`,
			wantColumn:  "billing_amount_minor",
			wantAPIName: "amount",
		},
		{
			// Absent is the overwhelmingly common case, and it has to stay
			// empty rather than be filled in with the column name: the
			// exporters distinguish "no API name declared" from "declared, and
			// happens to match", and only the first may change if the column
			// is renamed.
			name:        "an absent one stays empty",
			annotation:  `name="note" type="TEXT"`,
			wantColumn:  "note",
			wantAPIName: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			dir := t.TempDir()
			path := filepath.Join(dir, "model.go")
			source := "package model\n\n" +
				"//ptah:schema:table name=\"invoices\"\n" +
				"type Invoice struct {\n" +
				"\t//ptah:schema:field " + tt.annotation + "\n" +
				"\tValue string\n" +
				"}\n"
			c.Assert(os.WriteFile(path, []byte(source), 0o600), qt.IsNil)

			db, err := goschema.ParseFile(path)

			c.Assert(err, qt.IsNil)
			c.Assert(db.Fields, qt.HasLen, 1)
			c.Assert(db.Fields[0].Name, qt.Equals, tt.wantColumn)
			c.Assert(db.Fields[0].APIName, qt.Equals, tt.wantAPIName)
		})
	}
}

// The table-level attribute has the same reachability question as the field's:
// the strict unknown-key validator would reject it if it were not registered.
func TestParseFileReadsTheTableAPIName(t *testing.T) {
	tests := []struct {
		name        string
		annotation  string
		wantTable   string
		wantAPIName string
	}{
		{
			name:        "a declared API name is carried beside the table name",
			annotation:  `name="billing_invoices" api_name="invoices"`,
			wantTable:   "billing_invoices",
			wantAPIName: "invoices",
		},
		{
			name:        "an absent one stays empty",
			annotation:  `name="invoices"`,
			wantTable:   "invoices",
			wantAPIName: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			dir := t.TempDir()
			path := filepath.Join(dir, "model.go")
			source := "package model\n\n" +
				"//ptah:schema:table " + tt.annotation + "\n" +
				"type Invoice struct {\n" +
				"\t//ptah:schema:field name=\"id\" type=\"BIGSERIAL\" primary=\"true\"\n" +
				"\tID int64\n" +
				"}\n"
			c.Assert(os.WriteFile(path, []byte(source), 0o600), qt.IsNil)

			db, err := goschema.ParseFile(path)

			c.Assert(err, qt.IsNil)
			c.Assert(db.Tables, qt.HasLen, 1)
			c.Assert(db.Tables[0].Name, qt.Equals, tt.wantTable)
			c.Assert(db.Tables[0].APIName, qt.Equals, tt.wantAPIName)
		})
	}
}

// The type override has the same reachability question as the two names, and
// the same answer: an unregistered attribute is refused by the strict
// unknown-key validator, so a value that arrives here is one an author can
// write. It is carried beside the column type, never in place of it — the
// storage type is what the migration engine plans against.
func TestParseFileReadsTheAPIType(t *testing.T) {
	tests := []struct {
		name        string
		annotation  string
		wantType    string
		wantAPIType string
	}{
		{
			name:        "a declared override is carried beside the column type",
			annotation:  `name="amount" type="DECIMAL(12,2)" api_type="TEXT"`,
			wantType:    "DECIMAL(12,2)",
			wantAPIType: "TEXT",
		},
		{
			// Absent has to stay empty rather than be filled in with the
			// column type: the exporters refuse an override they cannot map
			// and only warn about a column type they cannot map, so the two
			// cases must remain distinguishable.
			name:        "an absent one stays empty",
			annotation:  `name="note" type="TEXT"`,
			wantType:    "TEXT",
			wantAPIType: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			dir := t.TempDir()
			path := filepath.Join(dir, "model.go")
			source := "package model\n\n" +
				"//ptah:schema:table name=\"invoices\"\n" +
				"type Invoice struct {\n" +
				"\t//ptah:schema:field " + tt.annotation + "\n" +
				"\tValue string\n" +
				"}\n"
			c.Assert(os.WriteFile(path, []byte(source), 0o600), qt.IsNil)

			db, err := goschema.ParseFile(path)

			c.Assert(err, qt.IsNil)
			c.Assert(db.Fields, qt.HasLen, 1)
			c.Assert(db.Fields[0].Type, qt.Equals, tt.wantType)
			c.Assert(db.Fields[0].APIType, qt.Equals, tt.wantAPIType)
		})
	}
}

// The per-target names have the same reachability question as every other
// attribute, times six: three targets on the field directive and three on the
// table directive. An unregistered one is refused by the strict unknown-key
// validator, so a value that arrives here is one an author can write.
func TestParseFileReadsThePerTargetNames(t *testing.T) {
	tests := []struct {
		name  string
		field string
		table string
		want  goschema.TargetNames
	}{
		{
			name:  "all three, declared together",
			field: `name="amount" type="INTEGER" openapi_name="amount" graphql_name="amountMinor" proto_name="amount_minor"`,
			table: `name="billing_invoices" openapi_name="invoices" graphql_name="Invoice" proto_name="invoice_records"`,
			want: goschema.TargetNames{
				OpenAPI:  "amount",
				GraphQL:  "amountMinor",
				Protobuf: "amount_minor",
			},
		},
		{
			// One declared and two absent has to stay one declared and two
			// absent: an absent target name falls back to the shared one, and
			// filling it in here would make the fallback unobservable.
			name:  "one alone leaves the others empty",
			field: `name="amount" type="INTEGER" graphql_name="amountMinor"`,
			table: `name="billing_invoices" graphql_name="Invoice"`,
			want:  goschema.TargetNames{GraphQL: "amountMinor"},
		},
		{
			name:  "none declared is the zero value",
			field: `name="amount" type="INTEGER"`,
			table: `name="billing_invoices"`,
			want:  goschema.TargetNames{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			dir := t.TempDir()
			path := filepath.Join(dir, "model.go")
			source := "package model\n\n" +
				"//ptah:schema:table " + tt.table + "\n" +
				"type Invoice struct {\n" +
				"\t//ptah:schema:field " + tt.field + "\n" +
				"\tValue string\n" +
				"}\n"
			c.Assert(os.WriteFile(path, []byte(source), 0o600), qt.IsNil)

			db, err := goschema.ParseFile(path)

			c.Assert(err, qt.IsNil)
			c.Assert(db.Fields, qt.HasLen, 1)
			c.Assert(db.Fields[0].APINames, qt.Equals, tt.want)
		})
	}
}

// The table directive carries its own three, read from the same place, so a
// target cannot end up declarable on one level and not the other.
func TestParseFileReadsThePerTargetTableNames(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "model.go")
	source := "package model\n\n" +
		"//ptah:schema:table name=\"billing_invoices\" openapi_name=\"invoices\" " +
		"graphql_name=\"Invoice\" proto_name=\"invoice_records\"\n" +
		"type Invoice struct {\n" +
		"\t//ptah:schema:field name=\"id\" type=\"BIGSERIAL\" primary=\"true\"\n" +
		"\tID int64\n" +
		"}\n"
	c.Assert(os.WriteFile(path, []byte(source), 0o600), qt.IsNil)

	db, err := goschema.ParseFile(path)

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].APINames, qt.Equals, goschema.TargetNames{
		OpenAPI:  "invoices",
		GraphQL:  "Invoice",
		Protobuf: "invoice_records",
	})
}
