package yamlschema_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-extras/go-kit/must"

	"ptah.run/core/renderer"
	"ptah.run/core/yamlschema"
)

// ExampleParse reads a YAML document into the schema model and reports what the
// model holds. The result is a *schemamodel.Database, the same type Go annotations,
// HCL, SQL, and DBML produce.
func ExampleParse() {
	document := []byte(`
tables:
  accounts:
    comment: Customer accounts
    columns:
      id:
        type: SERIAL
        primary: true
      email:
        type: VARCHAR(255)
        not_null: true
    indexes:
      accounts_email_key:
        fields: [email]
        unique: true
`)

	db := must.Must(yamlschema.Parse(document))

	for _, table := range db.Tables {
		fmt.Printf("table %s: %s\n", table.Name, table.Comment)
	}
	for _, field := range db.Fields {
		fmt.Printf("column %s.%s %s primary=%t nullable=%t\n",
			field.StructName, field.Name, field.Type, field.Primary, field.Nullable)
	}
	for _, index := range db.Indexes {
		fmt.Printf("index %s on %v unique=%t\n", index.Name, index.Fields, index.Unique)
	}

	// Output:
	// table accounts: Customer accounts
	// column accounts.id SERIAL primary=true nullable=true
	// column accounts.email VARCHAR(255) primary=false nullable=false
	// index accounts_email_key on [email] unique=true
}

// ExampleParse_render carries a parsed YAML schema through to SQL. Nothing
// downstream knows the schema was authored in YAML: the renderer is handed the
// same model every other authoring format produces.
func ExampleParse_render() {
	document := []byte(`
tables:
  accounts:
    columns:
      id:
        type: SERIAL
        primary: true
      email:
        type: VARCHAR(255)
        not_null: true
`)

	db := must.Must(yamlschema.Parse(document))
	for _, statement := range must.Must(renderer.GetOrderedCreateStatements(db, "postgres")) {
		fmt.Println(statement)
	}

	// Output:
	// -- POSTGRES TABLE: accounts --
	// CREATE TABLE "accounts" (
	//   "id" SERIAL PRIMARY KEY NOT NULL,
	//   "email" VARCHAR(255) NOT NULL
	// );
}

// ExampleParseFile reads the same document from a path. A schema file is the
// usual case: ParseFile is what `ptah schema render --schema-file schema.yaml`
// reaches.
func ExampleParseFile() {
	dir := must.Must(os.MkdirTemp("", "yamlschema-example"))
	defer os.RemoveAll(dir)

	document := []byte("tables:\n  accounts:\n    columns:\n      id:\n        type: SERIAL\n        primary: true\n")
	path := filepath.Join(dir, "schema.yaml")
	must.Assert(os.WriteFile(path, document, 0o600))

	db := must.Must(yamlschema.ParseFile(path))
	fmt.Println(db.Tables[0].Name, len(db.Fields))

	// Output:
	// accounts 1
}

// ExampleParse_unknownKey shows the first of the two strict refusals: a
// misspelled attribute is an error naming the line it sits on, not a setting
// that silently fails to apply. A permissive reader would drop the key and
// render this table without the comment its author wrote.
func ExampleParse_unknownKey() {
	_, err := yamlschema.Parse([]byte("tables:\n  accounts:\n    commnet: Customer accounts\n"))

	// The decoder closes the sentence by naming the Go type it was filling.
	// That type is Ptah's internal representation of a table, not part of
	// this package's API, so cut the clause instead of printing it.
	message, _, _ := strings.Cut(err.Error(), " in type ")
	fmt.Println(message)

	// Output:
	// parse YAML schema: yaml: unmarshal errors:
	//   line 3: field commnet not found
}

// ExampleParse_strictness shows the second strict refusal: a schema split
// across a `---` document separator is rejected whole rather than half-applied.
// ExampleParse_unknownKey shows the first.
func ExampleParse_strictness() {
	_, err := yamlschema.Parse([]byte("tables:\n  accounts: {}\n---\ntables:\n  orders: {}\n"))
	fmt.Println(err)

	// Output:
	// parse YAML schema: multiple YAML documents are not supported
}
