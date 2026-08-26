package yamlschema_test

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/go-extras/go-kit/must"

	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/yamlschema"
)

// ExampleParse reads a YAML document into the schema model and reports what the
// model holds. The result is a *goschema.Database, the same type Go annotations,
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

// ExampleParse_strictness shows what the parser refuses rather than accepts
// halfway. A schema split across a document separator is one of the two
// refusals; the other is an unknown key, reported with the line that carries it
// instead of being dropped as an unrecognized setting.
func ExampleParse_strictness() {
	_, err := yamlschema.Parse([]byte("tables:\n  accounts: {}\n---\ntables:\n  orders: {}\n"))
	fmt.Println(err)

	// Output:
	// parse YAML schema: multiple YAML documents are not supported
}
