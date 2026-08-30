package goschema_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing/fstest"

	"github.com/go-extras/go-kit/must"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/schemamodel"
)

// ExampleParseSource is the minimum viable embedding: one annotated struct,
// parsed from source already in hand, no filesystem. The filename argument is
// never opened; it labels the source. This is first contact with the
// annotation grammar — a //ptah:schema:table directive on the struct and a
// //ptah:schema:field directive on each column-bearing field.
func ExampleParseSource() {
	source := `package models

//ptah:schema:table name="products"
type Product struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string
}
`

	db := must.Must(goschema.ParseSource("products.go", source))

	for _, table := range db.Tables {
		fmt.Printf("table %s (struct %s)\n", table.Name, table.StructName)
	}
	for _, field := range db.Fields {
		fmt.Printf("column %s %s primary=%t nullable=%t\n",
			field.Name, field.Type, field.Primary, field.Nullable)
	}

	// Output:
	// table products (struct Product)
	// column id SERIAL primary=true nullable=true
	// column name VARCHAR(255) primary=false nullable=false
}

// ExampleParseSource_unknownAttribute shows the typo contract: a misspelled
// attribute fails at parse time instead of becoming a silently missing
// setting. The error is a *ptaherr.ParseError naming the file, line,
// directive, and attribute, and errors.Is identifies the refusal kind.
func ExampleParseSource_unknownAttribute() {
	source := `package models

//ptah:schema:table name="products"
type Product struct {
	//ptah:schema:field name="name" type="VARCHAR(255)" not_nul="true"
	Name string
}
`

	_, err := goschema.ParseSource("products.go", source)

	if parseErr, ok := errors.AsType[*ptaherr.ParseError](err); ok {
		fmt.Printf("file=%s line=%d directive=%s attribute=%s\n",
			parseErr.File, parseErr.Line, parseErr.Directive, parseErr.Attribute)
	}
	fmt.Println("unknown attribute:", errors.Is(err, ptaherr.ErrUnknownAttribute))

	// Output:
	// file=products.go line=5 directive=ptah:schema:field attribute=not_nul
	// unknown attribute: true
}

// ExampleParseFS runs the finalized pipeline over an in-memory filesystem:
// two entity files where posts foreign-keys users. The order the files are
// read in does not decide the result: the finalize pipeline orders tables by
// their foreign-key dependencies, so users comes out before posts — the order
// tables must be created in, and the same order on every run.
func ExampleParseFS() {
	fsys := fstest.MapFS{
		"posts.go": &fstest.MapFile{Data: []byte(`package models

//ptah:schema:table name="posts"
type Post struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="user_id" type="INT" not_null="true" foreign="users(id)"
	UserID int64
}
`)},
		"users.go": &fstest.MapFile{Data: []byte(`package models

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}
`)},
	}

	db := must.Must(goschema.ParseFS(fsys, "."))

	for _, table := range db.Tables {
		fmt.Println(table.Name)
	}

	// Output:
	// users
	// posts
}

// ExampleParseDirRaw contrasts the raw walk with the finalized one. The raw
// schema carries the declarations and none of the derived metadata — no
// dependency graph, nothing deduplicated, embedded fields unexpanded — and
// handing it to schemamodel.Merge is what derives all of that. The deferral
// is the point: parse each authoring source raw, then let one Merge call
// apply the cross-source collision policy over all of them.
func ExampleParseDirRaw() {
	dir := must.Must(os.MkdirTemp("", "goschema-example"))
	defer os.RemoveAll(dir)

	must.Assert(os.WriteFile(filepath.Join(dir, "a.go"), []byte(`package models

//ptah:schema:table name="posts"
type Post struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="user_id" type="INT" not_null="true" foreign="users(id)"
	UserID int64
}
`), 0o600))
	must.Assert(os.WriteFile(filepath.Join(dir, "b.go"), []byte(`package models

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}
`), 0o600))

	raw := must.Must(goschema.ParseDirRaw(dir))
	fmt.Println("raw tables:", len(raw.Tables), "dependency entries:", len(raw.Dependencies))

	merged := must.Must(schemamodel.Merge(raw))
	for _, table := range merged.Tables {
		fmt.Println("merged:", table.Name, merged.Dependencies[table.QualifiedName()])
	}

	// Output:
	// raw tables: 2 dependency entries: 0
	// merged: users []
	// merged: posts [users]
}
