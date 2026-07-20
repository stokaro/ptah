package goschema_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/ptaherr"
	"github.com/stokaro/ptah/core/renderer"
	dbtypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/schemadiff"
)

func TestParseDir_EmbedPathReportsTypedParseError(t *testing.T) {
	c := qt.New(t)

	root := t.TempDir()
	err := os.WriteFile(filepath.Join(root, "models.go"), []byte(`package models

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="INT" bogus="y"
	ID int64
}
`), 0o600)
	c.Assert(err, qt.IsNil)

	err = runEmbedPath(root)

	var parseErr *ptaherr.ParseError
	c.Assert(err, qt.ErrorAs, &parseErr)
	c.Assert(parseErr.File, qt.Equals, "models.go")
	c.Assert(parseErr.Line, qt.Equals, 5)
	c.Assert(parseErr.Attribute, qt.Equals, "bogus")
	c.Assert(parseErr.Directive, qt.Equals, "migrator:schema:field")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnknownAttribute)
}

func TestParseFS_ReportsAllTypedParseErrors(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"one.go": &fstest.MapFile{Data: []byte(`package models

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="INT" bogus="y"
	ID int64
}
`)},
		"two.go": &fstest.MapFile{Data: []byte(`package models

//migrator:schema:table name="posts"
type Post struct {
	//migrator:schema:field name="id" type="INT" mystery="z"
	ID int64
}
`)},
	}

	_, err := goschema.ParseFS(fsys, ".")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnknownAttribute)
	c.Assert(countParseErrors(err), qt.Equals, 2)
}

func TestParseSource_InvalidAttributeValueIsTyped(t *testing.T) {
	c := qt.New(t)

	_, err := goschema.ParseSource("schema.go", `package models

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="INT" identity_generation="BY_DEFUALT"
	ID int64
}
`)

	var parseErr *ptaherr.ParseError
	c.Assert(err, qt.ErrorAs, &parseErr)
	c.Assert(parseErr.Directive, qt.Equals, "migrator:schema:field")
	c.Assert(parseErr.Attribute, qt.Equals, "identity_generation")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
}

func TestParseFS_AccumulatesInvalidAttributeValues(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"one.go": &fstest.MapFile{Data: []byte(`package models

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="INT" identity_generation="BY_DEFUALT"
	ID int64
}
`)},
		"two.go": &fstest.MapFile{Data: []byte(`package models

//migrator:schema:table name="events"
type Event struct {
	//migrator:schema:index name="idx_events_id" fields="id" granularity="-1"
	_ int
}
`)},
	}

	_, err := goschema.ParseFS(fsys, ".")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
	c.Assert(countParseErrors(err), qt.Equals, 2)
}

func runEmbedPath(root string) error {
	generated, err := goschema.ParseDir(root)
	if err != nil {
		return err
	}
	diff := schemadiff.Compare(generated, &dbtypes.DBSchema{})
	nodes, err := planner.GenerateSchemaDiffAST(diff, generated, "postgres")
	if err != nil {
		return err
	}
	_, err = renderer.RenderSQL("postgres", nodes...)
	return err
}

func countParseErrors(err error) int {
	if err == nil {
		return 0
	}
	var parseErr *ptaherr.ParseError
	count := 0
	if errors.As(err, &parseErr) {
		count++
	}

	type multiUnwrapper interface {
		Unwrap() []error
	}
	if multi, ok := err.(multiUnwrapper); ok {
		count = 0
		for _, child := range multi.Unwrap() {
			count += countParseErrors(child)
		}
		return count
	}

	type unwrapper interface {
		Unwrap() error
	}
	if single, ok := err.(unwrapper); ok {
		count += countParseErrors(single.Unwrap())
	}
	return count
}
