package annotation_test

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/annotation"
	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// recordingFile hands back what a test sets, and records the declarations
// and tables it is given.
type recordingFile struct {
	decoded  []annotation.Declaration
	tables   annotation.Tables
	finished []annotation.Contribution
}

func (f *recordingFile) Decode(declaration annotation.Declaration) ([]annotation.Contribution, error) {
	f.decoded = append(f.decoded, declaration)
	return nil, nil
}

func (f *recordingFile) Finish(tables annotation.Tables) ([]annotation.Contribution, error) {
	f.tables = tables
	return f.finished, nil
}

// fileWidget is widget read through a file decoder that finishes with
// finished. files receives each decoder the set starts.
func fileWidget(owner, directive string, files *[]*recordingFile, finished []annotation.Contribution) annotation.Extension {
	extension := widget(owner, directive)
	extension.Decode = nil
	extension.File = func() annotation.FileDecoder {
		file := &recordingFile{finished: finished}
		*files = append(*files, file)
		return file
	}
	return extension
}

func TestTables_Owning_HappyPath(t *testing.T) {
	tables := annotation.Tables{
		{Name: "items", Struct: "Item"},
		{Schema: "archive", Name: "events", Struct: "ArchivedEvent"},
		{Schema: "public", Name: "events", Struct: "Event"},
	}
	tests := []struct {
		name, structName, table string
		want                    int
	}{
		{name: "the struct's own table", structName: "Item", want: 0},
		{name: "a table the declaration names", structName: "Holder", table: "items", want: 0},
		{name: "a schema-qualified name", structName: "Holder", table: "public.events", want: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			index, err := tables.Owning(test.structName, test.table, "a gauge")

			c.Assert(err, qt.IsNil)
			c.Assert(index, qt.Equals, test.want)
		})
	}
}

func TestTables_Owning_FailurePath(t *testing.T) {
	tables := annotation.Tables{
		{Schema: "archive", Name: "events", Struct: "ArchivedEvent"},
		{Schema: "public", Name: "events", Struct: "Event"},
	}
	tests := []struct {
		name, structName, table, wantErr string
	}{
		{name: "a struct that maps to no table", structName: "Holder",
			wantErr: `struct Holder maps to no table in this file; name the table with the table attribute`},
		{name: "a table the file does not declare", structName: "Holder", table: "other",
			wantErr: `table "other" is not declared in this file, and a gauge is declared beside its table`},
		{name: "a name two schemas declare", structName: "Holder", table: "events",
			wantErr: `table "events" is declared in schemas "archive" and "public"; name the schema in the table attribute`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			index, err := tables.Owning(test.structName, test.table, "a gauge")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(index, qt.Equals, -1)
		})
	}
}

// TestReader_AFileDecoderSeesTheWholeFile pins the order a file decoder is
// driven in: one decoder per owner and file, every declaration in the order
// the file writes them, then Finish with the file's tables.
func TestReader_AFileDecoderSeesTheWholeFile(t *testing.T) {
	c := qt.New(t)
	var files []*recordingFile
	source := annotation.Declaration{Directive: "ptah:schema:widget", Struct: "W", Line: 3}
	finished := []annotation.Contribution{{Facet: &level{Value: "high"}, Label: "a level", Source: source}}
	set, err := annotation.NewSet(fileWidget("example.org/widget", "ptah:schema:widget", &files, finished))
	c.Assert(err, qt.IsNil)
	reader := set.Reader()
	tables := annotation.Tables{{Name: "items", Struct: "Item"}}

	first, firstErr := reader.Decode(source)
	second, secondErr := reader.Decode(annotation.Declaration{Directive: "ptah:schema:widget", Struct: "V", Line: 5})
	contributions, finishErr := reader.Finish(tables)

	c.Assert(firstErr, qt.IsNil)
	c.Assert(secondErr, qt.IsNil)
	c.Assert(finishErr, qt.IsNil)
	c.Assert(first, qt.HasLen, 0)
	c.Assert(second, qt.HasLen, 0)
	c.Assert(files, qt.HasLen, 1)
	c.Assert(files[0].decoded, qt.HasLen, 2)
	c.Assert(files[0].decoded[1].Line, qt.Equals, 5)
	c.Assert(files[0].tables, qt.DeepEquals, tables)
	c.Assert(contributions, qt.DeepEquals, finished)
}

// TestReader_AFileWithoutTheOwnersDirectivesStartsNoDecoder is the control
// on the lazy start: an owner whose directives a file does not write is not
// asked to finish it.
func TestReader_AFileWithoutTheOwnersDirectivesStartsNoDecoder(t *testing.T) {
	c := qt.New(t)
	var files []*recordingFile
	set, err := annotation.NewSet(fileWidget("example.org/widget", "ptah:schema:widget", &files, nil))
	c.Assert(err, qt.IsNil)

	contributions, err := set.Reader().Finish(nil)

	c.Assert(err, qt.IsNil)
	c.Assert(contributions, qt.HasLen, 0)
	c.Assert(files, qt.HasLen, 0)
}

func TestReader_Finish_FailurePath(t *testing.T) {
	declaration := annotation.Declaration{Directive: "ptah:schema:widget"}
	tests := []struct {
		name     string
		finished []annotation.Contribution
		wantErr  string
	}{
		{name: "a contribution that names no declaration",
			finished: []annotation.Contribution{{Facet: &level{}}},
			wantErr:  `.*example.org/widget finished a contribution that names no declaration`},
		{name: "a model the owner does not declare",
			finished: []annotation.Contribution{{Facet: &shade{}, Source: declaration}},
			wantErr:  `.*directive "ptah:schema:widget" contributed a model example.org/widget does not declare`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var files []*recordingFile
			set, err := annotation.NewSet(fileWidget("example.org/widget", "ptah:schema:widget", &files, test.finished))
			c.Assert(err, qt.IsNil)
			reader := set.Reader()
			_, err = reader.Decode(declaration)
			c.Assert(err, qt.IsNil)

			contributions, err := reader.Finish(nil)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(contributions, qt.IsNil)
		})
	}
}

func TestReader_ANilFileDecoderIsRefused(t *testing.T) {
	c := qt.New(t)
	extension := widget("example.org/widget", "ptah:schema:widget")
	extension.Decode = nil
	extension.File = func() annotation.FileDecoder { return nil }
	set, err := annotation.NewSet(extension)
	c.Assert(err, qt.IsNil)

	contributions, err := set.Reader().Decode(annotation.Declaration{Directive: "ptah:schema:widget"})

	c.Assert(err, qt.ErrorMatches, `.*example.org/widget started no file decoder`)
	c.Assert(contributions, qt.IsNil)
}

func TestNewSet_FileDecoderAndLimits_FailurePath(t *testing.T) {
	var files []*recordingFile
	both := fileWidget("example.org/widget", "ptah:schema:widget", &files, nil)
	both.Decode = noContributions
	limited := func(owner string, kinds ...string) annotation.Extension {
		extension := widget(owner, "ptah:schema:"+owner)
		extension.Kinds = nil
		extension.Limits = kinds
		return extension
	}
	tests := []struct {
		name       string
		extensions []annotation.Extension
		wantErr    string
	}{
		{name: "both a decoder and a file decoder", extensions: []annotation.Extension{both},
			wantErr: `.* declares both a decoder and a file decoder`},
		{name: "an empty limit kind", extensions: []annotation.Extension{limited("a", "")},
			wantErr: `.* reads not-described kind "", which is not a lower-case name`},
		{name: "a limit kind in upper case", extensions: []annotation.Extension{limited("a", "Gauge")},
			wantErr: `.* reads not-described kind "Gauge", which is not a lower-case name`},
		{name: "a limit kind of the frontend's own", extensions: []annotation.Extension{limited("a", "sequence")},
			wantErr: `.* reads not-described kind "sequence", which is the frontend's own`},
		{name: "a limit kind two owners read", extensions: []annotation.Extension{limited("a", "gauge"), limited("b", "gauge")},
			wantErr: `duplicate.*not-described kind "gauge" is read by a and b`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			set, err := annotation.NewSet(test.extensions...)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(set.Selected(), qt.IsFalse)
		})
	}
}

// TestSet_Coverage_HandsEachOwnerItsLimits pins that a limit reaches the
// owner that reads its kind, compared without case, and no other owner.
func TestSet_Coverage_HandsEachOwnerItsLimits(t *testing.T) {
	c := qt.New(t)
	received := make(map[string][]coverage.Object)
	set, err := annotation.NewSet(
		readingOwner(received, "example.org/gauge", []schemaext.Kind{levelKind}, levelCoverage, "gauge"),
		readingOwner(received, "example.org/dial", nil, emptyClaim, "dial"),
	)
	c.Assert(err, qt.IsNil)
	limit := coverage.Object{Kind: " Gauge ", Name: "spare", Provenance: coverage.Declared}

	known, err := set.Coverage(limit)
	owner, found := set.LimitOwner("GAUGE")

	c.Assert(err, qt.IsNil)
	c.Assert(received["example.org/gauge"], qt.DeepEquals, []coverage.Object{limit})
	c.Assert(received["example.org/dial"], qt.HasLen, 0)
	c.Assert(known.Lookup(levelKind, objectidentity.ID{}).State, qt.Equals, schemaext.Complete)
	c.Assert(found, qt.IsTrue)
	c.Assert(owner, qt.Equals, "example.org/gauge")
}

// readingOwner reads not-described declarations of limits, records the ones
// its claim is handed in received, and claims what claim does.
func readingOwner(received map[string][]coverage.Object, owner string, kinds []schemaext.Kind,
	claim func() (schemaext.Coverage, error), limits ...string,
) annotation.Extension {
	extension := widget(owner, "ptah:schema:"+owner)
	extension.Kinds = kinds
	extension.Limits = limits
	extension.Coverage = func(written []coverage.Object) (schemaext.Coverage, error) {
		received[owner] = written
		return claim()
	}
	return extension
}

func emptyClaim() (schemaext.Coverage, error) { return schemaext.Coverage{}, nil }

func TestSet_Coverage_FailurePath(t *testing.T) {
	c := qt.New(t)
	set, err := annotation.NewSet(widget("example.org/widget", "ptah:schema:widget"))
	c.Assert(err, qt.IsNil)

	known, err := set.Coverage(coverage.Object{Kind: "gauge"})
	owner, found := set.LimitOwner("gauge")

	c.Assert(err, qt.ErrorMatches, `no selected owner reads not-described kind "gauge"`)
	c.Assert(known.IsZero(), qt.IsTrue)
	c.Assert(found, qt.IsFalse)
	c.Assert(owner, qt.Equals, "")
}

func TestDeclarationError_ReadsAndUnwrapsAsItsRefusal(t *testing.T) {
	c := qt.New(t)
	refusal := errors.New("a gauge reads low or high")

	err := error(&annotation.DeclarationError{Attribute: "level", Err: refusal})

	c.Assert(err, qt.ErrorMatches, `a gauge reads low or high`)
	c.Assert(err, qt.ErrorIs, refusal)
}

func TestUnlimited_IgnoresTheLimits(t *testing.T) {
	c := qt.New(t)

	known, err := annotation.Unlimited(levelCoverage)([]coverage.Object{{Kind: "gauge"}})

	c.Assert(err, qt.IsNil)
	c.Assert(known.Lookup(levelKind, objectidentity.ID{}).State, qt.Equals, schemaext.Complete)
}
