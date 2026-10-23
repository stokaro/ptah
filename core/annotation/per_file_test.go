package annotation_test

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/annotation"
	"ptah.run/core/schemaext"
)

// other is a model the widget owner does not declare.
type other struct{ level }

func (*other) Kind() schemaext.Kind { return "example.org/other/model" }

// perFileWidget is a widget that adds attributes to the frontend's own
// directives and reads them through its file decoder.
func perFileWidget(files *[]*recordingFile, finished []annotation.Contribution, groups ...annotation.DirectiveAttributes) annotation.Extension {
	extension := fileWidget("example.org/widget", "ptah:schema:widget", files, finished)
	extension.Attributes = groups
	return extension
}

func perFile(directive string, attributes ...string) annotation.DirectiveAttributes {
	group := annotation.DirectiveAttributes{Directive: directive, PerFile: true}
	for _, name := range attributes {
		group.Attributes = append(group.Attributes, annotation.Attribute{Name: name, Value: "string"})
	}
	return group
}

func TestReader_DecodeAttributes_HappyPath(t *testing.T) {
	c := qt.New(t)
	var files []*recordingFile
	set, err := annotation.NewSet(perFileWidget(&files, nil, perFile("ptah:schema:field", "family"), perFile("ptah:schema:index", "bloom")))
	c.Assert(err, qt.IsNil)
	reader := set.Reader()
	subject := annotation.Subject{Kind: annotation.SubjectColumn, Struct: "Record", Name: "id"}

	contributions, err := reader.DecodeAttributes(annotation.Declaration{Directive: "ptah:schema:field", Struct: "Record", Subject: subject,
		Attributes: map[string]string{"name": "id", "type": "INTEGER", "family": "hot"}})

	c.Assert(err, qt.IsNil)
	c.Assert(contributions, qt.HasLen, 0)
	c.Assert(files, qt.HasLen, 1)
	c.Assert(files[0].decoded, qt.HasLen, 1)
	c.Assert(files[0].decoded[0].Directive, qt.Equals, "ptah:schema:field")
	c.Assert(files[0].decoded[0].Subject, qt.Equals, subject)
	c.Assert(files[0].decoded[0].Attributes, qt.DeepEquals, map[string]string{"family": "hot"},
		qt.Commentf("the owner sees its own attributes and none of the frontend's"))
}

func TestReader_DecodeAttributes_ReadsOnlyTheDirectiveAndAttributesItOwns(t *testing.T) {
	c := qt.New(t)
	var files []*recordingFile
	set, err := annotation.NewSet(perFileWidget(&files, nil, perFile("ptah:schema:field", "family")))
	c.Assert(err, qt.IsNil)
	reader := set.Reader()

	otherAttributes, otherAttributesErr := reader.DecodeAttributes(annotation.Declaration{Directive: "ptah:schema:field", Attributes: map[string]string{"name": "id"}})
	otherDirective, otherDirectiveErr := reader.DecodeAttributes(annotation.Declaration{Directive: "ptah:schema:index", Attributes: map[string]string{"family": "hot"}})

	c.Assert(otherAttributesErr, qt.IsNil)
	c.Assert(otherDirectiveErr, qt.IsNil)
	c.Assert(otherAttributes, qt.HasLen, 0)
	c.Assert(otherDirective, qt.HasLen, 0)
	c.Assert(files, qt.HasLen, 0, qt.Commentf("a declaration that writes none of the owner's attributes starts no decoder"))
}

// answeringFile answers every declaration with fixed contributions or a refusal.
type answeringFile struct {
	recordingFile
	answer []annotation.Contribution
	err    error
}

func (f *answeringFile) Decode(annotation.Declaration) ([]annotation.Contribution, error) {
	return f.answer, f.err
}

func answeringWidget(file *answeringFile) annotation.Extension {
	extension := perFileWidget(nil, nil, perFile("ptah:schema:table", "ttl"))
	extension.File = func() annotation.FileDecoder { return file }
	return extension
}

func TestReader_DecodeAttributes_ReturnsWhatTheOwnerDeclares(t *testing.T) {
	c := qt.New(t)
	answer := []annotation.Contribution{{Facet: &level{Value: "high"}, Label: "a level"}}
	set, err := annotation.NewSet(answeringWidget(&answeringFile{answer: answer}))
	c.Assert(err, qt.IsNil)

	contributions, err := set.Reader().DecodeAttributes(annotation.Declaration{Directive: "ptah:schema:table", Attributes: map[string]string{"ttl": "1"}})

	c.Assert(err, qt.IsNil)
	c.Assert(contributions, qt.DeepEquals, answer)
}

func TestReader_DecodeAttributes_FailurePath(t *testing.T) {
	refusal := errors.New("ttl is not a duration")
	tests := []struct {
		name string
		file *answeringFile
		want string
	}{
		{name: "the owner refuses", file: &answeringFile{err: refusal}, want: `ttl is not a duration`},
		{name: "a contribution of a model the owner does not declare", file: &answeringFile{answer: []annotation.Contribution{{Facet: &other{}, Label: "x"}}},
			want: `.*directive "ptah:schema:table" contributed a model example.org/widget does not declare`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			set, err := annotation.NewSet(answeringWidget(test.file))
			c.Assert(err, qt.IsNil)

			contributions, err := set.Reader().DecodeAttributes(annotation.Declaration{Directive: "ptah:schema:table", Attributes: map[string]string{"ttl": "1"}})

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(contributions, qt.HasLen, 0)
		})
	}
}

func TestNewSet_PerFileAttributesAndSubjects_FailurePath(t *testing.T) {
	var files []*recordingFile
	column := func(scopes ...annotation.Scope) annotation.Extension {
		extension := widget("example.org/widget", "ptah:schema:widget")
		extension.Directives[0].Scopes, extension.Directives[0].Describes = scopes, annotation.SubjectColumn
		return extension
	}
	withDescribes := func(kind annotation.SubjectKind, scopes ...annotation.Scope) annotation.Extension {
		extension := column(scopes...)
		extension.Directives[0].Describes = kind
		return extension
	}
	decoded := perFileWidget(&files, nil, perFile("ptah:schema:field", "family"))
	decoded.Attributes[0].Decode = func(map[string]string) (schemaext.Facets, error) { return schemaext.Facets{}, nil }
	tests := []struct {
		name      string
		extension annotation.Extension
		want      string
	}{
		{name: "a directive the frontend does not read per file", extension: perFileWidget(&files, nil, perFile("ptah:schema:function", "family")),
			want: `annotation extension of example.org/widget reads attributes of "ptah:schema:function" per file, which the frontend reads for .*`},
		{name: "no file decoder", extension: func() annotation.Extension {
			extension := widget("example.org/widget", "ptah:schema:widget")
			extension.Attributes = []annotation.DirectiveAttributes{perFile("ptah:schema:field", "family")}
			return extension
		}(), want: `annotation extension of example.org/widget reads attributes of "ptah:schema:field" per file, which needs a file decoder.*`},
		{name: "a decoder beside the file decoder", extension: decoded,
			want: `annotation extension of example.org/widget reads attributes of "ptah:schema:field" per file, which needs a file decoder.*`},
		{name: "a column directive on a struct", extension: column(annotation.ScopeStruct),
			want: `annotation extension of example.org/widget declares column directive "ptah:schema:widget" outside a field comment`},
		{name: "a column directive without a scope", extension: column(),
			want: `annotation extension of example.org/widget declares column directive "ptah:schema:widget" outside a field comment`},
		{name: "an index directive at file scope", extension: withDescribes(annotation.SubjectIndex, annotation.ScopeFile),
			want: `annotation extension of example.org/widget declares index directive "ptah:schema:widget" outside a struct or field comment`},
		{name: "an unknown part", extension: withDescribes("constraint", annotation.ScopeField),
			want: `annotation extension of example.org/widget declares directive "ptah:schema:widget" that describes unknown part "constraint"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			set, err := annotation.NewSet(test.extension)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(set.Selected(), qt.IsFalse)
		})
	}
}

func TestNewSet_PerFileAttributesAndSubjects_HappyPath(t *testing.T) {
	c := qt.New(t)
	var files []*recordingFile
	extension := perFileWidget(&files, nil, perFile("ptah:schema:table", "ttl"), perFile("ptah:schema:field", "family"), perFile("ptah:schema:index", "bloom"))
	extension.Directives[0].Scopes, extension.Directives[0].Describes = []annotation.Scope{annotation.ScopeStruct, annotation.ScopeField}, annotation.SubjectIndex

	set, err := annotation.NewSet(extension)

	c.Assert(err, qt.IsNil)
	c.Assert(set.AttributedDirectives(), qt.DeepEquals, []string{"ptah:schema:field", "ptah:schema:index", "ptah:schema:table"})
	c.Assert(set.Directives()[0].Describes, qt.Equals, annotation.SubjectIndex)
}
