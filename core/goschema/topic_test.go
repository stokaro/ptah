package goschema_test

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbtopic"
)

// TestParseSource_Topic_HappyPath reads a YDB topic and its consumers, which
// may be declared before the topic or on another struct of the same file, into
// the topic owner's model, and claims the topic namespace the source
// describes.
func TestParseSource_Topic_HappyPath(t *testing.T) {
	c := qt.New(t)
	source := `package entities

//ptah:schema:topic:consumer name="audit" topic="events" schema="app" read_from="2026-01-01T03:00:00+03:00"
type Audit struct{}

// Events is a queue.
//
//ptah:schema:topic name="events" schema=" app " retention_period="PT36H" supported_codecs="raw,gzip"
//ptah:schema:topic:consumer name="billing" topic="events" schema="app" important="true"
type Events struct{}

//ptah:schema:topic name="plain"
type Plain struct{}
`

	db, err := goschema.ParseSource("topics.go", source)

	c.Assert(err, qt.IsNil)
	objects, err := db.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.DeepEquals, []schemaext.Object{
		ydbtopic.DesiredObject("", "plain", "Plain", ydbtopic.Spec{}),
		ydbtopic.DesiredObject("app", "events", "Events", ydbtopic.Spec{
			RetentionPeriod: "PT36H", SupportedCodecs: []string{"raw", "gzip"},
			Consumers: []ydbtopic.ConsumerSpec{
				{Name: "audit", ReadFrom: "2026-01-01T00:00:00Z"},
				{Name: "billing", Important: true},
			},
		}),
	})
	c.Assert(db.FeatureCoverage.Lookup(ydbtopic.Kind, ydbtopic.Ref("", "undeclared")).State, qt.Equals, schemaext.Complete)
}

// TestParseSource_Topic_FailurePath refuses a topic or a consumer YDB would
// refuse or keep differently, where it was written, and a consumer that names
// no topic of its file.
func TestParseSource_Topic_FailurePath(t *testing.T) {
	tests := []struct {
		name        string
		annotations string
		wantErr     string
		wantIs      error
	}{
		{name: "an unknown attribute", annotations: `//ptah:schema:topic name="events" retention="PT1H"`,
			wantErr: `unknown annotation attribute "retention" on //ptah:schema:topic at Events`, wantIs: ptaherr.ErrUnknownAttribute},
		{name: "no name", annotations: `//ptah:schema:topic retention_period="PT1H"`,
			wantErr: `missing required annotation attribute "name" on //ptah:schema:topic at Events`, wantIs: ptaherr.ErrMissingRequiredAttribute},
		{name: "a value YDB keeps differently", annotations: `//ptah:schema:topic name="events" retention_period="PT1.5S"`,
			wantErr: `invalid retention_period "PT1.5S": YDB keeps whole seconds, and drops the fraction of this one on //ptah:schema:topic at Events`,
			wantIs:  ptaherr.ErrInvalidAttributeValue},
		{name: "a consumer YDB refuses", annotations: `//ptah:schema:topic name="events"` + "\n" +
			`//ptah:schema:topic:consumer name="c" topic="events" important="maybe"`,
			wantErr: `invalid important "maybe": takes true or false on //ptah:schema:topic:consumer at Events`,
			wantIs:  ptaherr.ErrInvalidAttributeValue},
		{name: "a consumer of no topic", annotations: `//ptah:schema:topic:consumer name="c" topic="events"`,
			wantErr: `the file declares no topic "events" for consumer "c" on //ptah:schema:topic:consumer at Events`,
			wantIs:  ptaherr.ErrInvalidAttributeValue},
		{name: "a consumer of a topic in another directory", annotations: `//ptah:schema:topic name="events" schema="app"` + "\n" +
			`//ptah:schema:topic:consumer name="c" topic="events"`,
			wantErr: `the file declares no topic "events" for consumer "c" on //ptah:schema:topic:consumer at Events`,
			wantIs:  ptaherr.ErrInvalidAttributeValue},
		{name: "a directory written from the server root", annotations: `//ptah:schema:topic name="events" schema="/local/app"`,
			wantErr: `invalid schema "/local/app": starts with a slash; name the directory relative to the database root, ` +
				`without the database's own path on //ptah:schema:topic at Events`,
			wantIs: ptaherr.ErrInvalidAttributeValue},
		{name: "a topic declared twice", annotations: `//ptah:schema:topic name="events" schema="app"` + "\n" +
			`//ptah:schema:topic name="events" schema="app"`,
			wantErr: `topic app/events is declared twice on //ptah:schema:topic at Events`,
			wantIs:  ptaherr.ErrInvalidAttributeValue},
		{name: "a directory with a trailing slash", annotations: `//ptah:schema:topic name="events" schema="app/"`,
			wantErr: `invalid schema "app/": is not a directory path relative to the database root; write it without a trailing slash ` +
				`or an empty, \. or \.\. segment on //ptah:schema:topic at Events`,
			wantIs: ptaherr.ErrInvalidAttributeValue},
		{name: "a consumer's directory with a trailing slash", annotations: `//ptah:schema:topic name="events" schema="app"` + "\n" +
			`//ptah:schema:topic:consumer name="c" topic="events" schema="app/"`,
			wantErr: `invalid schema "app/": is not a directory path relative to the database root; write it without a trailing slash ` +
				`or an empty, \. or \.\. segment on //ptah:schema:topic:consumer at Events`,
			wantIs: ptaherr.ErrInvalidAttributeValue},
		{name: "a consumer's directory written from the server root", annotations: `//ptah:schema:topic name="events" schema="app"` + "\n" +
			`//ptah:schema:topic:consumer name="c" topic="events" schema="/local/app"`,
			wantErr: `invalid schema "/local/app": starts with a slash; .* on //ptah:schema:topic:consumer at Events`,
			wantIs:  ptaherr.ErrInvalidAttributeValue},
		{name: "a consumer declared twice", annotations: `//ptah:schema:topic name="events"` + "\n" +
			`//ptah:schema:topic:consumer name="c" topic="events"` + "\n" + `//ptah:schema:topic:consumer name="c" topic="events"`,
			wantErr: `topic "events" declares consumer "c" twice on //ptah:schema:topic:consumer at Events`,
			wantIs:  ptaherr.ErrInvalidAttributeValue},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := goschema.ParseSource("topics.go", "package entities\n\n"+test.annotations+"\ntype Events struct{}\n")
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(db, qt.DeepEquals, schemamodel.Database{})
		})
	}
}

// TestParse_Topics_DeclaredTwiceInOneFile refuses a topic one file declares
// twice with the error a duplicate carries in every other source, naming the
// name attribute.
func TestParse_Topics_DeclaredTwiceInOneFile(t *testing.T) {
	c := qt.New(t)
	db, err := goschema.ParseSource("topics.go", "package entities\n\n//ptah:schema:topic name=\"events\" schema=\"app\"\n"+
		"//ptah:schema:topic name=\"events\" schema=\"app\"\ntype Events struct{}\n")

	c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
	parseErr, ok := errors.AsType[*ptaherr.ParseError](err)
	c.Assert(ok, qt.IsTrue)
	c.Assert(parseErr.Attribute, qt.Equals, ydbtopic.AttributeName)
	c.Assert(db, qt.DeepEquals, schemamodel.Database{})
}

// TestMerge_Topics_DeclaredTwice refuses two files that declare one topic,
// even the same way: a topic has one declaration.
func TestMerge_Topics_DeclaredTwice(t *testing.T) {
	c := qt.New(t)
	first, err := goschema.ParseSource("a.go", "package entities\n\n//ptah:schema:topic name=\"events\" schema=\"app\"\ntype A struct{}\n")
	c.Assert(err, qt.IsNil)
	second, err := goschema.ParseSource("b.go", "package entities\n\n//ptah:schema:topic name=\"events\" schema=\"app\"\ntype B struct{}\n")
	c.Assert(err, qt.IsNil)

	merged, err := schemamodel.Merge(&first, &second)

	c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
	c.Assert(merged, qt.IsNil)
}
