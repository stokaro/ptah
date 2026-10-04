package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
)

// TestParseSource_Topic_HappyPath reads a YDB topic and its consumers, which
// may be declared before the topic or on another struct of the same file.
func TestParseSource_Topic_HappyPath(t *testing.T) {
	c := qt.New(t)
	source := `package entities

//ptah:schema:topic:consumer name="audit" topic="events" schema="app" read_from="2026-01-01T03:00:00+03:00"
type Audit struct{}

// Events is a queue.
//
//ptah:schema:topic name="events" schema="/app/" retention_period="PT36H" supported_codecs="raw,gzip"
//ptah:schema:topic:consumer name="billing" topic="events" schema="app" important="true"
type Events struct{}

//ptah:schema:topic name="plain"
type Plain struct{}
`

	db, err := goschema.ParseSource("topics.go", source)

	c.Assert(err, qt.IsNil)
	c.Assert(db.Topics, qt.DeepEquals, []schemamodel.Topic{
		{StructName: "Events", Name: "events", Schema: "app", Spec: ast.TopicSpec{
			RetentionPeriod: "PT36H", SupportedCodecs: []string{"raw", "gzip"},
			Consumers: []ast.TopicConsumerSpec{
				{Name: "audit", ReadFrom: "2026-01-01T00:00:00Z"},
				{Name: "billing", Important: true},
			},
		}},
		{StructName: "Plain", Name: "plain"},
	})
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

// Two files declaring one topic differently are refused when they are
// merged, as two declarations of one table are.
func TestMerge_Topics_Conflict(t *testing.T) {
	c := qt.New(t)
	first := &schemamodel.Database{Topics: []schemamodel.Topic{{StructName: "A", Name: "events", Schema: "app"}}}
	same := &schemamodel.Database{Topics: []schemamodel.Topic{{StructName: "B", Name: "events", Schema: "app"}}}
	other := &schemamodel.Database{Topics: []schemamodel.Topic{{StructName: "C", Name: "events", Schema: "app",
		Spec: ast.TopicSpec{RetentionPeriod: "PT1H"}}}}

	merged, mergeErr := schemamodel.Merge(first, same)
	conflict, conflictErr := schemamodel.Merge(first, other)

	c.Assert(mergeErr, qt.IsNil)
	c.Assert(merged.Topics, qt.HasLen, 1)
	c.Assert(conflictErr, qt.ErrorMatches, `conflicting topic "app.events" definitions`)
	c.Assert(conflict, qt.IsNil)
}
