package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/core/yamlschema"
)

// TestParse_Topic_HappyPath reads a YDB topic in YAML: its settings under the
// keys the annotation reads, with the same spellings, and its consumers in
// the order they are written.
func TestParse_Topic_HappyPath(t *testing.T) {
	c := qt.New(t)

	db, err := yamlschema.Parse([]byte(`
topics:
  events:
    schema: app
    min_active_partitions: 2
    max_active_partitions: 6
    auto_partitioning_strategy: scale_up
    retention_period: PT36H
    supported_codecs: [raw, gzip]
    consumers:
      billing:
        important: true
      audit:
        read_from: "2026-01-01T00:00:00Z"
        supported_codecs: raw
  queue_alias:
    name: queue
`))

	c.Assert(err, qt.IsNil)
	c.Assert(db.Topics, qt.DeepEquals, []schemamodel.Topic{
		{Name: "events", Schema: "app", Spec: ast.TopicSpec{
			MinActivePartitions: 2, MaxActivePartitions: 6, AutoPartitioningStrategy: "scale_up",
			RetentionPeriod: "PT36H", SupportedCodecs: []string{"raw", "gzip"},
			Consumers: []ast.TopicConsumerSpec{
				{Name: "billing", Important: true},
				{Name: "audit", ReadFrom: "2026-01-01T00:00:00Z", SupportedCodecs: []string{"raw"}},
			},
		}},
		{Name: "queue"},
	})
}

// TestParse_Topic_FailurePath refuses a topic YDB would refuse or keep
// differently, naming the topic and the consumer, and a key the topic does
// not take.
func TestParse_Topic_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		document string
		wantErr  string
	}{
		{name: "an unknown key", document: "topics:\n  events:\n    retention: PT1H\n",
			wantErr: `(?s)parse YAML schema: .*field retention not found.*`},
		{name: "an empty value", document: "topics:\n  events:\n    retention_period: \"\"\n",
			wantErr: `topic "events": invalid retention_period: takes an ISO 8601 duration such as PT12H or P1D`},
		{name: "a maximum without a strategy", document: "topics:\n  events:\n    max_active_partitions: 3\n",
			wantErr: `topic "events": invalid max_active_partitions: it shapes auto-partitioning, .*`},
		{name: "a maximum below the minimum",
			document: "topics:\n  events:\n    auto_partitioning_strategy: scale_up\n    min_active_partitions: 4\n    max_active_partitions: 2\n",
			wantErr:  `topic "events": invalid max_active_partitions "2": it is below min_active_partitions, 4, .*`},
		{name: "a codec YDB keeps as no list", document: "topics:\n  events:\n    supported_codecs: [raw, snappy]\n",
			wantErr: `topic "events": invalid supported_codecs "raw,snappy": takes a comma-separated list of .*`},
		{name: "a consumer YDB refuses", document: "topics:\n  events:\n    consumers:\n      c:\n        important: true\n        availability_period: PT1H\n",
			wantErr: `topic "events", consumer "c": invalid availability_period "PT1H": .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := yamlschema.Parse([]byte(test.document))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}
