package ydbchangefeed_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbchangefeed"
)

// base is a changefeed as a declaration states it; each row below changes one
// thing about a copy of it.
func base() ydbschema.ChangefeedSpec {
	return ydbschema.ChangefeedSpec{
		Name: "feed", Mode: "UPDATES", Format: "JSON", RetentionPeriod: "PT12H",
		Consumers: []ast.TopicConsumerSpec{{Name: "audit", Important: true, SupportedCodecs: []string{"raw", "gzip"}}},
	}
}

// TestEqual_ReadsAsYDBKeeps holds the spellings YDB keeps as one value equal
// to each other, so a database read back compares equal to its declaration.
func TestEqual_ReadsAsYDBKeeps(t *testing.T) {
	read := base()
	read.Mode, read.Format = "updates", "json"
	read.RetentionPeriod = "PT720M"
	read.Consumers = []ast.TopicConsumerSpec{{Name: "audit", Important: true, SupportedCodecs: []string{"GZIP", "raw"},
		ReadFrom: "1970-01-01T00:00:00Z"}}
	defaultRetention := base()
	defaultRetention.RetentionPeriod = ""
	readDefault := base()
	readDefault.RetentionPeriod = "P1D"
	unknownPartitions := base()
	unknownPartitions.TopicMinActivePartitions = 3
	tests := []struct {
		name             string
		desired, current ydbschema.ChangefeedSpec
	}{
		{name: "case, interval spelling, codec order and the epoch", desired: base(), current: read},
		{name: "no retention and YDB's 24 hours", desired: defaultRetention, current: readDefault},
		{name: "a partition count the declaration does not name", desired: base(), current: unknownPartitions},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbchangefeed.Equal(test.desired, test.current), qt.IsTrue)
			c.Assert(ydbchangefeed.Equal(test.current, test.desired), qt.IsTrue)
		})
	}
}

// TestRecreated_HappyPath pins each option of ADD CHANGEFEED as one whose
// change drops and adds the changefeed.
func TestRecreated_HappyPath(t *testing.T) {
	change := func(edit func(*ydbschema.ChangefeedSpec)) ydbschema.ChangefeedSpec {
		spec := base()
		edit(&spec)
		return spec
	}
	tests := []struct {
		name    string
		current ydbschema.ChangefeedSpec
	}{
		{name: "mode", current: change(func(s *ydbschema.ChangefeedSpec) { s.Mode = "KEYS_ONLY" })},
		{name: "format", current: change(func(s *ydbschema.ChangefeedSpec) { s.Format = "DEBEZIUM_JSON" })},
		{name: "virtual timestamps", current: change(func(s *ydbschema.ChangefeedSpec) { s.VirtualTimestamps = true })},
		{name: "resolved timestamps", current: change(func(s *ydbschema.ChangefeedSpec) { s.ResolvedTimestamps = "PT1S" })},
		{name: "initial scan", current: change(func(s *ydbschema.ChangefeedSpec) { s.InitialScan = true })},
		{name: "user SIDs", current: change(func(s *ydbschema.ChangefeedSpec) { s.UserSIDs = true })},
		{name: "schema changes", current: change(func(s *ydbschema.ChangefeedSpec) { s.SchemaChanges = true })},
		{name: "auto partitioning", current: change(func(s *ydbschema.ChangefeedSpec) { s.TopicAutoPartitioning = true })},
		{name: "disabled", current: change(func(s *ydbschema.ChangefeedSpec) { s.Disabled = true })},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbchangefeed.Recreated(base(), test.current), qt.IsTrue)
			c.Assert(ydbchangefeed.Equal(base(), test.current), qt.IsFalse)
		})
	}
}

// TestRecreated_PartitionCountBothSidesName holds a partition count both sides
// name as an option like the others.
func TestRecreated_PartitionCountBothSidesName(t *testing.T) {
	c := qt.New(t)
	desired, current := base(), base()
	desired.TopicMinActivePartitions, current.TopicMinActivePartitions = 2, 3
	c.Assert(ydbchangefeed.Recreated(desired, current), qt.IsTrue)
}

// TestTopicChanged_HappyPath pins the retention and each consumer setting as
// changes of the topic rather than of the changefeed.
func TestTopicChanged_HappyPath(t *testing.T) {
	consumer := func(edit func(*ast.TopicConsumerSpec)) ydbschema.ChangefeedSpec {
		spec := base()
		edit(&spec.Consumers[0])
		return spec
	}
	retention := base()
	retention.RetentionPeriod = ""
	added := base()
	added.Consumers = append(added.Consumers, ast.TopicConsumerSpec{Name: "other"})
	renamed := base()
	renamed.Consumers = []ast.TopicConsumerSpec{{Name: "audit2", Important: true, SupportedCodecs: []string{"raw", "gzip"}}}
	tests := []struct {
		name    string
		current ydbschema.ChangefeedSpec
	}{
		{name: "retention", current: retention},
		{name: "a consumer added", current: added},
		{name: "a consumer under another name", current: renamed},
		{name: "important", current: consumer(func(c *ast.TopicConsumerSpec) { c.Important = false })},
		{name: "read_from", current: consumer(func(c *ast.TopicConsumerSpec) { c.ReadFrom = "2026-01-01T00:00:00Z" })},
		{name: "codecs", current: consumer(func(c *ast.TopicConsumerSpec) { c.SupportedCodecs = []string{"raw"} })},
		{name: "availability", current: consumer(func(c *ast.TopicConsumerSpec) { c.AvailabilityPeriod = "PT1H" })},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbchangefeed.TopicChanged(base(), test.current), qt.IsTrue)
			c.Assert(ydbchangefeed.Recreated(base(), test.current), qt.IsFalse)
		})
	}
}

func TestListsEqual(t *testing.T) {
	other := base()
	other.Name = "other"
	changed := base()
	changed.Mode = "KEYS_ONLY"
	tests := []struct {
		name             string
		desired, current []ydbschema.ChangefeedSpec
		want             bool
	}{
		{name: "both empty", want: true},
		{name: "the same two in another order", desired: []ydbschema.ChangefeedSpec{base(), other},
			current: []ydbschema.ChangefeedSpec{other, base()}, want: true},
		{name: "one missing", desired: []ydbschema.ChangefeedSpec{base(), other}, current: []ydbschema.ChangefeedSpec{base()}},
		{name: "one changed", desired: []ydbschema.ChangefeedSpec{base()}, current: []ydbschema.ChangefeedSpec{changed}},
		{name: "another name", desired: []ydbschema.ChangefeedSpec{base()}, current: []ydbschema.ChangefeedSpec{other}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbchangefeed.ListsEqual(test.desired, test.current), qt.Equals, test.want)
		})
	}
}
