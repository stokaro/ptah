package capabilityprobe

// White-box testing required: which paths an experiment creates is read off
// the plan's unexported experiments -- their setup, the statements they carry
// and the paths their deciders declare -- and no exported entry point lists
// them without a server.

import (
	"maps"
	"regexp"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
)

var (
	// createdObject matches a YQL statement that creates an object at a path
	// of the namespace, and captures the path.
	createdObject = regexp.MustCompile("(?is)^\\s*CREATE\\s+(?:OR\\s+REPLACE\\s+)?" +
		"(?:TABLE|TOPIC|VIEW|TRANSFER|ASYNC\\s+REPLICATION|SEQUENCE|COORDINATION\\s+NODE|RESOURCE\\s+POOL|" +
		"SECRET|EXTERNAL\\s+DATA\\s+SOURCE|EXTERNAL\\s+TABLE)\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?" +
		"(`[^`]+`|[^\\s(]+)")
	// replicaTarget matches each `AS <target>` of a CREATE ASYNC REPLICATION,
	// a replica table it creates.
	replicaTarget = regexp.MustCompile("(?i)\\bAS\\s+(`[^`]+`|[^\\s,(]+)")
	// addedToTable matches an ALTER TABLE that adds a changefeed or an index,
	// each a path under the table.
	addedToTable = regexp.MustCompile("(?is)^\\s*ALTER\\s+TABLE\\s+(`[^`]+`|\\S+)\\s+ADD\\s+" +
		"(?:CHANGEFEED|INDEX)\\s+(`[^`]+`|\\S+)")
)

// createdPaths lists the namespace paths one YQL statement creates.
func createdPaths(statement string) []string {
	unquote := func(name string) string { return strings.Trim(name, "`") }
	if match := addedToTable.FindStringSubmatch(statement); match != nil {
		return []string{unquote(match[1]) + "/" + unquote(match[2])}
	}
	match := createdObject.FindStringSubmatch(statement)
	if match == nil {
		return nil
	}
	paths := []string{unquote(match[1])}
	if fields := strings.Fields(strings.ToUpper(statement)); len(fields) > 2 && fields[1] == "ASYNC" {
		head, _, _ := strings.Cut(statement, " WITH ")
		for _, target := range replicaTarget.FindAllStringSubmatch(head, -1) {
			paths = append(paths, unquote(target[1]))
		}
	}
	return paths
}

// experimentPaths lists every namespace path an experiment creates.
func experimentPaths(e experiment) []string {
	var paths []string
	for _, statement := range slices.Concat(e.setup, e.runs) {
		paths = append(paths, createdPaths(statement)...)
	}
	return append(paths, e.creates...)
}

// TestYDBPlan_ExperimentsCreateDistinctPaths holds every experiment of the YDB
// plan to paths no other experiment creates. The run shares one namespace
// directory between them and drops nothing until it ends, so a second
// experiment creating a path is refused for the path, and its refusal is
// scored as the answer about its key.
//
// The paths come from the statements an experiment carries as data and the
// paths a decider that builds its statements at run time declares in
// creates; such a decider that creates a path without declaring it is outside
// what this test can read.
func TestYDBPlan_ExperimentsCreateDistinctPaths(t *testing.T) {
	c := qt.New(t)
	p, ok := planFor(platform.YDB)
	c.Assert(ok, qt.IsTrue)

	owners := make(map[string][]string)
	for _, e := range p.experiments {
		keys := make([]string, 0, len(e.decides))
		for _, key := range e.decides {
			keys = append(keys, string(key))
		}
		owner := strings.Join(keys, ",")
		for _, created := range slices.Compact(slices.Sorted(slices.Values(experimentPaths(e)))) {
			owners[created] = append(owners[created], owner)
		}
	}
	shared := make(map[string][]string)
	for created, experiments := range owners {
		if len(experiments) > 1 {
			shared[created] = experiments
		}
	}

	c.Assert(len(owners) > 40, qt.IsTrue, qt.Commentf("the YDB plan created only %d paths: %v", len(owners),
		slices.Sorted(maps.Keys(owners))))
	// One path from each source the check reads: a setup statement, a change
	// a schemaChange carries, and the paths a run-time decider declares.
	for _, created := range []string{"cfk/feed", "tpk", "xfer_key", "repl_replica"} {
		c.Assert(owners[created], qt.HasLen, 1, qt.Commentf("path %s", created))
	}
	c.Assert(shared, qt.DeepEquals, map[string][]string{})
}

// The paths a statement creates are read as the probe's statements spell
// them: a quoted path, a name before its column list, a changefeed or index
// under its table, and the replica tables of a replication.
func TestCreatedPaths(t *testing.T) {
	tests := []struct {
		statement string
		want      []string
	}{
		{statement: "CREATE TABLE xfer_src (id Uint64 NOT NULL, PRIMARY KEY (id))", want: []string{"xfer_src"}},
		{statement: "CREATE TABLE `dir/t`(id Uint64)", want: []string{"dir/t"}},
		{statement: "create topic IF NOT EXISTS tpk (CONSUMER c)", want: []string{"tpk"}},
		{statement: "ALTER TABLE cfk ADD CHANGEFEED feed WITH (MODE = 'UPDATES')", want: []string{"cfk/feed"}},
		{statement: "CREATE ASYNC REPLICATION repl_key FOR `/local/ns/a` AS repl_replica, b AS `dir/rb` WITH (" +
			"CONNECTION_STRING = 'grpc://h:2136/?database=/local')",
			want: []string{"repl_key", "repl_replica", "dir/rb"}},
		{statement: "INSERT INTO t (id) VALUES (1)", want: nil},
	}
	for _, test := range tests {
		t.Run(test.statement, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(createdPaths(test.statement), qt.DeepEquals, test.want)
		})
	}
}
