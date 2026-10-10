//go:build integration

package ydb_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/engine/builtin"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/featurejson"
)

// featureJSONSchema is the directory the drift document run writes into.
const featureJSONSchema = "ptah_ydb_feature_json_e2e"

// coordinationDocument declares one coordination node in that directory.
func coordinationDocument(mode string) string {
	return "coordination_nodes:\n  locks:\n    schema: " + featureJSONSchema + "\n    read_consistency_mode: " + mode + "\n"
}

// TestYDBBinary_DriftJSONCarriesACoordinationNodeChange drives the shipped
// binary against a coordination node whose read consistency differs from the
// declared one. The drift is a standalone owner change at the diff's own
// scope, and `schema drift --format json` exited 2 with "feature data
// requires an explicit codec registry" instead of writing it
// (stokaro/ptah#4279). The document now carries the change, exits 1 as drift
// does, and reads back through the codecs as the change the comparison made.
func TestYDBBinary_DriftJSONCarriesACoordinationNodeChange(t *testing.T) {
	c := qt.New(t)
	binary := buildBinary(c, c.Context())
	codecs := must.Must(builtin.New()).Codecs()
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			url := dbtarget.URL(t, line.engine)
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			conn := openYDB(c, line)
			dropCoordinationDirectory(c, conn, featureJSONSchema)
			c.Cleanup(func() { dropCoordinationDirectory(c, conn, featureJSONSchema) })
			dir := c.TempDir()
			live := filepath.Join(dir, "live.yaml")
			desired := filepath.Join(dir, "desired.yaml")
			c.Assert(os.WriteFile(live, []byte(coordinationDocument("relaxed")), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(desired, []byte(coordinationDocument("strict")), 0o600), qt.IsNil)
			scope := []string{"--db-url", url, "--schemas", featureJSONSchema}

			applied, applyErr := runBinary(ctx, binary, append([]string{"schema", "apply", "--schema-file", live, "--auto-approve"}, scope...)...)
			c.Assert(applyErr, qt.IsNil, qt.Commentf("apply:\n%s", applied))
			stdout, stderr, exit := runBinaryStreams(ctx, binary, append([]string{"schema", "drift", "--schema-file", desired, "--format", "json"}, scope...)...)

			c.Assert(exit, qt.Equals, 1, qt.Commentf("stderr:\n%s", stderr))
			var document struct {
				Drift bool `json:"drift"`
				Diff  struct {
					FeatureChanges []schemaext.ChangeRecord `json:"feature_changes"`
				} `json:"diff"`
			}
			c.Assert(featurejson.Unmarshal(c.Context(), codecs, schemaext.Desired, []byte(stdout), &document), qt.IsNil, qt.Commentf("stdout:\n%s", stdout))
			c.Assert(document.Drift, qt.IsTrue)
			c.Assert(document.Diff.FeatureChanges, qt.HasLen, 1)
			c.Assert(document.Diff.FeatureChanges[0].Subject.Name.Source, qt.Equals, "locks")
			change, ok := document.Diff.FeatureChanges[0].Value.(*ydbdiff.CoordinationNode)
			c.Assert(ok, qt.IsTrue, qt.Commentf("decoded %T", document.Diff.FeatureChanges[0].Value))
			c.Assert(change.Before.Spec.ReadConsistencyMode, qt.Equals, "relaxed")
			c.Assert(change.After.Spec, qt.DeepEquals, ydbcoordination.Spec{ReadConsistencyMode: "strict"})
			c.Assert(stdout, qt.Contains, `"kind": "ptah.run/ydb/coordination-node-change"`)
		})
	}
}

// runBinaryStreams runs the binary and returns its two streams apart and its
// exit status, because a JSON document is the standard output alone and a
// drift exits 1.
func runBinaryStreams(ctx context.Context, binary string, args ...string) (stdout, stderr string, exit int) {
	var out, errOut bytes.Buffer
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Stdout, cmd.Stderr = &out, &errOut
	err := cmd.Run()
	var exited *exec.ExitError
	switch {
	case err == nil:
		return out.String(), errOut.String(), 0
	case errors.As(err, &exited):
		return out.String(), errOut.String(), exited.ExitCode()
	default:
		return out.String(), errOut.String() + err.Error(), -1
	}
}
