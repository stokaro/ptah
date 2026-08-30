package inference_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"go.yaml.in/yaml/v3"

	"go.5x5.cz/ptah/cmd/inference"
)

// exampleDir is where the Kubernetes manifests live, relative to this package.
const exampleDir = "../../examples/kubernetes"

// TestKubernetesExample_EveryCommandIsOneThisBuildHas keeps the manifests tied
// to the command tree.
//
// A manifest is not run by anything here -- it needs a cluster -- so the failure
// it would otherwise have is silent and slow: a verb or a flag renamed in the
// code, the example still naming the old spelling, and the first person to find
// out is an operator whose Job crashes in a namespace they cannot easily debug.
//
// What this can establish without a cluster is that every argv the manifests
// carry names a verb this build registers and flags that verb accepts. That is
// the drift that actually happens (stokaro/ptah#2068).
func TestKubernetesExample_EveryCommandIsOneThisBuildHas(t *testing.T) {
	c := qt.New(t)
	invocations := inferenceInvocations(c)

	// A floor, so a walk that stopped finding containers reports as a failure
	// rather than as a clean run over nothing. Six today: prepare, backfill,
	// catchup, index, verify, status, cutover and the maintenance catch-up.
	c.Assert(len(invocations) >= 6, qt.IsTrue,
		qt.Commentf("found %d ptah invocations in %s", len(invocations), exampleDir))

	for _, invocation := range invocations {
		t.Run(strings.Join(invocation, " "), func(t *testing.T) {
			c := qt.New(t)
			command, remaining, err := inference.NewCommand().Find(invocation[1:])
			c.Assert(err, qt.IsNil)
			c.Assert(command.Name(), qt.Not(qt.Equals), "inference",
				qt.Commentf("%q names no verb", invocation))
			c.Assert(command.ParseFlags(remaining), qt.IsNil)
		})
	}
}

// TestKubernetesExample_NoSecretIsOnACommandLine is the property the manifests
// exist to demonstrate.
//
// A pod specification is readable by anything that can read the namespace, and
// `kubectl get pod -o yaml` prints argv in full. The database URL carries a
// password and the provider token is one, so both reach Ptah through the
// environment -- and an example that quietly stopped doing that would still
// apply cleanly and still work.
func TestKubernetesExample_NoSecretIsOnACommandLine(t *testing.T) {
	c := qt.New(t)
	invocations := inferenceInvocations(c)

	for _, invocation := range invocations {
		line := strings.Join(invocation, " ")
		c.Assert(line, qt.Not(qt.Contains), "--db-url",
			qt.Commentf("the database URL carries a password and belongs in PTAH_DB_URL"))
		c.Assert(line, qt.Not(qt.Contains), "postgres://")
		c.Assert(line, qt.Not(qt.Contains), "--credential")
	}
}

// inferenceInvocations reads every container argv in the manifests that starts
// with an inference verb.
//
// The manifests are parsed as YAML rather than scanned as text, because a
// scanner would find the same strings inside the comments that explain them and
// would keep passing after somebody deleted the container they document.
func inferenceInvocations(c *qt.C) [][]string {
	c.Helper()
	entries, err := os.ReadDir(exampleDir)
	c.Assert(err, qt.IsNil)

	var invocations [][]string
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) != ".yaml" {
			continue
		}
		body, err := os.ReadFile(filepath.Join(exampleDir, entry.Name()))
		c.Assert(err, qt.IsNil)
		invocations = append(invocations, argsInDocuments(c, body)...)
	}
	return invocations
}

// argsInDocuments walks every YAML document in one file.
func argsInDocuments(c *qt.C, body []byte) [][]string {
	c.Helper()
	var invocations [][]string
	decoder := yaml.NewDecoder(strings.NewReader(string(body)))
	for {
		var document any
		err := decoder.Decode(&document)
		if err != nil {
			// io.EOF ends the file, and anything else is a manifest that would
			// not apply -- which is worth failing on here rather than in a
			// cluster.
			c.Assert(err.Error(), qt.Equals, "EOF")
			return invocations
		}
		invocations = append(invocations, argsIn(document)...)
	}
}

// argsIn collects every `args` list whose first element is "inference".
//
// A recursive walk rather than a path, because the same list appears at four
// depths -- a Job's containers, its initContainers, a Deployment's
// initContainers, and a CronJob's jobTemplate -- and a walk that named each
// would report nothing for a shape somebody adds next.
func argsIn(node any) [][]string {
	switch value := node.(type) {
	case map[string]any:
		return argsInMapping(value)
	case []any:
		var found [][]string
		for _, element := range value {
			found = append(found, argsIn(element)...)
		}
		return found
	}
	return nil
}

// argsInMapping is the mapping half of the walk.
func argsInMapping(mapping map[string]any) [][]string {
	var found [][]string
	if args, ok := stringList(mapping["args"]); ok && len(args) > 0 && args[0] == "inference" {
		found = append(found, args)
	}
	for key, child := range mapping {
		if key == "args" {
			continue
		}
		found = append(found, argsIn(child)...)
	}
	return found
}

// stringList reads a YAML sequence of scalars.
func stringList(node any) ([]string, bool) {
	elements, ok := node.([]any)
	if !ok {
		return nil, false
	}
	values := make([]string, 0, len(elements))
	for _, element := range elements {
		text, ok := element.(string)
		if !ok {
			return nil, false
		}
		values = append(values, text)
	}
	return values, true
}

// TestKubernetesExample_TheWalkFindsTheShapesTheManifestsUse is the control for
// the reader above.
//
// A walk that returned nothing would make both tests pass over an empty list.
// This pins the four shapes the manifests actually use, so a reader that
// stopped descending into one of them fails here rather than reporting a clean
// run.
func TestKubernetesExample_TheWalkFindsTheShapesTheManifestsUse(t *testing.T) {
	c := qt.New(t)
	var verbs []string
	for _, invocation := range inferenceInvocations(c) {
		verbs = append(verbs, invocation[1])
	}

	for _, verb := range []string{"prepare", "backfill", "catchup", "index", "verify", "status", "cutover"} {
		c.Assert(verbs, qt.Contains, verb,
			qt.Commentf("the manifests carry no %s container, or the walk did not find it", verb))
	}
}
