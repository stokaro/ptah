//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"oras.land/oras-go/v2/content/memory"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/clirun"
	"ptah.run/internal/ociartifact"
	"ptah.run/internal/schemaartifact"
)

// futureManagedDataLayerMediaType is the row layer as a later Ptah would type
// it. Nothing in this build writes it, which is the point: it stands for
// content this executor can fetch and must not act on.
const futureManagedDataLayerMediaType = "application/vnd.stokaro.ptah.managed-data.v2+json"

// unreachableDatabaseURL is a database nothing answers on. It is the instrument
// rather than the subject: a run that reports this address has opened a
// connection, and a run that reports the artifact has not.
const unreachableDatabaseURL = "postgres://ptah:ptah@127.0.0.1:1/ptah?sslmode=disable&connect_timeout=1"

// TestSchemaArtifactFromANewerPtahRefusesBeforeTheDatabaseE2E is the row
// stokaro/ptah-operator#41 carries: an executor meets an artifact built by a
// newer Ptah, and refuses before the mutation with no data skipped.
//
// ADR 0019 puts the refusal before a database connection exists. The mechanism
// is the layer media type -- section 3.2's reading rule, where an unknown layer
// refuses the whole artifact rather than being read around -- and this measures
// that the refusal really does land on that side of the connection, through the
// shipped binary rather than through a package boundary.
//
// The control is what makes the measurement mean something. The same files,
// typed as this build writes them, take the same command past verification and
// into the connection it then fails to open, so the refusal above is the
// artifact and not the address (stokaro/ptah#3291).
func TestSchemaArtifactFromANewerPtahRefusesBeforeTheDatabaseE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	registry := requiredOCIRegistry(t)
	suffix := time.Now().UnixNano()

	future := fmt.Sprintf("oci://%s/ptah/future-layer-%d:latest", registry, suffix)
	pushArtifactLayers(c, ctx, future, futureManagedDataLayerMediaType)
	current := fmt.Sprintf("oci://%s/ptah/current-layer-%d:latest", registry, suffix)
	pushArtifactLayers(c, ctx, current, schemaartifact.ManagedDataLayerMediaType)

	refused := clirun.Run(c, clirun.Ptah, clirun.Options{},
		"schema", "apply", "--schema-file", future,
		"--db-url", unreachableDatabaseURL, "--auto-approve", "--plain-http")

	c.Assert(refused.ExitCode, qt.Not(qt.Equals), 0,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", refused.Stdout, refused.Stderr))
	c.Assert(refused.Stderr, qt.Contains, schemaartifact.ManagedDataFileName)
	// The database was never opened, so nothing names it.
	c.Assert(refused.Stderr, qt.Not(qt.Contains), "127.0.0.1:1")
	c.Assert(refused.Stderr, qt.Not(qt.Contains), "connect")

	// The control: the same files, typed as this build writes them.
	reached := clirun.Run(c, clirun.Ptah, clirun.Options{},
		"schema", "apply", "--schema-file", current,
		"--db-url", unreachableDatabaseURL, "--auto-approve", "--plain-http")

	c.Assert(reached.ExitCode, qt.Not(qt.Equals), 0,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", reached.Stdout, reached.Stderr))
	c.Assert(reached.Stderr, qt.Contains, "127.0.0.1:1")
}

// pushArtifactLayers publishes one schema artifact whose row layer carries the
// given media type, with everything else as this build writes it.
//
// The files come out of a real publish into memory, so the artifact under test
// differs from a published one in the one axis the test is about.
func pushArtifactLayers(c *qt.C, ctx context.Context, reference, managedDataMediaType string) {
	c.Helper()

	store := memory.New()
	_, err := schemaartifact.PushTo(ctx, store, futureLayerDatabase(), schemaartifact.PushOptions{
		Tags: []string{"source"},
	})
	c.Assert(err, qt.IsNil)
	published, err := schemaartifact.PullFrom(ctx, store, "source")
	c.Assert(err, qt.IsNil)
	// The annotations come from the publish, so the re-push differs from a
	// published artifact in the layer's media type and in nothing else.
	source, err := ociartifact.PullFrom(ctx, store, "source", ociartifact.PullOptions{
		ExpectedArtifactTypes:   []string{ociartifact.SchemaArtifactType},
		AcceptedLayerMediaTypes: []string{schemaartifact.LayerMediaType, schemaartifact.ManagedDataLayerMediaType},
	})
	c.Assert(err, qt.IsNil)

	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: true})
	c.Assert(err, qt.IsNil)
	_, err = client.Push(ctx, reference, published.FileSystem, ociartifact.PushOptions{
		ArtifactType:   ociartifact.SchemaArtifactType,
		LayerMediaType: schemaartifact.LayerMediaType,
		LayerMediaTypes: map[string]string{
			schemaartifact.ManagedDataFileName: managedDataMediaType,
		},
		Annotations: source.Annotations,
		Tags:        []string{"latest"},
	})
	c.Assert(err, qt.IsNil)
}

func futureLayerDatabase() *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Region", Name: "regions"}},
		Fields: []schemamodel.Field{
			{StructName: "Region", FieldName: "Code", Name: "code", Type: "text", Primary: true},
			{StructName: "Region", FieldName: "Name", Name: "name", Type: "text"},
		},
		ManagedData: []schemamodel.ManagedData{{
			StructName: "Region",
			Table:      "regions",
			Keys:       []string{"code"},
			File:       "regions.yaml",
			Rows: []schemamodel.ManagedRow{{
				"code": {Tag: "str", Text: "NO"},
				"name": {Tag: "str", Text: "Norway"},
			}},
		}},
	}
	schemamodel.Finalize(db)
	return db
}
