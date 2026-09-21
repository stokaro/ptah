package schemaartifact_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"oras.land/oras-go/v2/content/memory"

	"ptah.run/internal/schemaartifact"
)

const publishedChecks = `-- +ptah check name="every user has a tier" assert="SELECT COUNT(*) = 0 FROM users WHERE tier IS NULL"
`

// The checks a reviewer approved and the checks that run afterwards are two
// different things when the second is read from a path. A layer beside the
// schema makes them one set of bytes, addressed by the artifact's digest
// (stokaro/ptah#3458).
func TestPushToPullFrom_CarriesTheChecksLayer(t *testing.T) {
	c := qt.New(t)
	store := memory.New()

	_, err := schemaartifact.PushTo(context.Background(), store, usersDatabase(), schemaartifact.PushOptions{
		Tags:   []string{"v1"},
		Checks: []byte(publishedChecks),
	})
	c.Assert(err, qt.IsNil)

	pulled, err := schemaartifact.PullFrom(context.Background(), store, "v1")

	c.Assert(err, qt.IsNil)
	c.Assert(string(pulled.Checks), qt.Equals, publishedChecks)
}

// An artifact published without checks carries no layer and says so by
// carrying no bytes, which is what lets a reader tell "this artifact publishes
// none" from "these are the assertions".
func TestPushToPullFrom_CarriesNoChecksLayerWhenNoneWerePublished(t *testing.T) {
	c := qt.New(t)
	store := memory.New()

	_, err := schemaartifact.PushTo(context.Background(), store, usersDatabase(), schemaartifact.PushOptions{
		Tags: []string{"v1"},
	})
	c.Assert(err, qt.IsNil)

	pulled, err := schemaartifact.PullFrom(context.Background(), store, "v1")

	c.Assert(err, qt.IsNil)
	c.Assert(pulled.Checks, qt.IsNil)
}

// Replacing the checks changes the artifact, which is the property the whole
// layer rests on: a digest names one set of bytes, so a reference pinned at
// review time cannot be made to evaluate something else.
func TestPushTo_ChangingTheChecksChangesTheDigest(t *testing.T) {
	c := qt.New(t)
	store := memory.New()

	approved, err := schemaartifact.PushTo(context.Background(), store, usersDatabase(), schemaartifact.PushOptions{
		Tags:   []string{"v1"},
		Checks: []byte(publishedChecks),
	})
	c.Assert(err, qt.IsNil)

	replaced, err := schemaartifact.PushTo(context.Background(), store, usersDatabase(), schemaartifact.PushOptions{
		Tags: []string{"v2"},
		Checks: []byte(
			"-- +ptah check name=\"anything\" assert=\"SELECT 1\"\n",
		),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(replaced.Descriptor.Digest, qt.Not(qt.Equals), approved.Descriptor.Digest)
}
