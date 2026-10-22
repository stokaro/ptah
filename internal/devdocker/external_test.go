package devdocker_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devdocker"
)

type fakeOwner struct {
	engines  []string
	url      string
	err      error
	requests []string
	released int
}

func (o *fakeOwner) DevEngines() []string { return o.engines }

func (o *fakeOwner) ProvisionDev(_ context.Context, rawURL string) (string, func(), error) {
	o.requests = append(o.requests, rawURL)
	if o.err != nil {
		return "", nil, o.err
	}
	return o.url, func() { o.released++ }, nil
}

func TestResolveHandsAnOwnedEngineToItsOwner_HappyPath(t *testing.T) {
	c := qt.New(t)
	owner := &fakeOwner{engines: []string{"owned"}, url: "owned://localhost/dev-1"}
	ctx := devdocker.WithExternal(c.Context(), owner)

	resolved, release, err := devdocker.Resolve(ctx, "docker://owned/1.2/dev", devdocker.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(resolved, qt.Equals, "owned://localhost/dev-1")
	c.Assert(owner.requests, qt.DeepEquals, []string{"docker://owned/1.2/dev"})
	c.Assert(devdocker.RunOwned(resolved), qt.IsTrue)
	c.Assert(devdocker.Provisioned(resolved), qt.IsTrue)

	release()
	release()
	c.Assert(owner.released, qt.Equals, 1)
	c.Assert(devdocker.RunOwned(resolved), qt.IsFalse)
	c.Assert(devdocker.Provisioned(resolved), qt.IsFalse)
}

func TestResolveHandsAnOwnedEngineToItsOwner_FailurePath(t *testing.T) {
	c := qt.New(t)
	refusal := errors.New("owner refuses")
	owner := &fakeOwner{engines: []string{"owned"}, err: refusal}
	ctx := devdocker.WithExternal(c.Context(), owner)

	resolved, release, err := devdocker.Resolve(ctx, "docker://owned", devdocker.Options{})
	c.Assert(err, qt.ErrorIs, refusal)
	c.Assert(resolved, qt.Equals, "")
	release()
	c.Assert(owner.released, qt.Equals, 0)
}

func TestResolveLeavesOtherEnginesAlone(t *testing.T) {
	c := qt.New(t)
	owner := &fakeOwner{engines: []string{"owned"}, url: "owned://localhost/dev-1"}
	ctx := devdocker.WithExternal(c.Context(), owner)

	// Without the owner, an unknown engine is refused by the built-in table.
	_, _, err := devdocker.Resolve(c.Context(), "docker://owned/1", devdocker.Options{})
	c.Assert(err, qt.ErrorMatches, `.*unsupported docker image "owned".*`)
	// With another engine's URL the owner is not asked.
	_, _, err = devdocker.Resolve(ctx, "docker://nosuch/1", devdocker.Options{})
	c.Assert(err, qt.ErrorMatches, `.*unsupported docker image "nosuch".*`)
	c.Assert(owner.requests, qt.HasLen, 0)
	// A URL that is not a docker URL passes through untouched.
	resolved, release, err := devdocker.Resolve(ctx, "postgres://localhost/dev", devdocker.Options{})
	c.Assert(err, qt.IsNil)
	release()
	c.Assert(resolved, qt.Equals, "postgres://localhost/dev")
	c.Assert(owner.requests, qt.HasLen, 0)
}
