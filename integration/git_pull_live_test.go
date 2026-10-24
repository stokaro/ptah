//go:build integration

package integration_test

import (
	"context"
	"os"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/capability"
	gittransport "github.com/go-git/go-git/v5/plumbing/transport"

	"ptah.run/internal/gitsource"
	"ptah.run/internal/gitsource/gitsourcetest"
	"ptah.run/internal/sourcecontent"
)

const (
	gitUser     = "deploy"
	gitPassword = "s3cret"
	schemaV1    = "table \"users\" {\n  schema = schema.public\n}\n"
	schemaV2    = "table \"users\" {\n  schema = schema.public\n}\ntable \"orders\" {\n  schema = schema.public\n}\n"
)

// gitFetch fetches over HTTPS from a server that requires the credential.
func gitFetch(c *qt.C, server *gitsourcetest.HTTPS, options gitsource.Options) (gitsource.Result, error) {
	reference, err := gitsource.ParseReference(server.Reference("app.git"))
	c.Assert(err, qt.IsNil)
	transport, auth, err := gitsource.Dial(reference,
		gitsource.Credentials{Username: gitUser, Password: gitPassword}, gitsource.Trust{CAFile: server.CAFile})
	c.Assert(err, qt.IsNil)
	options.Reference, options.Transport, options.Auth = reference, transport, auth
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	return gitsource.Fetch(ctx, options)
}

// sshFetch fetches over SSH from a server that exports repository.
func sshFetch(c *qt.C, repository *gitsourcetest.Repository, options gitsource.Options) (gitsource.Result, error) {
	server := gitsourcetest.ServeSSH(c, repository.Root)
	reference, err := gitsource.ParseReference(server.Reference("app.git"))
	c.Assert(err, qt.IsNil)
	key, err := os.ReadFile(server.KeyFile)
	c.Assert(err, qt.IsNil)
	dialer, auth, err := gitsource.Dial(reference, gitsource.Credentials{SSHKey: key}, gitsource.Trust{KnownHostsFile: server.KnownHostsFile})
	c.Assert(err, qt.IsNil)
	options.Reference, options.Transport, options.Auth = reference, dialer, auth
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	return gitsource.Fetch(ctx, options)
}

// advertisesCommitsByID reports whether the HTTPS server says, in its ref
// advertisement, that it gives commits by their id.
func advertisesCommitsByID(c *qt.C, server *gitsourcetest.HTTPS) bool {
	reference, err := gitsource.ParseReference(server.Reference("app.git"))
	c.Assert(err, qt.IsNil)
	dialer, auth, err := gitsource.Dial(reference,
		gitsource.Credentials{Username: gitUser, Password: gitPassword}, gitsource.Trust{CAFile: server.CAFile})
	c.Assert(err, qt.IsNil)
	endpoint, err := gittransport.NewEndpoint(reference.URL.String())
	c.Assert(err, qt.IsNil)
	session, err := dialer.NewUploadPackSession(endpoint, auth)
	c.Assert(err, qt.IsNil)
	defer func() { _ = session.Close() }()
	advertised, err := session.AdvertisedReferencesContext(context.Background())
	c.Assert(err, qt.IsNil)
	return advertised.Capabilities.Supports(capability.AllowReachableSHA1InWant) ||
		advertised.Capabilities.Supports(capability.AllowTipSHA1InWant)
}

func schemaDigest(c *qt.C, content string) string {
	digest, err := sourcecontent.Digest([]sourcecontent.File{{Path: "schema.hcl", Data: []byte(content)}})
	c.Assert(err, qt.IsNil)
	return digest
}

// history is a repository whose main went from v1 to v2, served over HTTPS.
type history struct {
	repository *gitsourcetest.Repository
	server     *gitsourcetest.HTTPS
	first      string
	second     string
}

func newHistory(c *qt.C) history {
	repository := gitsourcetest.NewRepository(c)
	first := repository.Commit(c, map[string]string{"db/schema.hcl": schemaV1}, "v1")
	repository.AnnotatedTag(c, "v1")
	repository.Tag(c, "light-v1")
	second := repository.Commit(c, map[string]string{"db/schema.hcl": schemaV2}, "v2")
	server := gitsourcetest.ServeHTTPS(c, repository.Root, gitsourcetest.HTTPSOptions{Username: gitUser, Password: gitPassword})
	return history{repository: repository, server: server, first: first, second: second}
}

// newUntagged is a history whose first commit no ref names any more.
func newUntagged(c *qt.C) history {
	repository := gitsourcetest.NewRepository(c)
	first := repository.Commit(c, map[string]string{"db/schema.hcl": schemaV1}, "v1")
	second := repository.Commit(c, map[string]string{"db/schema.hcl": schemaV2}, "v2")
	server := gitsourcetest.ServeHTTPS(c, repository.Root, gitsourcetest.HTTPSOptions{Username: gitUser, Password: gitPassword})
	return history{repository: repository, server: server, first: first, second: second}
}

func TestGitFetch_HappyPath(t *testing.T) {
	t.Run("a branch gives its tip", func(t *testing.T) {
		c := qt.New(t)
		h := newHistory(c)
		result, err := gitFetch(c, h.server, gitsource.Options{Ref: gitsource.Ref{Branch: "main"}, Path: "db/schema.hcl", Kind: gitsource.File})
		c.Assert(err, qt.IsNil)
		c.Assert(result.Commit, qt.Equals, h.second)
		c.Assert(result.ContentDigest, qt.Equals, schemaDigest(c, schemaV2))
		c.Assert(result.Files, qt.DeepEquals, []sourcecontent.File{{Path: "schema.hcl", Data: []byte(schemaV2)}})
	})
	t.Run("an annotated tag gives the commit it names", func(t *testing.T) {
		c := qt.New(t)
		h := newHistory(c)
		result, err := gitFetch(c, h.server, gitsource.Options{Ref: gitsource.Ref{Tag: "v1"}, Path: "db/schema.hcl", Kind: gitsource.File})
		c.Assert(err, qt.IsNil)
		c.Assert(result.Commit, qt.Equals, h.first)
		c.Assert(result.ContentDigest, qt.Equals, schemaDigest(c, schemaV1))
	})
	t.Run("a lightweight tag gives its commit", func(t *testing.T) {
		c := qt.New(t)
		h := newHistory(c)
		result, err := gitFetch(c, h.server, gitsource.Options{Ref: gitsource.Ref{Tag: "light-v1"}, Path: "db/schema.hcl", Kind: gitsource.File})
		c.Assert(err, qt.IsNil)
		c.Assert(result.Commit, qt.Equals, h.first)
	})
	t.Run("a commit the branch moved past is found along it", func(t *testing.T) {
		c := qt.New(t)
		u := newUntagged(c)
		result, err := gitFetch(c, u.server, gitsource.Options{
			Ref:  gitsource.Ref{Branch: "main", Commit: u.first},
			Path: "db/schema.hcl", Kind: gitsource.File, ExpectDigest: schemaDigest(c, schemaV1),
		})
		c.Assert(err, qt.IsNil)
		c.Assert(result.Commit, qt.Equals, u.first)
	})
	t.Run("a commit no ref names, from a server that gives commits by id", func(t *testing.T) {
		c := qt.New(t)
		u := newUntagged(c)
		u.repository.Config(c, "uploadpack.allowReachableSHA1InWant", "true")
		c.Assert(advertisesCommitsByID(c, u.server), qt.IsTrue)
		result, err := gitFetch(c, u.server, gitsource.Options{Ref: gitsource.Ref{Commit: u.first}, Path: "db/schema.hcl", Kind: gitsource.File})
		c.Assert(err, qt.IsNil)
		c.Assert(result.Commit, qt.Equals, u.first)
		c.Assert(result.ContentDigest, qt.Equals, schemaDigest(c, schemaV1))
	})
	// Git's smart HTTP server gives a commit a branch reaches without saying
	// so: each request is answered on its own, against the refs as they are.
	t.Run("a commit no ref names, over HTTPS from a server that does not say it gives commits by id", func(t *testing.T) {
		c := qt.New(t)
		u := newUntagged(c)
		c.Assert(advertisesCommitsByID(c, u.server), qt.IsFalse)
		result, err := gitFetch(c, u.server, gitsource.Options{Ref: gitsource.Ref{Commit: u.first}, Path: "db/schema.hcl", Kind: gitsource.File})
		c.Assert(err, qt.IsNil)
		c.Assert(result.Commit, qt.Equals, u.first)
		c.Assert(result.ContentDigest, qt.Equals, schemaDigest(c, schemaV1))
	})
	t.Run("a commit the branch moved past is found along it over SSH", func(t *testing.T) {
		c := qt.New(t)
		u := newUntagged(c)
		result, err := sshFetch(c, u.repository, gitsource.Options{
			Ref:  gitsource.Ref{Branch: "main", Commit: u.first},
			Path: "db/schema.hcl", Kind: gitsource.File, ExpectDigest: schemaDigest(c, schemaV1),
		})
		c.Assert(err, qt.IsNil)
		c.Assert(result.Commit, qt.Equals, u.first)
	})
	t.Run("a commit alone that a tag names, from any server", func(t *testing.T) {
		c := qt.New(t)
		h := newHistory(c)
		result, err := gitFetch(c, h.server, gitsource.Options{Ref: gitsource.Ref{Commit: h.first}, Path: "db/schema.hcl", Kind: gitsource.File})
		c.Assert(err, qt.IsNil)
		c.Assert(result.Commit, qt.Equals, h.first)
	})
	t.Run("a migration directory gives its files, nested ones included", func(t *testing.T) {
		c := qt.New(t)
		repository := gitsourcetest.NewRepository(c)
		files := map[string]string{
			"db/migrations/0001_create.up.sql":   "CREATE TABLE t (id int);\n",
			"db/migrations/0001_create.down.sql": "DROP TABLE t;\n",
			"db/migrations/data/seed.sql":        "INSERT INTO t VALUES (1);\n",
			"db/schema.hcl":                      schemaV1,
		}
		commit := repository.Commit(c, files, "migrations")
		server := gitsourcetest.ServeHTTPS(c, repository.Root, gitsourcetest.HTTPSOptions{Username: gitUser, Password: gitPassword})
		result, err := gitFetch(c, server, gitsource.Options{Ref: gitsource.Ref{Branch: "main"}, Path: "db/migrations", Kind: gitsource.Directory})
		c.Assert(err, qt.IsNil)
		c.Assert(result.Commit, qt.Equals, commit)
		want := []sourcecontent.File{
			{Path: "0001_create.down.sql", Data: []byte(files["db/migrations/0001_create.down.sql"])},
			{Path: "0001_create.up.sql", Data: []byte(files["db/migrations/0001_create.up.sql"])},
			{Path: "data/seed.sql", Data: []byte(files["db/migrations/data/seed.sql"])},
		}
		c.Assert(result.Files, qt.ContentEquals, want)
		digest, err := sourcecontent.Digest(want)
		c.Assert(err, qt.IsNil)
		c.Assert(result.ContentDigest, qt.Equals, digest)
	})
	t.Run("SSH with the server's host key", func(t *testing.T) {
		c := qt.New(t)
		repository := gitsourcetest.NewRepository(c)
		commit := repository.Commit(c, map[string]string{"db/schema.hcl": schemaV1}, "v1")
		server := gitsourcetest.ServeSSH(c, repository.Root)
		reference, err := gitsource.ParseReference(server.Reference("app.git"))
		c.Assert(err, qt.IsNil)
		key, err := os.ReadFile(server.KeyFile)
		c.Assert(err, qt.IsNil)
		transport, auth, err := gitsource.Dial(reference, gitsource.Credentials{SSHKey: key}, gitsource.Trust{KnownHostsFile: server.KnownHostsFile})
		c.Assert(err, qt.IsNil)
		result, err := gitsource.Fetch(context.Background(), gitsource.Options{
			Reference: reference, Transport: transport, Auth: auth,
			Ref: gitsource.Ref{Branch: "main"}, Path: "db/schema.hcl", Kind: gitsource.File,
		})
		c.Assert(err, qt.IsNil)
		c.Assert(result.Commit, qt.Equals, commit)
		c.Assert(result.ContentDigest, qt.Equals, schemaDigest(c, schemaV1))
	})
}

func TestGitFetch_FailurePath(t *testing.T) {
	t.Run("a commit force-pushed away", func(t *testing.T) {
		c := qt.New(t)
		h := newHistory(c)
		h.repository.Reset(c, h.first)
		result, err := gitFetch(c, h.server, gitsource.Options{Ref: gitsource.Ref{Branch: "main", Commit: h.second}, Path: "db/schema.hcl", Kind: gitsource.File})
		c.Assert(err, qt.ErrorIs, gitsource.ErrCommit)
		c.Assert(err, qt.ErrorMatches, `.*may have been force-pushed away`)
		c.Assert(result, qt.DeepEquals, gitsource.Result{})
	})
	// git-upload-pack over SSH holds one conversation, and gives only what it
	// advertised unless it is configured to give more.
	t.Run("a commit no ref names, over SSH from a server that gives commits only at a ref", func(t *testing.T) {
		c := qt.New(t)
		u := newUntagged(c)
		result, err := sshFetch(c, u.repository, gitsource.Options{Ref: gitsource.Ref{Commit: u.first}, Path: "db/schema.hcl", Kind: gitsource.File})
		c.Assert(err, qt.ErrorIs, gitsource.ErrUnsupportedID)
		c.Assert(err, qt.ErrorMatches, `.*no branch or tag names commit [0-9a-f]{40}, and the server gives commits only at a branch or tag`)
		c.Assert(result, qt.DeepEquals, gitsource.Result{})
	})
	t.Run("a commit alone force-pushed away, over HTTPS", func(t *testing.T) {
		c := qt.New(t)
		h := newHistory(c)
		h.repository.Reset(c, h.first)
		result, err := gitFetch(c, h.server, gitsource.Options{Ref: gitsource.Ref{Commit: h.second}, Path: "db/schema.hcl", Kind: gitsource.File})
		c.Assert(err, qt.ErrorIs, gitsource.ErrUnsupportedID)
		c.Assert(err, qt.ErrorMatches, `.*no branch or tag names commit [0-9a-f]{40}, and the server did not give it by its id: .*not our ref.*`)
		c.Assert(result, qt.DeepEquals, gitsource.Result{})
	})
	t.Run("content other than the expected digest", func(t *testing.T) {
		c := qt.New(t)
		h := newHistory(c)
		result, err := gitFetch(c, h.server, gitsource.Options{
			Ref: gitsource.Ref{Branch: "main"}, Path: "db/schema.hcl", Kind: gitsource.File, ExpectDigest: schemaDigest(c, schemaV1),
		})
		c.Assert(err, qt.ErrorIs, gitsource.ErrDigest)
		c.Assert(result, qt.DeepEquals, gitsource.Result{})
	})
	t.Run("a path the commit lacks", func(t *testing.T) {
		c := qt.New(t)
		h := newHistory(c)
		result, err := gitFetch(c, h.server, gitsource.Options{Ref: gitsource.Ref{Branch: "main"}, Path: "db/missing.hcl", Kind: gitsource.File})
		c.Assert(err, qt.ErrorIs, gitsource.ErrNotFound)
		c.Assert(result, qt.DeepEquals, gitsource.Result{})
	})
	t.Run("a submodule", func(t *testing.T) {
		c := qt.New(t)
		repository := gitsourcetest.NewRepository(c)
		other := repository.Commit(c, map[string]string{"README": "app\n"}, "readme")
		repository.Submodule(c, "db", "https://example.com/schemas.git", other)
		server := gitsourcetest.ServeHTTPS(c, repository.Root, gitsourcetest.HTTPSOptions{Username: gitUser, Password: gitPassword})
		result, err := gitFetch(c, server, gitsource.Options{Ref: gitsource.Ref{Branch: "main"}, Path: "db", Kind: gitsource.Directory})
		c.Assert(err, qt.ErrorIs, gitsource.ErrSubmodule)
		c.Assert(result, qt.DeepEquals, gitsource.Result{})
	})
	t.Run("an LFS pointer", func(t *testing.T) {
		c := qt.New(t)
		repository := gitsourcetest.NewRepository(c)
		pointer := "version https://git-lfs.github.com/spec/v1\noid sha256:4d7a214614ab2935c943f9e0ff69d22eadbb8f32b1258daaa5e2ca24d17e2393\nsize 12345\n"
		repository.Commit(c, map[string]string{"db/schema.hcl": pointer}, "lfs")
		server := gitsourcetest.ServeHTTPS(c, repository.Root, gitsourcetest.HTTPSOptions{Username: gitUser, Password: gitPassword})
		result, err := gitFetch(c, server, gitsource.Options{Ref: gitsource.Ref{Branch: "main"}, Path: "db/schema.hcl", Kind: gitsource.File})
		c.Assert(err, qt.ErrorIs, gitsource.ErrLFS)
		c.Assert(result, qt.DeepEquals, gitsource.Result{})
	})
	t.Run("a symbolic link in a migration directory", func(t *testing.T) {
		c := qt.New(t)
		repository := gitsourcetest.NewRepository(c)
		repository.Commit(c, map[string]string{"db/migrations/0001_create.up.sql": "CREATE TABLE t (id int);\n"}, "one")
		repository.Symlink(c, "db/migrations/0002_link.up.sql", "../../etc/passwd")
		server := gitsourcetest.ServeHTTPS(c, repository.Root, gitsourcetest.HTTPSOptions{Username: gitUser, Password: gitPassword})
		result, err := gitFetch(c, server, gitsource.Options{Ref: gitsource.Ref{Branch: "main"}, Path: "db/migrations", Kind: gitsource.Directory})
		c.Assert(err, qt.ErrorIs, gitsource.ErrSymlink)
		c.Assert(result, qt.DeepEquals, gitsource.Result{})
	})
	t.Run("a wrong password", func(t *testing.T) {
		c := qt.New(t)
		h := newHistory(c)
		reference, err := gitsource.ParseReference(h.server.Reference("app.git"))
		c.Assert(err, qt.IsNil)
		transport, auth, err := gitsource.Dial(reference, gitsource.Credentials{Username: gitUser, Password: "wrong"}, gitsource.Trust{CAFile: h.server.CAFile})
		c.Assert(err, qt.IsNil)
		result, err := gitsource.Fetch(context.Background(), gitsource.Options{
			Reference: reference, Transport: transport, Auth: auth, Ref: gitsource.Ref{Branch: "main"}, Path: "db/schema.hcl", Kind: gitsource.File,
		})
		c.Assert(err, qt.ErrorMatches, `.*authentication required.*|.*authorization failed.*`)
		c.Assert(result, qt.DeepEquals, gitsource.Result{})
	})
	t.Run("a redirect to another origin with a credential", func(t *testing.T) {
		c := qt.New(t)
		h := newHistory(c)
		elsewhere := gitsourcetest.ServeHTTPS(c, h.repository.Root, gitsourcetest.HTTPSOptions{RedirectTo: h.server.Origin})
		reference, err := gitsource.ParseReference(elsewhere.Reference("app.git"))
		c.Assert(err, qt.IsNil)
		transport, auth, err := gitsource.Dial(reference, gitsource.Credentials{Username: gitUser, Password: gitPassword}, gitsource.Trust{CAFile: elsewhere.CAFile})
		c.Assert(err, qt.IsNil)
		result, err := gitsource.Fetch(context.Background(), gitsource.Options{
			Reference: reference, Transport: transport, Auth: auth, Ref: gitsource.Ref{Branch: "main"}, Path: "db/schema.hcl", Kind: gitsource.File,
		})
		c.Assert(err, qt.ErrorIs, gitsource.ErrRedirect)
		c.Assert(result, qt.DeepEquals, gitsource.Result{})
	})
	t.Run("an SSH host key the known hosts do not hold", func(t *testing.T) {
		c := qt.New(t)
		repository := gitsourcetest.NewRepository(c)
		repository.Commit(c, map[string]string{"db/schema.hcl": schemaV1}, "v1")
		server := gitsourcetest.ServeSSH(c, repository.Root)
		reference, err := gitsource.ParseReference(server.Reference("app.git"))
		c.Assert(err, qt.IsNil)
		key, err := os.ReadFile(server.KeyFile)
		c.Assert(err, qt.IsNil)
		transport, auth, err := gitsource.Dial(reference, gitsource.Credentials{SSHKey: key}, gitsource.Trust{KnownHostsFile: server.OtherKnownHostsFile})
		c.Assert(err, qt.IsNil)
		result, err := gitsource.Fetch(context.Background(), gitsource.Options{
			Reference: reference, Transport: transport, Auth: auth, Ref: gitsource.Ref{Branch: "main"}, Path: "db/schema.hcl", Kind: gitsource.File,
		})
		c.Assert(err, qt.ErrorMatches, `.*key mismatch.*`)
		c.Assert(result, qt.DeepEquals, gitsource.Result{})
	})
}
