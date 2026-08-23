package agentpatch_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/agentgate"
	"go.5x5.cz/ptah/internal/agentpatch"
	"go.5x5.cz/ptah/internal/agentpolicy"
	"go.5x5.cz/ptah/internal/agentworkspace"
)

// stubVerifier answers with a scripted report per call: the first is the
// baseline the applier takes before writing, the second is the verification it
// takes afterwards. It is an input to the applier, not a way to pick an
// assertion.
type stubVerifier struct {
	reports []agentgate.Report
	calls   int
}

func (s *stubVerifier) Run(context.Context, *agentworkspace.Scope) (agentgate.Report, error) {
	index := min(s.calls, len(s.reports)-1)
	s.calls++
	return s.reports[index], nil
}

// passing is a verifier that finds nothing, before or after.
func passing() *stubVerifier {
	clean := agentgate.Report{OK: true, Results: []agentgate.Result{{
		Gate: "test-gate", OK: true, Diagnostics: make([]agentgate.Diagnostic, 0),
	}}}
	return &stubVerifier{reports: []agentgate.Report{clean, clean}}
}

// introducing is a verifier that is clean before the write and reports one
// error afterwards, which is the shape a patch that broke something produces.
func introducing() *stubVerifier {
	clean := agentgate.Report{OK: true, Results: []agentgate.Result{{
		Gate: "test-gate", OK: true, Diagnostics: make([]agentgate.Diagnostic, 0),
	}}}
	broken := agentgate.Report{OK: false, Results: []agentgate.Result{{
		Gate: "test-gate", OK: false, Diagnostics: []agentgate.Diagnostic{{
			Gate:     "test-gate",
			Severity: agentgate.SeverityError,
			Path:     "0002_add_status.up.sql",
			Message:  "syntax error at or near \"CRATE\"",
		}},
	}}}
	return &stubVerifier{reports: []agentgate.Report{clean, broken}}
}

// preexisting is a verifier that reports the same error before and after, which
// is a directory that was already broken when the patch arrived.
func preexisting() *stubVerifier {
	broken := agentgate.Report{OK: false, Results: []agentgate.Result{{
		Gate: "test-gate", OK: false, Diagnostics: []agentgate.Diagnostic{{
			Gate:     "test-gate",
			Severity: agentgate.SeverityError,
			Path:     "0001_init.up.sql",
			Message:  "syntax error at or near \"CRATE\"",
		}},
	}}}
	return &stubVerifier{reports: []agentgate.Report{broken, broken}}
}

// scopeWith builds a workspace whose schema class holds the given files, and
// returns the scope. The schema class is used for the mechanics because it has
// no integrity file, so a test about writing is about writing.
func scopeWith(c *qt.C, files map[string]string) *agentworkspace.Scope {
	root := c.TempDir()
	dir := filepath.Join(root, "schema")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	for name, content := range files {
		full := filepath.Join(dir, filepath.FromSlash(name))
		c.Assert(os.MkdirAll(filepath.Dir(full), 0o755), qt.IsNil)
		c.Assert(os.WriteFile(full, []byte(content), 0o600), qt.IsNil)
	}

	workspace, err := agentworkspace.Open(agentworkspace.Config{
		Root: root,
		Classes: map[agentpolicy.ArtifactClass]agentworkspace.ClassConfig{
			agentpolicy.ClassSchema: {Dir: "schema", Writable: true},
		},
		Dialect: "postgres",
	})
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = workspace.Close() })

	scope, err := workspace.Scope(agentpolicy.ClassSchema)
	c.Assert(err, qt.IsNil)
	return scope
}

// digestOf is the scope's current content address.
func digestOf(c *qt.C, scope *agentworkspace.Scope) string {
	digest, err := scope.Digest()
	c.Assert(err, qt.IsNil)
	return digest
}

// readBack returns one file's content from the scope.
func readBack(c *qt.C, scope *agentworkspace.Scope, name string) string {
	content, err := scope.ReadFile(name)
	c.Assert(err, qt.IsNil)
	return string(content)
}

func TestPlanPatch_HappyPath(t *testing.T) {
	c := qt.New(t)
	scope := scopeWith(c, map[string]string{"users.sql": "CREATE TABLE users (id BIGINT);\n"})

	plan, err := agentpatch.PlanPatch(scope, agentpatch.Patch{
		Class:          agentpolicy.ClassSchema,
		ExpectedDigest: digestOf(c, scope),
		Changes: []agentpatch.Change{
			{Path: "orders.sql", Operation: agentpatch.Create, Content: "CREATE TABLE orders (id BIGINT);\n"},
			{Path: "users.sql", Operation: agentpatch.Update, Content: "CREATE TABLE users (id BIGSERIAL);\n"},
		},
	})

	c.Assert(err, qt.IsNil)
	preview := plan.Preview()
	c.Assert(preview.PatchID, qt.Matches, "sha256:[0-9a-f]{64}")
	c.Assert(preview.BaseDigest, qt.Equals, digestOf(c, scope))
	c.Assert(preview.ResultDigest, qt.Not(qt.Equals), preview.BaseDigest)
	c.Assert(preview.Files, qt.HasLen, 2)
	c.Assert(preview.Files[0].Path, qt.Equals, "orders.sql")
	c.Assert(preview.Files[0].Operation, qt.Equals, agentpatch.Create)
	c.Assert(preview.Files[0].BeforeDigest, qt.Equals, "")
	c.Assert(preview.Files[1].Path, qt.Equals, "users.sql")
	c.Assert(preview.Files[1].Operation, qt.Equals, agentpatch.Update)
	c.Assert(preview.Files[1].BeforeDigest, qt.Matches, "sha256:[0-9a-f]{64}")
	c.Assert(preview.Capabilities, qt.DeepEquals, []string{"artifact.write:schema"})
}

func TestPlanPatch_PlanningWritesNothing(t *testing.T) {
	c := qt.New(t)
	scope := scopeWith(c, map[string]string{"users.sql": "CREATE TABLE users (id BIGINT);\n"})
	before := digestOf(c, scope)

	_, err := agentpatch.PlanPatch(scope, agentpatch.Patch{
		Class:   agentpolicy.ClassSchema,
		Changes: []agentpatch.Change{{Path: "orders.sql", Operation: agentpatch.Create, Content: "x\n"}},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(digestOf(c, scope), qt.Equals, before)
}

func TestPlanPatch_DeleteAsksForItsOwnCapability(t *testing.T) {
	// A deletion riding along inside a write approval is how a migration that
	// recorded a production change disappears.
	c := qt.New(t)
	scope := scopeWith(c, map[string]string{"users.sql": "CREATE TABLE users (id BIGINT);\n"})

	plan, err := agentpatch.PlanPatch(scope, agentpatch.Patch{
		Class:   agentpolicy.ClassSchema,
		Changes: []agentpatch.Change{{Path: "users.sql", Operation: agentpatch.Delete}},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(plan.Preview().Capabilities, qt.DeepEquals,
		[]string{"artifact.write:schema", "artifact.delete:schema"})
}

func TestPlanPatch_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		patch   agentpatch.Patch
		wantErr string
	}{
		{
			name:    "no changes",
			patch:   agentpatch.Patch{Class: agentpolicy.ClassSchema},
			wantErr: `invalid patch: it changes nothing`,
		},
		{
			name: "wrong artifact class",
			patch: agentpatch.Patch{
				Class:   agentpolicy.ClassMigrations,
				Changes: []agentpatch.Change{{Path: "a.sql", Operation: agentpatch.Create, Content: "x"}},
			},
			wantErr: `invalid patch: patch names artifact "migrations" and the scope is "schema"`,
		},
		{
			name: "create over an existing file",
			patch: agentpatch.Patch{
				Class:   agentpolicy.ClassSchema,
				Changes: []agentpatch.Change{{Path: "users.sql", Operation: agentpatch.Create, Content: "x"}},
			},
			wantErr: `invalid patch: "users.sql" already exists; use the update operation to replace it`,
		},
		{
			name: "update a file that is not there",
			patch: agentpatch.Patch{
				Class:   agentpolicy.ClassSchema,
				Changes: []agentpatch.Change{{Path: "orders.sql", Operation: agentpatch.Update, Content: "x"}},
			},
			wantErr: `invalid patch: "orders.sql" does not exist; use the create operation to add it`,
		},
		{
			name: "delete a file that is not there",
			patch: agentpatch.Patch{
				Class:   agentpolicy.ClassSchema,
				Changes: []agentpatch.Change{{Path: "orders.sql", Operation: agentpatch.Delete}},
			},
			wantErr: `invalid patch: "orders.sql" does not exist`,
		},
		{
			name: "an unknown operation",
			patch: agentpatch.Patch{
				Class:   agentpolicy.ClassSchema,
				Changes: []agentpatch.Change{{Path: "orders.sql", Operation: "rename", Content: "x"}},
			},
			wantErr: `invalid patch: unknown operation "rename" for "orders.sql"`,
		},
		{
			name: "the same path twice",
			patch: agentpatch.Patch{
				Class: agentpolicy.ClassSchema,
				Changes: []agentpatch.Change{
					{Path: "orders.sql", Operation: agentpatch.Create, Content: "a"},
					{Path: "orders.sql", Operation: agentpatch.Create, Content: "b"},
				},
			},
			wantErr: `invalid patch: "orders.sql" appears twice`,
		},
		{
			// On a case-insensitive filesystem these are one file, so the second
			// publication finds a destination that is not what it expected and
			// the patch half-applies.
			name: "two paths one filesystem cannot tell apart",
			patch: agentpatch.Patch{
				Class: agentpolicy.ClassSchema,
				Changes: []agentpatch.Change{
					{Path: "orders.sql", Operation: agentpatch.Create, Content: "a"},
					{Path: "Orders.SQL", Operation: agentpatch.Create, Content: "b"},
				},
			},
			wantErr: `invalid patch: "Orders.SQL" and "orders.sql": path collides with another path in the same scope`,
		},
		{
			name: "a path that collides with an existing file",
			patch: agentpatch.Patch{
				Class:   agentpolicy.ClassSchema,
				Changes: []agentpatch.Change{{Path: "Users.sql", Operation: agentpatch.Create, Content: "a"}},
			},
			wantErr: `invalid patch: "Users.sql" and the existing "users.sql": path collides with another path in the same scope`,
		},
		{
			name: "a path outside the scope",
			patch: agentpatch.Patch{
				Class: agentpolicy.ClassSchema,
				Changes: []agentpatch.Change{
					{Path: "../.github/workflows/release.yml", Operation: agentpatch.Create, Content: "a"},
				},
			},
			wantErr: `path "../.github/workflows/release.yml" leaves the artifact scope: unsafe artifact path`,
		},
		{
			name: "an absolute path",
			patch: agentpatch.Patch{
				Class: agentpolicy.ClassSchema,
				Changes: []agentpatch.Change{
					{Path: "/etc/cron.d/ptah", Operation: agentpatch.Create, Content: "a"},
				},
			},
			wantErr: `path "/etc/cron.d/ptah" is absolute: unsafe artifact path`,
		},
		{
			name: "content that is not text",
			patch: agentpatch.Patch{
				Class: agentpolicy.ClassSchema,
				Changes: []agentpatch.Change{
					{Path: "orders.sql", Operation: agentpatch.Create, Content: "CREATE\x00TABLE"},
				},
			},
			wantErr: `invalid patch: "orders.sql" contains a NUL byte`,
		},
		{
			name: "content that is not valid UTF-8",
			patch: agentpatch.Patch{
				Class: agentpolicy.ClassSchema,
				Changes: []agentpatch.Change{
					{Path: "orders.sql", Operation: agentpatch.Create, Content: "\xff\xfe"},
				},
			},
			wantErr: `invalid patch: "orders.sql" is not valid UTF-8`,
		},
		{
			name: "a delete carrying content",
			patch: agentpatch.Patch{
				Class: agentpolicy.ClassSchema,
				Changes: []agentpatch.Change{
					{Path: "users.sql", Operation: agentpatch.Delete, Content: "x"},
				},
			},
			wantErr: `invalid patch: a delete of "users.sql" carries content`,
		},
		{
			name: "a stale file digest",
			patch: agentpatch.Patch{
				Class: agentpolicy.ClassSchema,
				Changes: []agentpatch.Change{{
					Path:           "users.sql",
					Operation:      agentpatch.Update,
					Content:        "x",
					ExpectedDigest: "sha256:" + strings.Repeat("0", 64),
				}},
			},
			wantErr: `artifact digest does not match: "users.sql" expects sha256:0+ and holds sha256:[0-9a-f]{64}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scope := scopeWith(c, map[string]string{"users.sql": "CREATE TABLE users (id BIGINT);\n"})

			plan, err := agentpatch.PlanPatch(scope, test.patch)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(plan, qt.IsNil)
		})
	}
}

func TestPlanPatch_StaleArtifactDigest(t *testing.T) {
	c := qt.New(t)
	scope := scopeWith(c, map[string]string{"users.sql": "CREATE TABLE users (id BIGINT);\n"})

	plan, err := agentpatch.PlanPatch(scope, agentpatch.Patch{
		Class:          agentpolicy.ClassSchema,
		ExpectedDigest: "sha256:" + strings.Repeat("0", 64),
		Changes: []agentpatch.Change{
			{Path: "orders.sql", Operation: agentpatch.Create, Content: "x\n"},
		},
	})

	c.Assert(err, qt.ErrorIs, agentpatch.ErrDigestMismatch)
	c.Assert(plan, qt.IsNil)
}

func TestPlanPatch_RefusesTheIntegrityFile(t *testing.T) {
	// A caller that could write both a migration and the checksum over it would
	// produce a directory that verifies against itself.
	c := qt.New(t)
	root := c.TempDir()
	c.Assert(os.MkdirAll(filepath.Join(root, "migrations"), 0o755), qt.IsNil)
	workspace, err := agentworkspace.Open(agentworkspace.Config{
		Root: root,
		Classes: map[agentpolicy.ArtifactClass]agentworkspace.ClassConfig{
			agentpolicy.ClassMigrations: {Dir: "migrations", Writable: true},
		},
		Dialect: "postgres",
	})
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = workspace.Close() })
	scope, err := workspace.Scope(agentpolicy.ClassMigrations)
	c.Assert(err, qt.IsNil)

	plan, err := agentpatch.PlanPatch(scope, agentpatch.Patch{
		Class: agentpolicy.ClassMigrations,
		Changes: []agentpatch.Change{
			{Path: "ptah.sum", Operation: agentpatch.Create, Content: "h1:whatever\n"},
		},
	})

	c.Assert(err, qt.ErrorMatches,
		`invalid patch: "ptah.sum" is the migration integrity file; Ptah rewrites it after every patch`)
	c.Assert(plan, qt.IsNil)
}

func TestPatchID_IsTheEffectAndNotTheDescription(t *testing.T) {
	// An approval binds to the patch id, so rewording a description must not
	// mint a new identity and must not change an approved one.
	c := qt.New(t)
	scope := scopeWith(c, map[string]string{"users.sql": "CREATE TABLE users (id BIGINT);\n"})
	digest := digestOf(c, scope)
	change := agentpatch.Change{Path: "orders.sql", Operation: agentpatch.Create, Content: "SELECT 1;\n"}

	first, err := agentpatch.PlanPatch(scope, agentpatch.Patch{
		Class: agentpolicy.ClassSchema, ExpectedDigest: digest,
		Changes: []agentpatch.Change{change}, Summary: "add the orders table",
	})
	c.Assert(err, qt.IsNil)
	reworded, err := agentpatch.PlanPatch(scope, agentpatch.Patch{
		Class: agentpolicy.ClassSchema, ExpectedDigest: digest,
		Changes: []agentpatch.Change{change}, Summary: "completely different words",
	})
	c.Assert(err, qt.IsNil)
	different, err := agentpatch.PlanPatch(scope, agentpatch.Patch{
		Class: agentpolicy.ClassSchema, ExpectedDigest: digest,
		Changes: []agentpatch.Change{{
			Path: "orders.sql", Operation: agentpatch.Create, Content: "SELECT 2;\n",
		}},
	})
	c.Assert(err, qt.IsNil)

	c.Assert(reworded.ID(), qt.Equals, first.ID())
	c.Assert(different.ID(), qt.Not(qt.Equals), first.ID())
}

func TestPreview_RendersAUnifiedDiff(t *testing.T) {
	c := qt.New(t)
	scope := scopeWith(c, map[string]string{
		"users.sql": "CREATE TABLE users (\n  id BIGINT\n);\n",
	})

	plan, err := agentpatch.PlanPatch(scope, agentpatch.Patch{
		Class: agentpolicy.ClassSchema,
		Changes: []agentpatch.Change{{
			Path:      "users.sql",
			Operation: agentpatch.Update,
			Content:   "CREATE TABLE users (\n  id BIGSERIAL\n);\n",
		}},
	})
	c.Assert(err, qt.IsNil)

	diff := plan.Preview().Files[0].Diff
	c.Assert(diff, qt.Contains, "--- users.sql")
	c.Assert(diff, qt.Contains, "+++ users.sql")
	c.Assert(diff, qt.Contains, "-  id BIGINT")
	c.Assert(diff, qt.Contains, "+  id BIGSERIAL")
	c.Assert(diff, qt.Contains, " CREATE TABLE users (")
}

func TestPreview_ACreationDiffsAgainstNothing(t *testing.T) {
	c := qt.New(t)
	scope := scopeWith(c, nil)

	plan, err := agentpatch.PlanPatch(scope, agentpatch.Patch{
		Class: agentpolicy.ClassSchema,
		Changes: []agentpatch.Change{{
			Path: "orders.sql", Operation: agentpatch.Create, Content: "SELECT 1;\n",
		}},
	})
	c.Assert(err, qt.IsNil)

	diff := plan.Preview().Files[0].Diff
	c.Assert(diff, qt.Contains, "--- /dev/null")
	c.Assert(diff, qt.Contains, "+++ orders.sql")
	c.Assert(diff, qt.Contains, "+SELECT 1;")
}

func TestApply_HappyPath(t *testing.T) {
	c := qt.New(t)
	scope := scopeWith(c, map[string]string{"users.sql": "CREATE TABLE users (id BIGINT);\n"})
	base := digestOf(c, scope)

	plan, err := agentpatch.PlanPatch(scope, agentpatch.Patch{
		Class:          agentpolicy.ClassSchema,
		ExpectedDigest: base,
		Changes: []agentpatch.Change{
			{Path: "orders.sql", Operation: agentpatch.Create, Content: "CREATE TABLE orders (id BIGINT);\n"},
			{Path: "users.sql", Operation: agentpatch.Update, Content: "CREATE TABLE users (id BIGSERIAL);\n"},
		},
	})
	c.Assert(err, qt.IsNil)
	promised := plan.ResultDigest()

	result, err := agentpatch.Apply(context.Background(), plan, passing())

	c.Assert(err, qt.IsNil)
	c.Assert(result.RolledBack, qt.IsFalse)
	c.Assert(result.BaseDigest, qt.Equals, base)
	c.Assert(result.ResultDigest, qt.Equals, promised)
	c.Assert(result.ProjectedDigest, qt.Equals, "")
	c.Assert(result.IntegrityRefreshed, qt.IsFalse)
	c.Assert(result.Introduced, qt.HasLen, 0)
	c.Assert(readBack(c, scope, "orders.sql"), qt.Equals, "CREATE TABLE orders (id BIGINT);\n")
	c.Assert(readBack(c, scope, "users.sql"), qt.Equals, "CREATE TABLE users (id BIGSERIAL);\n")
	c.Assert(digestOf(c, scope), qt.Equals, promised)
}

func TestApply_LeavesNoStagedFilesBehind(t *testing.T) {
	// A staged file that outlived its publication would be a name in the
	// artifact directory, and the digest would report the directory as changed
	// by something nobody asked for.
	c := qt.New(t)
	scope := scopeWith(c, nil)

	plan, err := agentpatch.PlanPatch(scope, agentpatch.Patch{
		Class:          agentpolicy.ClassSchema,
		ExpectedDigest: digestOf(c, scope),
		Changes: []agentpatch.Change{
			{Path: "orders.sql", Operation: agentpatch.Create, Content: "SELECT 1;\n"},
		},
	})
	c.Assert(err, qt.IsNil)

	_, err = agentpatch.Apply(context.Background(), plan, passing())
	c.Assert(err, qt.IsNil)

	entries, err := scope.List()
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 1)
	c.Assert(entries[0].Path, qt.Equals, "orders.sql")
}

func TestApply_RollsBackWhenTheGateFindsAnIntroducedError(t *testing.T) {
	c := qt.New(t)
	scope := scopeWith(c, map[string]string{"users.sql": "CREATE TABLE users (id BIGINT);\n"})
	base := digestOf(c, scope)

	plan, err := agentpatch.PlanPatch(scope, agentpatch.Patch{
		Class:          agentpolicy.ClassSchema,
		ExpectedDigest: base,
		Changes: []agentpatch.Change{
			{Path: "orders.sql", Operation: agentpatch.Create, Content: "CRATE TABLE orders;\n"},
			{Path: "users.sql", Operation: agentpatch.Update, Content: "CREATE TABLE users (id BIGSERIAL);\n"},
		},
	})
	c.Assert(err, qt.IsNil)

	result, err := agentpatch.Apply(context.Background(), plan, introducing())

	c.Assert(err, qt.ErrorIs, agentpatch.ErrGateFailed)
	c.Assert(result.RolledBack, qt.IsTrue)
	c.Assert(result.RollbackFailure, qt.Equals, "")
	c.Assert(result.Introduced, qt.HasLen, 1)
	c.Assert(result.ResultDigest, qt.Equals, base)
	c.Assert(digestOf(c, scope), qt.Equals, base)
	c.Assert(readBack(c, scope, "users.sql"), qt.Equals, "CREATE TABLE users (id BIGINT);\n")

	_, statErr := scope.Stat("orders.sql")
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestApply_AProblemThatWasAlreadyThereDoesNotBlockThePatch(t *testing.T) {
	// The control for the rollback test. Without it, a rule of "fail on any
	// error-severity diagnostic" would pass the same assertions and would make
	// every patch to an already-broken directory unappliable.
	c := qt.New(t)
	scope := scopeWith(c, map[string]string{"users.sql": "CRATE TABLE users;\n"})

	plan, err := agentpatch.PlanPatch(scope, agentpatch.Patch{
		Class:          agentpolicy.ClassSchema,
		ExpectedDigest: digestOf(c, scope),
		Changes: []agentpatch.Change{
			{Path: "orders.sql", Operation: agentpatch.Create, Content: "CREATE TABLE orders (id BIGINT);\n"},
		},
	})
	c.Assert(err, qt.IsNil)

	result, err := agentpatch.Apply(context.Background(), plan, preexisting())

	c.Assert(err, qt.IsNil)
	c.Assert(result.RolledBack, qt.IsFalse)
	c.Assert(result.Introduced, qt.HasLen, 0)
	c.Assert(result.Verification.OK, qt.IsFalse)
	c.Assert(readBack(c, scope, "orders.sql"), qt.Equals, "CREATE TABLE orders (id BIGINT);\n")
}

func TestApply_RefusesAPatchWhoseArtifactChangedSincePreview(t *testing.T) {
	// Scenario 7 of #1487: the directory changed between preview and apply, so
	// the expected digest check fails and no partial write occurs.
	c := qt.New(t)
	scope := scopeWith(c, map[string]string{"users.sql": "CREATE TABLE users (id BIGINT);\n"})

	plan, err := agentpatch.PlanPatch(scope, agentpatch.Patch{
		Class:          agentpolicy.ClassSchema,
		ExpectedDigest: digestOf(c, scope),
		Changes: []agentpatch.Change{
			{Path: "orders.sql", Operation: agentpatch.Create, Content: "SELECT 1;\n"},
		},
	})
	c.Assert(err, qt.IsNil)

	// Somebody else edits the directory in the approval window.
	c.Assert(os.WriteFile(
		filepath.Join(scope.Path(), "invoices.sql"), []byte("SELECT 2;\n"), 0o600), qt.IsNil)
	afterEdit := digestOf(c, scope)

	result, err := agentpatch.Apply(context.Background(), plan, passing())

	c.Assert(err, qt.ErrorIs, agentpatch.ErrDigestMismatch)
	c.Assert(result, qt.IsNil)
	c.Assert(digestOf(c, scope), qt.Equals, afterEdit)
	_, statErr := scope.Stat("orders.sql")
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestApply_FailurePath(t *testing.T) {
	t.Run("no base digest", func(t *testing.T) {
		c := qt.New(t)
		scope := scopeWith(c, nil)
		plan, err := agentpatch.PlanPatch(scope, agentpatch.Patch{
			Class: agentpolicy.ClassSchema,
			Changes: []agentpatch.Change{
				{Path: "orders.sql", Operation: agentpatch.Create, Content: "SELECT 1;\n"},
			},
		})
		c.Assert(err, qt.IsNil)

		result, err := agentpatch.Apply(context.Background(), plan, passing())

		c.Assert(err, qt.ErrorMatches,
			`invalid patch: apply requires the artifact digest the patch was composed against`)
		c.Assert(result, qt.IsNil)
	})

	t.Run("no verifier", func(t *testing.T) {
		c := qt.New(t)
		scope := scopeWith(c, nil)
		plan, err := agentpatch.PlanPatch(scope, agentpatch.Patch{
			Class:          agentpolicy.ClassSchema,
			ExpectedDigest: digestOf(c, scope),
			Changes: []agentpatch.Change{
				{Path: "orders.sql", Operation: agentpatch.Create, Content: "SELECT 1;\n"},
			},
		})
		c.Assert(err, qt.IsNil)

		result, err := agentpatch.Apply(context.Background(), plan, nil)

		c.Assert(err, qt.ErrorMatches,
			`apply requires a verifier: an unverified write is not this operation`)
		c.Assert(result, qt.IsNil)
	})
}

func TestApply_DeleteRemovesAndRollbackRestores(t *testing.T) {
	c := qt.New(t)
	scope := scopeWith(c, map[string]string{"users.sql": "CREATE TABLE users (id BIGINT);\n"})
	base := digestOf(c, scope)

	plan, err := agentpatch.PlanPatch(scope, agentpatch.Patch{
		Class:          agentpolicy.ClassSchema,
		ExpectedDigest: base,
		Changes:        []agentpatch.Change{{Path: "users.sql", Operation: agentpatch.Delete}},
	})
	c.Assert(err, qt.IsNil)

	result, err := agentpatch.Apply(context.Background(), plan, introducing())

	c.Assert(err, qt.ErrorIs, agentpatch.ErrGateFailed)
	c.Assert(result.RolledBack, qt.IsTrue)
	c.Assert(readBack(c, scope, "users.sql"), qt.Equals, "CREATE TABLE users (id BIGINT);\n")
	c.Assert(digestOf(c, scope), qt.Equals, base)
}
