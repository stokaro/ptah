package agentdiag_test

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/agentapi"
	"go.5x5.cz/ptah/internal/agentdiag"
	"go.5x5.cz/ptah/internal/agentpatch"
	"go.5x5.cz/ptah/internal/agentpolicy"
	"go.5x5.cz/ptah/internal/agenttarget"
	"go.5x5.cz/ptah/internal/agentworkspace"
)

// TestOf_ReadsTheCodeThroughAWrappedChain pins the property the whole design
// rests on: a code assigned once at the sentinel survives every %w between the
// sentinel and the surface.
func TestOf_ReadsTheCodeThroughAWrappedChain(t *testing.T) {
	c := qt.New(t)

	wrapped := fmt.Errorf("apply: %w", fmt.Errorf("plan: %w", agentworkspace.ErrUnsafePath))

	c.Assert(agentdiag.Of(wrapped).Code, qt.Equals, agentdiag.CodeUnsafePath)
	c.Assert(wrapped, qt.ErrorIs, agentworkspace.ErrUnsafePath,
		qt.Commentf("a coded sentinel must still match with errors.Is"))
}

// TestOf_TakesTheOutermostCode pins which end of a chain wins.
//
// The outer one does, because it is the one nearest the caller: a patch that is
// invalid because the path inside it is unsafe is an invalid patch to the agent
// composing it, and telling it "unsafe path" would send it to fix a file rather
// than the change list it just sent.
func TestOf_TakesTheOutermostCode(t *testing.T) {
	c := qt.New(t)

	nested := fmt.Errorf("%w: %q is a directory: %w",
		agentpatch.ErrInvalidPatch, "migrations", agentworkspace.ErrNotRegularFile)

	c.Assert(agentdiag.Of(nested).Code, qt.Equals, agentdiag.CodeInvalidPatch)
	c.Assert(nested, qt.ErrorIs, agentworkspace.ErrNotRegularFile,
		qt.Commentf("the inner sentinel is still in the chain for a caller that wants it"))
}

// TestOf_ReportsAnUnclassifiedErrorRatherThanHidingIt pins the default.
//
// An error nobody classified answers internal, with an actor that names Ptah.
// The alternative -- reporting nothing for the errors nobody has got to yet --
// would hide exactly the ones worth finding.
func TestOf_ReportsAnUnclassifiedErrorRatherThanHidingIt(t *testing.T) {
	c := qt.New(t)

	diagnostic := agentdiag.Of(errors.New("something nobody classified"))

	c.Assert(diagnostic.Code, qt.Equals, agentdiag.CodeInternal)
	c.Assert(diagnostic.Actor, qt.Equals, agentdiag.ActorPtah)
	c.Assert(diagnostic.Message, qt.Equals, "something nobody classified")
}

// TestOf_AnswersNothingForNoError pins that a nil error is not a diagnostic.
func TestOf_AnswersNothingForNoError(t *testing.T) {
	c := qt.New(t)

	c.Assert(agentdiag.Of(nil), qt.DeepEquals, agentdiag.Diagnostic{})
}

// TestCodeOf_SeparatesUnclassifiedFromClassifiedInternal pins the distinction
// [agentdiag.Of] cannot make, and the MCP surface depends on.
func TestCodeOf_SeparatesUnclassifiedFromClassifiedInternal(t *testing.T) {
	c := qt.New(t)

	deliberate, coded := agentdiag.CodeOf(agentdiag.Wrap(agentdiag.CodeInternal, errors.New("x")))
	c.Assert(coded, qt.IsTrue)
	c.Assert(deliberate, qt.Equals, agentdiag.CodeInternal)

	accidental, coded := agentdiag.CodeOf(errors.New("x"))
	c.Assert(coded, qt.IsFalse)
	c.Assert(accidental, qt.Equals, agentdiag.CodeInternal)
}

// TestWithFallback_LeavesAClassifiedErrorAlone pins that the fallback is a
// floor rather than an override.
func TestWithFallback_LeavesAClassifiedErrorAlone(t *testing.T) {
	c := qt.New(t)

	kept := agentdiag.WithFallback(agentdiag.CodeInvalidRequest, agentapi.ErrNoWorkspace)
	c.Assert(agentdiag.Of(kept).Code, qt.Equals, agentdiag.CodeNoWorkspace)

	filled := agentdiag.WithFallback(agentdiag.CodeInvalidRequest, errors.New("bare"))
	c.Assert(agentdiag.Of(filled).Code, qt.Equals, agentdiag.CodeInvalidRequest)

	c.Assert(agentdiag.WithFallback(agentdiag.CodeInternal, nil), qt.IsNil)
}

// TestOf_ClassifiesAnErrorThatNamesItsOwnCode pins the interface half: a
// package with an error type of its own joins the taxonomy by adding a method,
// without being wrapped by anything.
func TestOf_ClassifiesAnErrorThatNamesItsOwnCode(t *testing.T) {
	c := qt.New(t)

	denied := &agentpolicy.DeniedError{
		Request:  agentpolicy.Request{Capability: agentpolicy.ArtifactWrite},
		Decision: agentpolicy.Decision{Layer: agentpolicy.LayerInvocation},
	}

	c.Assert(agentdiag.Of(fmt.Errorf("apply: %w", denied)).Code,
		qt.Equals, agentdiag.CodeCapabilityDenied)
}

// TestDiagnostic_StringLeadsWithTheCode pins what a model reads.
func TestDiagnostic_StringLeadsWithTheCode(t *testing.T) {
	c := qt.New(t)

	bare := agentdiag.Diagnostic{Code: agentdiag.CodeUnsafePath, Message: "path leaves the scope"}
	c.Assert(bare.String(), qt.Equals, "unsafe_path: path leaves the scope")

	hinted := bare
	hinted.Hint = "Name a path inside the artifact directory."
	c.Assert(hinted.String(), qt.Equals,
		"unsafe_path: path leaves the scope. Name a path inside the artifact directory.")
}

// TestCodes_AreEachInTheTaxonomy walks the published list and requires every
// member to be a member: an entry with no meaning would answer internal to a
// caller while appearing in the documentation as something else.
func TestCodes_AreEachInTheTaxonomy(t *testing.T) {
	for _, code := range agentdiag.Codes() {
		t.Run(string(code), func(t *testing.T) {
			c := qt.New(t)
			diagnostic := agentdiag.Of(agentdiag.Sentinel(code, "measured"))

			c.Assert(diagnostic.Code, qt.Equals, code)
			c.Assert(string(diagnostic.Actor), qt.Not(qt.Equals), "")
			c.Assert(agentdiag.Summary(code), qt.Not(qt.Equals), "")
		})
	}
}

// TestCodes_AreDistinct pins that the list is a set. Two entries with one
// spelling would make the published table describe one of them twice.
func TestCodes_AreDistinct(t *testing.T) {
	c := qt.New(t)

	codes := agentdiag.Codes()
	unique := make(map[agentdiag.Code]struct{}, len(codes))
	for _, code := range codes {
		unique[code] = struct{}{}
	}

	c.Assert(unique, qt.HasLen, len(codes))
}

// TestRetryable_IsOnlyForWaiting pins the field's meaning against the way an
// agent uses it.
//
// A model told "retryable" calls again. So the flag is set only where calling
// again could genuinely answer differently, and never on a refusal -- a policy
// that denied a capability will deny it identically for as long as the session
// lives, and an agent looping on that is the failure this taxonomy exists to
// prevent.
func TestRetryable_IsOnlyForWaiting(t *testing.T) {
	c := qt.New(t)

	retryable := make([]agentdiag.Code, 0, len(agentdiag.Codes()))
	for _, code := range agentdiag.Codes() {
		retryable = append(retryable, retryableOnly(code)...)
	}

	c.Assert(retryable, qt.DeepEquals, []agentdiag.Code{
		agentdiag.CodeDatabaseUnreachable,
		agentdiag.CodeDatabaseReadFailed,
		agentdiag.CodeWriteFailed,
	})
}

// retryableOnly returns the code when it is retryable, so the test body above
// stays a loop with no branch in it.
func retryableOnly(code agentdiag.Code) []agentdiag.Code {
	if !agentdiag.Of(agentdiag.Sentinel(code, "measured")).Retryable {
		return nil
	}
	return []agentdiag.Code{code}
}

// TestDocumentation_PublishesEveryCode pins the table in docs/agent-errors.md
// against the taxonomy, both ways.
//
// A code the document omits is a token a client cannot look up; a row for a
// code that no longer exists is worse, because it reads as a promise.
func TestDocumentation_PublishesEveryCode(t *testing.T) {
	c := qt.New(t)

	documented := documentedCodes(c)
	published := agentdiag.Codes()
	slices.Sort(published)
	slices.Sort(documented)

	c.Assert(documented, qt.DeepEquals, published)
}

// TestDocumentation_PublishesTheMeasuredRow pins the actor and the retryable
// column too, so a code cannot be documented as something it is not.
func TestDocumentation_PublishesTheMeasuredRow(t *testing.T) {
	rows := documentedRows(qt.New(t))

	for _, code := range agentdiag.Codes() {
		t.Run(string(code), func(t *testing.T) {
			c := qt.New(t)
			diagnostic := agentdiag.Of(agentdiag.Sentinel(code, "measured"))

			c.Assert(rows[code], qt.DeepEquals, []string{
				string(diagnostic.Actor),
				retryableWord(diagnostic),
				agentdiag.Summary(code),
			})
		})
	}
}

// retryableWord is how the document spells the diagnostic's retryable field.
func retryableWord(diagnostic agentdiag.Diagnostic) string {
	if diagnostic.Retryable {
		return "yes"
	}
	return "no"
}

// documentedRows parses the code table out of docs/agent-errors.md.
func documentedRows(c *qt.C) map[agentdiag.Code][]string {
	c.Helper()
	body, err := os.ReadFile(filepath.Join("..", "..", "docs", "agent-errors.md"))
	c.Assert(err, qt.IsNil)

	rows := make(map[agentdiag.Code][]string)
	for line := range strings.SplitSeq(string(body), "\n") {
		code, cells := tableRow(line)
		rows[code] = cells
	}
	delete(rows, "")
	return rows
}

// documentedCodes is the same table, as a list of its codes.
func documentedCodes(c *qt.C) []agentdiag.Code {
	c.Helper()
	codes := make([]agentdiag.Code, 0, len(agentdiag.Codes()))
	for code := range documentedRows(c) {
		codes = append(codes, code)
	}
	return codes
}

// tableRow reads one markdown row of the four-column code table, and answers an
// empty code for every line that is not one.
func tableRow(line string) (agentdiag.Code, []string) {
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, "| `") {
		return "", nil
	}
	cells := strings.Split(strings.Trim(trimmed, "|"), "|")
	if len(cells) != 4 {
		return "", nil
	}
	for i, cell := range cells {
		cells[i] = strings.TrimSpace(cell)
	}
	name := strings.Trim(cells[0], "`")
	if !slices.Contains(agentdiag.Codes(), agentdiag.Code(name)) {
		return "", nil
	}
	return agentdiag.Code(name), cells[1:]
}

// TestEverySentinelIsClassified is the guard the taxonomy needs to stay true.
//
// Every sentinel the agent packages declare is checked here, and a new one
// declared with plain errors.New answers internal -- which is the failure this
// test names, before a client ever sees it.
func TestEverySentinelIsClassified(t *testing.T) {
	for name, err := range classifiedSentinels() {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			code, coded := agentdiag.CodeOf(err)

			c.Assert(coded, qt.IsTrue,
				qt.Commentf("%s carries no code: declare it with agentdiag.Sentinel", name))
			c.Assert(code, qt.Not(qt.Equals), agentdiag.CodeInternal,
				qt.Commentf("%s answers the unclassified code", name))
		})
	}
}

// TestSentinelListIsComplete guards the list above by reading the source.
//
// A hand-written roster of sentinels is exactly the shape that goes stale: a
// package gains one, nobody adds it here, and the guard above passes while the
// new error reaches clients as internal. So the roster is measured against the
// declarations rather than trusted.
func TestSentinelListIsComplete(t *testing.T) {
	c := qt.New(t)

	declared := declaredSentinels(c)
	covered := make([]string, 0, len(declared))
	for name := range classifiedSentinels() {
		covered = append(covered, name)
	}
	for name := range sentinelsNotOnTheSurface {
		covered = append(covered, name)
	}
	slices.Sort(declared)
	slices.Sort(covered)

	c.Assert(covered, qt.DeepEquals, declared)
}

// classifiedSentinels is every sentinel that can reach an agent client.
func classifiedSentinels() map[string]error {
	return map[string]error{
		"agentapi.ErrNoSourceScope":            agentapi.ErrNoSourceScope,
		"agentapi.ErrNoWorkspace":              agentapi.ErrNoWorkspace,
		"agentapi.ErrSourceNotLocal":           agentapi.ErrSourceNotLocal,
		"agentapi.ErrSourceOutsideScope":       agentapi.ErrSourceOutsideScope,
		"agenttarget.ErrNoneConfigured":        agenttarget.ErrNoneConfigured,
		"agenttarget.ErrUnknown":               agenttarget.ErrUnknown,
		"agentapi.ErrUnknownPreview":           agentapi.ErrUnknownPreview,
		"agentpatch.ErrDigestMismatch":         agentpatch.ErrDigestMismatch,
		"agentpatch.ErrGateFailed":             agentpatch.ErrGateFailed,
		"agentpatch.ErrInvalidPatch":           agentpatch.ErrInvalidPatch,
		"agentpatch.ErrNameCollision":          agentpatch.ErrNameCollision,
		"agentpolicy.ErrApprovalRefused":       agentpolicy.ErrApprovalRefused,
		"agentpolicy.ErrApprovalUnavailable":   agentpolicy.ErrApprovalUnavailable,
		"agentpolicy.ErrHardDenied":            agentpolicy.ErrHardDenied,
		"agentworkspace.ErrClassNotConfigured": agentworkspace.ErrClassNotConfigured,
		"agentworkspace.ErrNotRegularFile":     agentworkspace.ErrNotRegularFile,
		"agentworkspace.ErrTooLarge":           agentworkspace.ErrTooLarge,
		"agentworkspace.ErrUnsafePath":         agentworkspace.ErrUnsafePath,
	}
}

// sentinelsNotOnTheSurface names the declarations a client cannot reach, with
// the reason each one cannot.
//
// It is empty, and that is a claim rather than an omission: every sentinel the
// agent packages declare today is reachable from a tool call and carries a
// code. An entry belongs here only for an error raised before the first call --
// a configuration fault the operator sees on the command line.
var sentinelsNotOnTheSurface = make(map[string]string)

// agentPackages are the packages whose sentinels this taxonomy is responsible
// for.
//
// They are listed from the filesystem rather than written down, because a
// roster is the same hand-maintained list this test exists to guard: a new
// internal/agent… package would otherwise be outside the check that says every
// sentinel is classified, and nothing would say so.
func agentPackages(c *qt.C) []string {
	c.Helper()
	entries, err := os.ReadDir("..")
	c.Assert(err, qt.IsNil)

	packages := make([]string, 0, 8)
	for _, entry := range entries {
		packages = append(packages, agentPackageName(entry)...)
	}
	c.Assert(len(packages) > 1, qt.IsTrue, qt.Commentf("found no agent packages to walk"))
	return packages
}

// agentPackageName returns the entry's name when it is an agent package other
// than this one.
func agentPackageName(entry os.DirEntry) []string {
	if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "agent") || entry.Name() == "agentdiag" {
		return nil
	}
	return []string{entry.Name()}
}

// declaredSentinels reads every package-level Err… declaration out of the agent
// packages' source.
func declaredSentinels(c *qt.C) []string {
	c.Helper()
	found := make([]string, 0, len(classifiedSentinels()))
	for _, pkg := range agentPackages(c) {
		found = append(found, sentinelsIn(c, pkg)...)
	}
	return found
}

// sentinelsIn parses one package and returns its exported error variables.
func sentinelsIn(c *qt.C, pkg string) []string {
	c.Helper()
	fset := token.NewFileSet()
	dir := filepath.Join("..", pkg)
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)

	names := make([]string, 0, 4)
	for _, entry := range entries {
		names = append(names, errorVarsInFile(c, fset, dir, entry)...)
	}
	return names
}

// errorVarsInFile parses one source file, skipping anything that is not the
// package's own non-test Go.
func errorVarsInFile(c *qt.C, fset *token.FileSet, dir string, entry os.DirEntry) []string {
	c.Helper()
	name := entry.Name()
	if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
		return nil
	}
	file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.SkipObjectResolution)
	c.Assert(err, qt.IsNil)
	return errorVarNames(filepath.Base(dir), file)
}

// errorVarNames returns the exported Err… variables one file declares.
func errorVarNames(pkg string, file *ast.File) []string {
	names := make([]string, 0, 4)
	for _, decl := range file.Decls {
		general, isVar := decl.(*ast.GenDecl)
		if !isVar || general.Tok != token.VAR {
			continue
		}
		for _, spec := range general.Specs {
			value, isValue := spec.(*ast.ValueSpec)
			if !isValue {
				continue
			}
			for _, name := range value.Names {
				if !strings.HasPrefix(name.Name, "Err") {
					continue
				}
				names = append(names, pkg+"."+name.Name)
			}
		}
	}
	return names
}
