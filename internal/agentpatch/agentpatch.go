// Package agentpatch is the only way an agent-driven surface changes a file in
// a Ptah workspace.
//
// # The shape, and why it is this shape
//
// A patch names an artifact class, the digest of that class's directory it was
// computed against, and a list of whole-file changes. It does not name a root,
// a directory, or an absolute path: the class resolves to a directory the
// operator configured, and every path is relative to it.
//
// Whole files rather than hunks. A hunk-based patch has to be applied to
// something, and "something" is a file that may have changed since the model
// read it -- so the applier would be reconstructing content the reviewer never
// saw. A whole-file change is exactly what the reviewer approves and exactly
// what lands.
//
// # Two digests, doing different jobs
//
// [Patch.ExpectedDigest] is the directory's content address when the patch was
// composed. A change to any file in the class between composing and applying
// changes it, and the apply refuses. That is #1487's scenario 7, and it is why
// a preview is not merely a courtesy: the digest the preview reports is the one
// the apply has to still find.
//
// [Change.ExpectedDigest] is one file's content address, and it answers the
// narrower question of whether this file is the one the model read. Both are
// checked, and the file-level check is also enforced by the operating system:
// publication is conditional on the destination through
// [fsdurable.Destination], so a file that changed in the window between the
// check and the rename is reported rather than overwritten.
//
// # What a failed gate does
//
// The applier writes, then verifies, then keeps or undoes. A patch whose
// verification introduces an error-severity diagnostic is rolled back to the
// bytes that were there before, and the diagnostics are returned. The model
// therefore cannot leave a repository in a state Ptah would refuse to load by
// writing and then declining to check.
//
// The undo is in-process. A patch interrupted by a process kill between
// publications leaves the files published so far, and the next operation
// against that class refuses because the directory digest no longer matches --
// so the damage is visible rather than silent. Crash-recoverable journaling for
// a multi-file batch exists in internal/atlasmigrate and generalizing it is
// separate work.
package agentpatch

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"sort"
	"strings"
	"unicode/utf8"

	"go.5x5.cz/ptah/internal/agentpolicy"
	"go.5x5.cz/ptah/internal/agentworkspace"
)

// Limits on one patch. They bound what a single tool call can do to a
// repository, which matters more here than in a command a person typed: the
// caller is a model, and "how many files should this touch" is not a question
// it always gets right.
const (
	// MaxChanges is the most files one patch may touch.
	MaxChanges = 64
	// MaxPatchBytes is the most content one patch may carry in total.
	MaxPatchBytes = 4 << 20
)

var (
	// ErrDigestMismatch reports that the artifact changed since the patch was
	// composed. It is the conflict-detection result, and it is recoverable: the
	// caller re-reads and composes a new patch.
	ErrDigestMismatch = errors.New("artifact digest does not match")
	// ErrInvalidPatch reports a patch that is malformed rather than merely
	// unappliable.
	ErrInvalidPatch = errors.New("invalid patch")
	// ErrGateFailed reports that verification found an error the patch
	// introduced, and that the patch was rolled back.
	ErrGateFailed = errors.New("verification gate failed")
	// ErrNameCollision reports two paths one filesystem cannot tell apart.
	ErrNameCollision = errors.New("path collides with another path in the same scope")
)

// Operation is what a change does to one path.
type Operation string

const (
	// Create writes a file that does not exist.
	Create Operation = "create"
	// Update replaces a file that does.
	Update Operation = "update"
	// Delete removes one. It carries its own capability, denied by default.
	Delete Operation = "delete"
)

// Operations lists the operations in a stable order, for the surfaces that
// enumerate them.
func Operations() []Operation {
	return []Operation{Create, Update, Delete}
}

// Change is one file's worth of a patch.
type Change struct {
	// Path is relative to the artifact class's directory, slash-separated.
	Path string `json:"path" jsonschema:"path relative to the artifact directory, using forward slashes"`
	// Operation is create, update or delete.
	Operation Operation `json:"operation" jsonschema:"create, update or delete"`
	// Content is the complete new content, for create and update.
	Content string `json:"content,omitempty" jsonschema:"the complete file content for create and update"`
	// ExpectedDigest is the sha256 of the file this change is based on, for
	// update and delete. Empty skips the file-level check; the directory digest
	// still applies.
	ExpectedDigest string `json:"expected_digest,omitempty" jsonschema:"sha256 digest of the file this change replaces"`
}

// Patch is a proposed change to one artifact class.
type Patch struct {
	// Class is the artifact family: migrations, schema or tests.
	Class agentpolicy.ArtifactClass `json:"artifact" jsonschema:"artifact class: migrations, schema or tests"`
	// ExpectedDigest is the class directory's digest the patch was composed
	// against, as reported by a read or a preview.
	ExpectedDigest string `json:"expected_digest" jsonschema:"the artifact digest this patch was composed against"`
	// Changes are the files to write, in any order.
	Changes []Change `json:"changes" jsonschema:"the file changes this patch applies"`
	// Summary is the caller's description of the intent.
	//
	// It is recorded and displayed as the CALLER's words, never as Ptah's. The
	// sentence a person reads before approving a patch is composed by Ptah from
	// the paths and digests; letting the party that wrote the patch also write
	// the sentence describing it is the prompt-injection problem in one field.
	Summary string `json:"summary,omitempty" jsonschema:"a short description of what this patch is for"`
}

// FileChange is one planned change, with everything a reviewer needs.
type FileChange struct {
	Path         string    `json:"path"`
	Operation    Operation `json:"operation"`
	BeforeDigest string    `json:"before_digest,omitempty"`
	AfterDigest  string    `json:"after_digest,omitempty"`
	Bytes        int       `json:"bytes"`
	// Diff is the unified diff between the two states.
	Diff string `json:"diff"`

	// before is the content to restore if the patch is undone, and beforeInfo
	// is the destination identity publication is made conditional on.
	before     []byte
	beforeInfo fs.FileInfo
	beforeMode fs.FileMode
	content    []byte
}

// Plan is a validated patch: everything checked, nothing written.
//
// It holds the scope it was planned against, because applying it to a different
// one would be applying it to a directory nobody checked.
type Plan struct {
	scope        *agentworkspace.Scope
	patch        Patch
	id           string
	baseDigest   string
	resultDigest string
	files        []FileChange
}

// Preview is the reviewable form of a plan.
type Preview struct {
	// PatchID is the content address of the patch itself: an approval refers to
	// this, so approving one patch cannot authorize another.
	PatchID string                    `json:"patch_id"`
	Class   agentpolicy.ArtifactClass `json:"artifact"`
	// BaseDigest is what the class held when the plan was made, and what apply
	// must still find.
	BaseDigest string `json:"base_digest"`
	// ResultDigest is what it will hold afterwards.
	ResultDigest string       `json:"result_digest"`
	Files        []FileChange `json:"files"`
	// Capabilities lists what applying this patch requires, so a surface can
	// show the person exactly which permissions their approval grants.
	Capabilities []string `json:"capabilities"`
}

// ID is the patch's content address.
func (p *Plan) ID() string { return p.id }

// BaseDigest is the artifact digest the plan was made against.
func (p *Plan) BaseDigest() string { return p.baseDigest }

// ResultDigest is the artifact digest applying the plan produces.
func (p *Plan) ResultDigest() string { return p.resultDigest }

// Class names the artifact family the plan touches.
func (p *Plan) Class() agentpolicy.ArtifactClass { return p.patch.Class }

// Files lists the planned changes.
func (p *Plan) Files() []FileChange { return slices.Clone(p.files) }

// Preview renders the plan for review.
func (p *Plan) Preview() Preview {
	requests := p.Requests()
	capabilities := make([]string, 0, len(requests))
	for _, request := range requests {
		capabilities = append(capabilities, request.String())
	}
	return Preview{
		PatchID:      p.id,
		Class:        p.patch.Class,
		BaseDigest:   p.baseDigest,
		ResultDigest: p.resultDigest,
		Files:        p.Files(),
		Capabilities: capabilities,
	}
}

// Requests are the capability requests applying this plan needs, one per
// distinct capability.
//
// A patch that only creates and updates needs artifact.write; one that also
// deletes needs artifact.delete as well, and asking for both separately is what
// keeps a deletion from riding along inside a write approval.
func (p *Plan) Requests() []agentpolicy.Request {
	paths := make([]string, 0, len(p.files))
	deletes := make([]string, 0)
	for _, file := range p.files {
		paths = append(paths, file.Path)
		if file.Operation == Delete {
			deletes = append(deletes, file.Path)
		}
	}
	requests := []agentpolicy.Request{{
		Capability: agentpolicy.ArtifactWrite,
		Artifact:   p.patch.Class,
		Paths:      paths,
		Reason:     fmt.Sprintf("write %d file(s) in the %s artifact", len(p.files), p.patch.Class),
	}}
	if len(deletes) > 0 {
		requests = append(requests, agentpolicy.Request{
			Capability: agentpolicy.ArtifactDelete,
			Artifact:   p.patch.Class,
			Paths:      deletes,
			Reason:     fmt.Sprintf("delete %d file(s) from the %s artifact", len(deletes), p.patch.Class),
		})
	}
	return requests
}

// Subject describes the plan for an approval prompt.
//
// Every field is Ptah's own text derived from the plan's facts. The patch's own
// summary is not here: it is the untrusted party's prose, and an approval
// prompt is the one place it must not appear as though Ptah wrote it.
func (p *Plan) Subject() agentpolicy.Subject {
	details := []agentpolicy.Detail{
		{Label: "artifact", Value: string(p.patch.Class)},
		{Label: "directory", Value: p.scope.Path()},
		{Label: "base digest", Value: p.baseDigest},
		{Label: "result digest", Value: p.resultDigest},
	}
	for _, file := range p.files {
		details = append(details, agentpolicy.Detail{
			Label: string(file.Operation),
			Value: file.Path,
		})
	}
	return agentpolicy.Subject{
		Summary: fmt.Sprintf("apply a %d-file patch to the %s artifact", len(p.files), p.patch.Class),
		Digest:  p.id,
		Details: details,
	}
}

// PlanPatch validates a patch against a scope and computes everything a review
// needs, writing nothing.
//
// An empty [Patch.ExpectedDigest] is accepted here and refused by [Apply]: a
// caller composing its first patch has not read a digest yet, and the plan is
// what tells it one. A caller that supplies a digest gets it checked at this
// point too, which is how a stale patch is refused before a person is asked to
// approve it.
func PlanPatch(scope *agentworkspace.Scope, patch Patch) (*Plan, error) {
	if scope.Class() != patch.Class {
		return nil, fmt.Errorf(
			"%w: patch names artifact %q and the scope is %q",
			ErrInvalidPatch, patch.Class, scope.Class())
	}
	if err := validateChangeList(patch.Changes); err != nil {
		return nil, err
	}
	entries, err := scope.List()
	if err != nil {
		return nil, err
	}
	baseDigest := agentworkspace.DigestOf(entries)
	if patch.ExpectedDigest != "" && patch.ExpectedDigest != baseDigest {
		return nil, fmt.Errorf(
			"%w: patch expects %s and the %s artifact is %s",
			ErrDigestMismatch, patch.ExpectedDigest, patch.Class, baseDigest)
	}

	index := indexEntries(entries)
	files, err := planFiles(scope, patch, index)
	if err != nil {
		return nil, err
	}
	return &Plan{
		scope:        scope,
		patch:        patch,
		id:           patchID(patch.Class, baseDigest, files),
		baseDigest:   baseDigest,
		resultDigest: resultDigest(entries, files),
		files:        files,
	}, nil
}

// validateChangeList checks the properties of the list as a whole.
func validateChangeList(changes []Change) error {
	if len(changes) == 0 {
		return fmt.Errorf("%w: it changes nothing", ErrInvalidPatch)
	}
	if len(changes) > MaxChanges {
		return fmt.Errorf("%w: %d changes exceeds the limit of %d",
			ErrInvalidPatch, len(changes), MaxChanges)
	}
	total := 0
	for _, change := range changes {
		total += len(change.Content)
	}
	if total > MaxPatchBytes {
		return fmt.Errorf("%w: %d bytes of content exceeds the limit of %d",
			ErrInvalidPatch, total, MaxPatchBytes)
	}
	return nil
}

// entryIndex is the scope's current state, addressed both exactly and by the
// folded name a case-insensitive filesystem would collapse.
type entryIndex struct {
	byPath map[string]agentworkspace.Entry
	byFold map[string]string
}

// lookup returns the entry at a path, or nil when the scope does not hold one.
func (i entryIndex) lookup(clean string) *agentworkspace.Entry {
	entry, found := i.byPath[clean]
	if !found {
		return nil
	}
	return &entry
}

func indexEntries(entries []agentworkspace.Entry) entryIndex {
	index := entryIndex{
		byPath: make(map[string]agentworkspace.Entry, len(entries)),
		byFold: make(map[string]string, len(entries)),
	}
	for _, entry := range entries {
		index.byPath[entry.Path] = entry
		index.byFold[agentworkspace.FoldKey(entry.Path)] = entry.Path
	}
	return index
}

// planFiles validates and describes every change.
func planFiles(scope *agentworkspace.Scope, patch Patch, index entryIndex) ([]FileChange, error) {
	seen := make(map[string]struct{}, len(patch.Changes))
	folded := make(map[string]string, len(patch.Changes))
	files := make([]FileChange, 0, len(patch.Changes))
	for _, change := range patch.Changes {
		clean, err := scope.ResolvePath(change.Path)
		if err != nil {
			return nil, err
		}
		if _, duplicate := seen[clean]; duplicate {
			return nil, fmt.Errorf("%w: %q appears twice", ErrInvalidPatch, clean)
		}
		seen[clean] = struct{}{}
		if err := recordFold(folded, index, clean); err != nil {
			return nil, err
		}
		file, err := planFile(scope, change, clean, index)
		if err != nil {
			return nil, err
		}
		files = append(files, file)
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, nil
}

// recordFold refuses two paths one filesystem would treat as one.
//
// Both directions matter: two changes that differ only in case, and a change
// whose folded name matches an existing file it does not exactly name. The
// second is the one that half-applies -- the create finds no file at
// `Users.sql`, and the publication finds `users.sql` already there.
func recordFold(folded map[string]string, index entryIndex, clean string) error {
	key := agentworkspace.FoldKey(clean)
	if other, collides := folded[key]; collides {
		return fmt.Errorf("%w: %q and %q: %w", ErrInvalidPatch, clean, other, ErrNameCollision)
	}
	folded[key] = clean
	if existing, collides := index.byFold[key]; collides && existing != clean {
		return fmt.Errorf("%w: %q and the existing %q: %w",
			ErrInvalidPatch, clean, existing, ErrNameCollision)
	}
	return nil
}

// planFile validates one change against the current state of its path.
func planFile(
	scope *agentworkspace.Scope,
	change Change,
	clean string,
	index entryIndex,
) (FileChange, error) {
	if err := refuseManagedFile(scope.Class(), clean); err != nil {
		return FileChange{}, err
	}
	existing := index.lookup(clean)
	switch change.Operation {
	case Create:
		return planCreate(change, clean, existing)
	case Update:
		return planUpdate(scope, change, clean, existing)
	case Delete:
		return planDelete(scope, change, clean, existing)
	}
	return FileChange{}, fmt.Errorf("%w: unknown operation %q for %q",
		ErrInvalidPatch, change.Operation, clean)
}

// managedFiles are the names Ptah maintains and a patch may not write.
//
// The integrity file is the whole of the list today, and the reason it is a
// refusal rather than a discouragement is that writing it is how tampering is
// concealed: a caller that could supply both the migration and the checksum
// over it would produce a directory that verifies against itself. Ptah
// recomputes the file after every patch, so there is nothing a caller loses.
var managedFiles = map[agentpolicy.ArtifactClass][]string{
	agentpolicy.ClassMigrations: {"ptah.sum", "atlas.sum"},
}

// refuseManagedFile rejects a change to a file Ptah maintains.
func refuseManagedFile(class agentpolicy.ArtifactClass, clean string) error {
	for _, managed := range managedFiles[class] {
		if agentworkspace.FoldKey(clean) != agentworkspace.FoldKey(managed) {
			continue
		}
		return fmt.Errorf(
			"%w: %q is the migration integrity file; Ptah rewrites it after every patch",
			ErrInvalidPatch, clean)
	}
	return nil
}

func planCreate(change Change, clean string, existing *agentworkspace.Entry) (FileChange, error) {
	if existing != nil {
		return FileChange{}, fmt.Errorf(
			"%w: %q already exists; use the update operation to replace it",
			ErrInvalidPatch, clean)
	}
	content, err := validateContent(clean, change.Content)
	if err != nil {
		return FileChange{}, err
	}
	return FileChange{
		Path:        clean,
		Operation:   Create,
		AfterDigest: digestBytes(content),
		Bytes:       len(content),
		Diff:        unifiedDiff(clean, nil, content),
		content:     content,
	}, nil
}

func planUpdate(
	scope *agentworkspace.Scope,
	change Change,
	clean string,
	existing *agentworkspace.Entry,
) (FileChange, error) {
	if existing == nil {
		return FileChange{}, fmt.Errorf(
			"%w: %q does not exist; use the create operation to add it",
			ErrInvalidPatch, clean)
	}
	if existing.Kind != "" {
		return FileChange{}, fmt.Errorf("%w: %q is a %s: %w",
			ErrInvalidPatch, clean, existing.Kind, agentworkspace.ErrNotRegularFile)
	}
	if change.ExpectedDigest != "" && change.ExpectedDigest != existing.Digest {
		return FileChange{}, fmt.Errorf("%w: %q expects %s and holds %s",
			ErrDigestMismatch, clean, change.ExpectedDigest, existing.Digest)
	}
	content, err := validateContent(clean, change.Content)
	if err != nil {
		return FileChange{}, err
	}
	before, info, err := readBefore(scope, clean)
	if err != nil {
		return FileChange{}, err
	}
	return FileChange{
		Path:         clean,
		Operation:    Update,
		BeforeDigest: existing.Digest,
		AfterDigest:  digestBytes(content),
		Bytes:        len(content),
		Diff:         unifiedDiff(clean, before, content),
		before:       before,
		beforeInfo:   info,
		beforeMode:   info.Mode(),
		content:      content,
	}, nil
}

func planDelete(
	scope *agentworkspace.Scope,
	change Change,
	clean string,
	existing *agentworkspace.Entry,
) (FileChange, error) {
	if existing == nil {
		return FileChange{}, fmt.Errorf("%w: %q does not exist", ErrInvalidPatch, clean)
	}
	if change.Content != "" {
		return FileChange{}, fmt.Errorf("%w: a delete of %q carries content", ErrInvalidPatch, clean)
	}
	if change.ExpectedDigest != "" && change.ExpectedDigest != existing.Digest {
		return FileChange{}, fmt.Errorf("%w: %q expects %s and holds %s",
			ErrDigestMismatch, clean, change.ExpectedDigest, existing.Digest)
	}
	before, info, err := readBefore(scope, clean)
	if err != nil {
		return FileChange{}, err
	}
	return FileChange{
		Path:         clean,
		Operation:    Delete,
		BeforeDigest: existing.Digest,
		Diff:         unifiedDiff(clean, before, nil),
		before:       before,
		beforeInfo:   info,
		beforeMode:   info.Mode(),
	}, nil
}

// readBefore captures the bytes and the identity of a file the patch replaces.
//
// The identity is captured here, at plan time, and the publication is made
// conditional on it. A file that changes between the plan and the commit is
// therefore refused by the operating system rather than by a second check that
// would have its own window.
func readBefore(scope *agentworkspace.Scope, clean string) ([]byte, fs.FileInfo, error) {
	content, err := scope.ReadFile(clean)
	if err != nil {
		return nil, nil, err
	}
	info, err := scope.Stat(clean)
	if err != nil {
		return nil, nil, err
	}
	return content, info, nil
}

// validateContent refuses content that is not the kind of text these artifacts
// hold.
//
// Ptah's artifacts are SQL, HCL, YAML and Go: all text. A NUL byte or invalid
// UTF-8 in one of them is either a mistake or an attempt to smuggle something
// past a reader, and neither is worth writing to disk to find out.
func validateContent(clean, content string) ([]byte, error) {
	if len(content) > agentworkspace.MaxFileBytes {
		return nil, fmt.Errorf("%w: %q is %d bytes, over the %d-byte limit",
			ErrInvalidPatch, clean, len(content), agentworkspace.MaxFileBytes)
	}
	if !utf8.ValidString(content) {
		return nil, fmt.Errorf("%w: %q is not valid UTF-8", ErrInvalidPatch, clean)
	}
	if strings.ContainsRune(content, 0) {
		return nil, fmt.Errorf("%w: %q contains a NUL byte", ErrInvalidPatch, clean)
	}
	return []byte(content), nil
}

// digestBytes is the one spelling of a content address in this package:
// `sha256:<hex>`, the same form internal/agentworkspace uses for a file and
// internal/atlasschema uses for a schema fingerprint.
func digestBytes(content []byte) string {
	sum := sha256.Sum256(content)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// patchID is the patch's own content address.
//
// It covers the class, the base digest and the effect of every change, and it
// deliberately does not cover [Patch.Summary]. An approval binds to what the
// patch does; rewording the description must not mint a new identity, and it
// must not be possible to change the description of an approved patch either.
func patchID(class agentpolicy.ArtifactClass, baseDigest string, files []FileChange) string {
	canonical := &strings.Builder{}
	fmt.Fprintf(canonical, "artifact %s\n", class)
	fmt.Fprintf(canonical, "base %s\n", baseDigest)
	for _, file := range files {
		fmt.Fprintf(canonical, "%s %s %s\n", file.Operation, file.Path, file.AfterDigest)
	}
	sum := sha256.Sum256([]byte(canonical.String()))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// resultDigest computes the artifact digest the plan produces, by applying the
// changes to the listing rather than to the disk.
//
// It is the same manifest computation the real digest uses, so the value a
// preview promises and the value an apply measures afterwards are comparable.
// They are compared: an apply whose measured result differs from its promised
// one reports the difference instead of the promise.
func resultDigest(entries []agentworkspace.Entry, files []FileChange) string {
	projected := make(map[string]agentworkspace.Entry, len(entries))
	for _, entry := range entries {
		projected[entry.Path] = entry
	}
	for _, file := range files {
		if file.Operation == Delete {
			delete(projected, file.Path)
			continue
		}
		projected[file.Path] = agentworkspace.Entry{
			Path:   file.Path,
			Size:   int64(file.Bytes),
			Digest: file.AfterDigest,
		}
	}
	listed := make([]agentworkspace.Entry, 0, len(projected))
	for _, entry := range projected {
		listed = append(listed, entry)
	}
	return agentworkspace.DigestOf(listed)
}
