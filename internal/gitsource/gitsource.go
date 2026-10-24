// Package gitsource fetches a schema file or a migration directory from a Git
// repository: one commit, shallowly, and only the selected path of its tree,
// with no working tree, hook, filter, submodule or LFS step.
//
// A reference is git+https:// or git+ssh:// and names the repository. The
// commit comes from a branch, a tag or a commit id, and the files from a path
// in that commit's tree. The result carries the commit and the content digest
// stokaro/ptah-operator defines in its internal/sourcecontent, which
// [sourcecontent] computes here, so the operator can bind a plan to what a
// pull fetched and refuse anything else later.
package gitsource

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"regexp"
	"strings"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/format/packfile"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/capability"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/sideband"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/storage/memory"

	"ptah.run/internal/sourcecontent"
)

// Limits on what one pull reads.
const (
	// MaxContentBytes bounds the selected files together.
	MaxContentBytes = 64 << 20
	// MaxFetchBytes bounds the pack data a pull receives, every attempt
	// together.
	MaxFetchBytes = 64 << 20
)

// deepenSteps are the depths a pinned commit is looked for along its branch
// or tag when the server does not give it by its id. Each attempt is a
// fetch of its own, all of them within MaxFetchBytes.
var deepenSteps = []int{16, 256, 4096}

const (
	schemeHTTPS = "git+https"
	schemeSSH   = "git+ssh"
	lfsPointer  = "version https://git-lfs.github.com/spec/v1\n"
	// lfsPointerMaxBytes is the most a pointer file holds; Git LFS keeps them
	// far below it, and a larger file is ordinary content.
	lfsPointerMaxBytes = 1024
)

var commitID = regexp.MustCompile(`^[0-9a-f]{40}$`)

// The refusals, matched with errors.Is. Each message says which reference,
// ref or path.
var (
	ErrReference     = errors.New("invalid Git reference")
	ErrRef           = errors.New("invalid Git ref selection")
	ErrNotFound      = errors.New("not in the repository")
	ErrCommit        = errors.New("commit not available")
	ErrSelection     = errors.New("selected path refused")
	ErrSubmodule     = errors.New("selected path is a submodule")
	ErrLFS           = errors.New("selected path is a Git LFS pointer")
	ErrSymlink       = errors.New("selected path is a symbolic link")
	ErrTooLarge      = errors.New("source too large")
	ErrDigest        = errors.New("content digest differs")
	ErrRedirect      = errors.New("redirect refused")
	ErrNoKnownHosts  = errors.New("SSH needs known hosts")
	ErrUnsupportedID = errors.New("server does not give this commit by its id")
)

// Kind is what a path selects.
type Kind int

const (
	// File selects one file, as a schema is.
	File Kind = iota
	// Directory selects every file under a directory, as a migration
	// directory is.
	Directory
)

// Reference is a parsed git+https:// or git+ssh:// reference.
type Reference struct {
	// Raw is the reference as given.
	Raw string
	// URL is what Git dials: https:// or ssh://.
	URL *url.URL
}

// IsReference reports whether s is a Git reference rather than another kind
// of source.
func IsReference(s string) bool {
	return strings.HasPrefix(s, schemeHTTPS+"://") || strings.HasPrefix(s, schemeSSH+"://")
}

// ParseReference reads a git+https:// or git+ssh:// reference. It names a
// repository and nothing else: no credential, query or fragment, since a
// password in a URL lands in shell history and process lists.
func ParseReference(raw string) (Reference, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return Reference{}, fmt.Errorf("%w %q: %w", ErrReference, raw, err)
	}
	dial := *parsed
	switch parsed.Scheme {
	case schemeHTTPS:
		dial.Scheme = "https"
		if parsed.User != nil {
			return Reference{}, fmt.Errorf("%w %q: put the credential in PTAH_GIT_USERNAME and PTAH_GIT_PASSWORD, not in the URL", ErrReference, raw)
		}
	case schemeSSH:
		dial.Scheme = "ssh"
		if _, hasPassword := parsed.User.Password(); hasPassword {
			return Reference{}, fmt.Errorf("%w %q: SSH authenticates with a key in PTAH_GIT_SSH_KEY_FILE, not a password in the URL", ErrReference, raw)
		}
	default:
		return Reference{}, fmt.Errorf("%w %q: it is neither git+https:// nor git+ssh://", ErrReference, raw)
	}
	switch {
	case parsed.Host == "":
		return Reference{}, fmt.Errorf("%w %q: no host", ErrReference, raw)
	case parsed.Path == "" || parsed.Path == "/":
		return Reference{}, fmt.Errorf("%w %q: no repository path", ErrReference, raw)
	case parsed.RawQuery != "" || parsed.Fragment != "" || parsed.ForceQuery:
		return Reference{}, fmt.Errorf("%w %q: no query or fragment; name the ref and the path with their flags", ErrReference, raw)
	}
	return Reference{Raw: raw, URL: &dial}, nil
}

// Origin is the scheme, host and port the reference dials.
func (r Reference) Origin() string {
	return r.URL.Scheme + "://" + r.URL.Host
}

// Ref says which commit to read: a branch or a tag, a commit, or a commit
// with the branch or tag that named it when it was resolved.
type Ref struct {
	Branch string
	Tag    string
	Commit string
}

func (r Ref) validate() error {
	switch {
	case r.Branch != "" && r.Tag != "":
		return fmt.Errorf("%w: name a branch or a tag, not both", ErrRef)
	case r.Branch == "" && r.Tag == "" && r.Commit == "":
		return fmt.Errorf("%w: name a branch, a tag or a commit", ErrRef)
	case r.Commit != "" && !commitID.MatchString(r.Commit):
		return fmt.Errorf("%w: commit %q is not a full 40-character lowercase id", ErrRef, r.Commit)
	}
	for _, name := range []string{r.Branch, r.Tag} {
		if name != "" && plumbing.ReferenceName("refs/heads/"+name).Validate() != nil {
			return fmt.Errorf("%w: %q is not a ref name", ErrRef, name)
		}
	}
	return nil
}

// name is the full ref name the branch or tag stands for, or "".
func (r Ref) name() plumbing.ReferenceName {
	switch {
	case r.Branch != "":
		return plumbing.NewBranchReferenceName(r.Branch)
	case r.Tag != "":
		return plumbing.NewTagReferenceName(r.Tag)
	}
	return ""
}

// Options say what to fetch and how to reach it.
type Options struct {
	Reference Reference
	Ref       Ref
	// Path is the selected path in the commit's tree: a file for File, a
	// directory for Directory, where "." is the root.
	Path string
	Kind Kind
	// ExpectDigest, when set, is the content digest the files have to have.
	ExpectDigest string
	// Transport and Auth reach the repository; see [Dial].
	Transport transport.Transport
	Auth      transport.AuthMethod
}

// Result is what a pull read.
type Result struct {
	// Commit is the id of the commit the files come from.
	Commit string
	// ContentDigest is the digest of Files, as sourcecontent defines it.
	ContentDigest string
	// Files are the selected files, with paths relative to the selected
	// directory, or the file's own name for a selected file.
	Files []sourcecontent.File
}

// Fetch reads the selected files of one commit.
func Fetch(ctx context.Context, o Options) (Result, error) {
	if err := o.Ref.validate(); err != nil {
		return Result{}, err
	}
	selected, err := selection(o.Path, o.Kind)
	if err != nil {
		return Result{}, err
	}
	endpoint, err := transport.NewEndpoint(o.Reference.URL.String())
	if err != nil {
		return Result{}, fmt.Errorf("%w %q: %w", ErrReference, o.Reference.Raw, err)
	}
	f := &fetcher{options: o, endpoint: endpoint, budget: MaxFetchBytes}
	storage, commit, err := f.commit(ctx)
	if err != nil {
		return Result{}, err
	}
	files, err := read(storage, commit, selected, o.Kind)
	if err != nil {
		return Result{}, err
	}
	digest, err := sourcecontent.Digest(files)
	if err != nil {
		return Result{}, fmt.Errorf("%w: %s at %s: %w", ErrSelection, o.Path, commit.Hash, err)
	}
	if o.ExpectDigest != "" && digest != o.ExpectDigest {
		return Result{}, fmt.Errorf("%w: %s at commit %s has %s, not %s", ErrDigest, o.Path, commit.Hash, digest, o.ExpectDigest)
	}
	return Result{Commit: commit.Hash.String(), ContentDigest: digest, Files: files}, nil
}

// selection cleans a selected path. A file needs a path; a directory may be
// the root.
func selection(selected string, kind Kind) (string, error) {
	if selected == "" {
		return "", fmt.Errorf("%w: no path given", ErrSelection)
	}
	if selected == "." && kind == Directory {
		return "", nil
	}
	if _, err := sourcecontent.Manifest([]sourcecontent.File{{Path: selected}}); err != nil {
		return "", fmt.Errorf("%w: %w", ErrSelection, err)
	}
	return selected, nil
}

// fetcher fetches into a fresh in-memory store per attempt, charging every
// attempt to one budget.
type fetcher struct {
	options  Options
	endpoint *transport.Endpoint
	budget   int64
}

// commit fetches the commit the options name and returns it with the store
// that holds it and its tree.
func (f *fetcher) commit(ctx context.Context) (*memory.Storage, *object.Commit, error) {
	advertised, err := f.advertised(ctx)
	if err != nil {
		return nil, nil, err
	}
	ref := f.options.Ref
	// A ref is asked for by the value the server advertised, which every
	// server gives: for an annotated tag that is the tag object, and the
	// commit it names is what the files are read from.
	tipValue, tip := plumbing.ZeroHash, plumbing.ZeroHash
	if name := ref.name(); name != "" {
		tipValue, tip, err = resolve(advertised, name)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: %s %w", ErrNotFound, f.options.Reference.Raw, err)
		}
	}
	if ref.Commit == "" {
		return f.fetchOne(ctx, advertised, tipValue, tip, 1)
	}
	want := plumbing.NewHash(ref.Commit)
	switch {
	case tip == want:
		return f.fetchOne(ctx, advertised, tipValue, want, 1)
	case advertisedValue(advertised, want),
		advertised.Capabilities.Supports(capability.AllowReachableSHA1InWant):
		return f.fetchOne(ctx, advertised, want, want, 1)
	}
	var refused error
	if f.endpoint.Protocol == "https" {
		// Git's smart HTTP server answers each request on its own and cannot
		// know which refs the client was shown, so it gives any commit a
		// branch or tag reaches without saying so in its advertisement.
		storage, commit, err := f.fetchOne(ctx, advertised, want, want, 1)
		switch {
		case err == nil:
			return storage, commit, nil
		case errors.Is(err, ErrTooLarge), ctx.Err() != nil:
			return nil, nil, err
		}
		refused = err
	}
	if tip.IsZero() {
		if refused != nil {
			return nil, nil, fmt.Errorf("%w: %s: no branch or tag names commit %s, and the server did not give it by its id: %w",
				ErrUnsupportedID, f.options.Reference.Raw, want, refused)
		}
		return nil, nil, fmt.Errorf("%w: %s: no branch or tag names commit %s, and the server gives commits only at a branch or tag",
			ErrUnsupportedID, f.options.Reference.Raw, want)
	}
	// The branch or tag moved on. Look for the commit along it, deeper each
	// time, until the fetch budget runs out.
	for _, depth := range deepenSteps {
		storage, commit, err := f.fetchOne(ctx, advertised, tipValue, want, depth)
		switch {
		case err == nil:
			return storage, commit, nil
		case !errors.Is(err, errNotFetched):
			return nil, nil, err
		}
	}
	return nil, nil, fmt.Errorf("%w: commit %s is not among the last %d commits of %s in %s; it may have been force-pushed away",
		ErrCommit, want, deepenSteps[len(deepenSteps)-1], ref.name().Short(), f.options.Reference.Raw)
}

// errNotFetched is a fetch that ended without the commit it was after.
var errNotFetched = errors.New("commit not in the fetched history")

func (f *fetcher) advertised(ctx context.Context) (*packp.AdvRefs, error) {
	session, err := f.options.Transport.NewUploadPackSession(f.endpoint, f.options.Auth)
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", f.options.Reference.Raw, err)
	}
	defer func() { _ = session.Close() }()
	advertised, err := session.AdvertisedReferencesContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("list the refs of %s: %w", f.options.Reference.Raw, err)
	}
	return advertised, nil
}

// fetchOne fetches want, at most depth commits deep, and returns target from
// what arrived.
func (f *fetcher) fetchOne(ctx context.Context, advertised *packp.AdvRefs, want, target plumbing.Hash, depth int) (*memory.Storage, *object.Commit, error) {
	session, err := f.options.Transport.NewUploadPackSession(f.endpoint, f.options.Auth)
	if err != nil {
		return nil, nil, fmt.Errorf("connect to %s: %w", f.options.Reference.Raw, err)
	}
	defer func() { _ = session.Close() }()
	// The advertisement opens every upload-pack conversation, and a session
	// answers UploadPack only once it has read it.
	if _, err := session.AdvertisedReferencesContext(ctx); err != nil {
		return nil, nil, fmt.Errorf("list the refs of %s: %w", f.options.Reference.Raw, err)
	}
	request := packp.NewUploadPackRequestFromCapabilities(advertised.Capabilities)
	request.Wants = []plumbing.Hash{want}
	request.Depth = packp.DepthCommits(depth)
	if err := request.Capabilities.Set(capability.Shallow); err != nil {
		return nil, nil, err
	}
	if advertised.Capabilities.Supports(capability.NoProgress) {
		if err := request.Capabilities.Set(capability.NoProgress); err != nil {
			return nil, nil, err
		}
	}
	response, err := session.UploadPack(ctx, request)
	if err != nil {
		return nil, nil, fmt.Errorf("fetch %s from %s: %w", want, f.options.Reference.Raw, err)
	}
	defer func() { _ = response.Close() }()
	storage := memory.NewStorage()
	limited := &budgetReader{reader: demux(request.Capabilities, response), budget: &f.budget}
	if err := packfile.UpdateObjectStorage(storage, limited); err != nil {
		if limited.exceeded {
			return nil, nil, fmt.Errorf("%w: fetching %s received more than %d bytes", ErrTooLarge, f.options.Reference.Raw, MaxFetchBytes)
		}
		return nil, nil, fmt.Errorf("read the pack of %s: %w", f.options.Reference.Raw, err)
	}
	commit, err := object.GetCommit(storage, target)
	if errors.Is(err, plumbing.ErrObjectNotFound) {
		return nil, nil, errNotFetched
	}
	if err != nil {
		return nil, nil, fmt.Errorf("read commit %s: %w", target, err)
	}
	return storage, commit, nil
}

func demux(capabilities *capability.List, reader io.Reader) io.Reader {
	switch {
	case capabilities.Supports(capability.Sideband64k):
		return sideband.NewDemuxer(sideband.Sideband64k, reader)
	case capabilities.Supports(capability.Sideband):
		return sideband.NewDemuxer(sideband.Sideband, reader)
	}
	return reader
}

// budgetReader fails a read past the fetch budget all attempts share.
type budgetReader struct {
	reader   io.Reader
	budget   *int64
	exceeded bool
}

func (b *budgetReader) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	*b.budget -= int64(n)
	if *b.budget < 0 {
		b.exceeded = true
		return n, ErrTooLarge
	}
	return n, err
}

// resolve is the value the server advertised for a branch or tag, and the
// commit it names, through an annotated tag.
func resolve(advertised *packp.AdvRefs, name plumbing.ReferenceName) (value, commit plumbing.Hash, err error) {
	value, ok := advertised.References[name.String()]
	if !ok {
		return plumbing.ZeroHash, plumbing.ZeroHash, fmt.Errorf("has no %s", name.Short())
	}
	if peeled, ok := advertised.Peeled[name.String()]; ok {
		return value, peeled, nil
	}
	return value, value, nil
}

// advertisedValue reports whether a ref the server advertised has hash as its
// value, which every server gives without being asked for anything by id.
func advertisedValue(advertised *packp.AdvRefs, hash plumbing.Hash) bool {
	for _, value := range advertised.References {
		if value == hash {
			return true
		}
	}
	return false
}

// read collects the selected files of the commit's tree.
func read(storage *memory.Storage, commit *object.Commit, selected string, kind Kind) ([]sourcecontent.File, error) {
	tree, err := commit.Tree()
	if err != nil {
		return nil, fmt.Errorf("read the tree of commit %s: %w", commit.Hash, err)
	}
	if kind == File {
		entry, err := tree.FindEntry(selected)
		if err != nil {
			return nil, fmt.Errorf("%w: %s at commit %s", ErrNotFound, selected, commit.Hash)
		}
		file, err := blob(storage, selected, entry, &contentBudget{left: MaxContentBytes})
		if err != nil {
			return nil, err
		}
		file.Path = path.Base(selected)
		return []sourcecontent.File{file}, nil
	}
	if selected != "" {
		entry, err := tree.FindEntry(selected)
		switch {
		case err != nil:
			return nil, fmt.Errorf("%w: %s at commit %s", ErrNotFound, selected, commit.Hash)
		case entry.Mode == filemode.Submodule:
			return nil, fmt.Errorf("%w: %s", ErrSubmodule, selected)
		case entry.Mode != filemode.Dir:
			return nil, fmt.Errorf("%w: %s is not a directory", ErrSelection, selected)
		}
		if tree, err = object.GetTree(storage, entry.Hash); err != nil {
			return nil, fmt.Errorf("read the tree of %s: %w", selected, err)
		}
	}
	var files []sourcecontent.File
	budget := &contentBudget{left: MaxContentBytes}
	walker := object.NewTreeWalker(tree, true, nil)
	defer walker.Close()
	for {
		name, entry, err := walker.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("walk %s: %w", selected, err)
		}
		if entry.Mode == filemode.Dir {
			continue
		}
		file, err := blob(storage, path.Join(selected, name), &entry, budget)
		if err != nil {
			return nil, err
		}
		file.Path = name
		files = append(files, file)
		if len(files) > sourcecontent.MaxFiles {
			return nil, fmt.Errorf("%w: %s holds more than %d files", ErrTooLarge, selected, sourcecontent.MaxFiles)
		}
	}
	return files, nil
}

// contentBudget is what the selected files may still hold together.
type contentBudget struct{ left int64 }

// blob reads one selected file, refusing what is not plain content.
func blob(storage *memory.Storage, at string, entry *object.TreeEntry, budget *contentBudget) (sourcecontent.File, error) {
	switch entry.Mode {
	case filemode.Regular, filemode.Executable, filemode.Deprecated:
	case filemode.Submodule:
		return sourcecontent.File{}, fmt.Errorf("%w: %s", ErrSubmodule, at)
	case filemode.Symlink:
		return sourcecontent.File{}, fmt.Errorf("%w: %s", ErrSymlink, at)
	default:
		return sourcecontent.File{}, fmt.Errorf("%w: %s is not a file", ErrSelection, at)
	}
	stored, err := object.GetBlob(storage, entry.Hash)
	if err != nil {
		return sourcecontent.File{}, fmt.Errorf("read %s: %w", at, err)
	}
	if budget.left -= stored.Size; budget.left < 0 {
		return sourcecontent.File{}, fmt.Errorf("%w: the selected files hold more than %d bytes", ErrTooLarge, MaxContentBytes)
	}
	reader, err := stored.Reader()
	if err != nil {
		return sourcecontent.File{}, fmt.Errorf("read %s: %w", at, err)
	}
	defer func() { _ = reader.Close() }()
	data, err := io.ReadAll(reader)
	if err != nil {
		return sourcecontent.File{}, fmt.Errorf("read %s: %w", at, err)
	}
	if len(data) <= lfsPointerMaxBytes && bytes.HasPrefix(data, []byte(lfsPointer)) {
		return sourcecontent.File{}, fmt.Errorf("%w: %s; Ptah reads the bytes in the repository and runs no LFS step", ErrLFS, at)
	}
	return sourcecontent.File{Data: data}, nil
}
