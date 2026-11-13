// Package sourcepull is the Git and HTTPS side of `ptah schema pull` and `ptah
// migrations pull`: the flags that say what to read, the fetch, the files it
// writes, and the report. An oci:// reference is the commands' own.
package sourcepull

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/spf13/pflag"

	"ptah.run/internal/gitsource"
	"ptah.run/internal/httpsource"
	"ptah.run/internal/pathguard"
	"ptah.run/internal/sourcearchive"
	"ptah.run/internal/sourcecontent"
)

// Flag names, shared by both pull commands.
const (
	PathFlag                 = "path"
	ExpectDigestFlag         = "expect-digest"
	CAFileFlag               = "ca-file"
	BranchFlag               = "branch"
	TagFlag                  = "tag"
	CommitFlag               = "commit"
	KnownHostsFlag           = "known-hosts-file"
	FormatFlag               = "format"
	ExpectDownloadDigestFlag = "expect-download-digest"
)

// Flags are the flags of a Git or an HTTPS pull.
type Flags struct {
	// Git and HTTPS.
	Path         string
	ExpectDigest string
	CAFile       string
	// Git.
	Branch, Tag, Commit string
	KnownHostsFile      string
	// HTTPS.
	Format               string
	ExpectDownloadDigest string
}

// Kind is what a pull selects.
type Kind = sourcearchive.Kind

// What a pull selects: a schema's one file or a migration directory.
const (
	File      = sourcearchive.File
	Directory = sourcearchive.Directory
)

// Register adds the Git and HTTPS flags of a pull that selects kind.
func Register(flags *pflag.FlagSet, f *Flags, kind Kind) {
	path := "Git: the schema file in the commit's tree; HTTPS: the file's name with --format file, or the schema file in the archive (required for Git and HTTPS)"
	if kind == Directory {
		path = `Git: the migration directory in the commit's tree; HTTPS: the migration directory in the archive; "." for the root (required for Git and HTTPS)`
	}
	flags.StringVar(&f.Path, PathFlag, "", path)
	flags.StringVar(&f.ExpectDigest, ExpectDigestFlag, "", "Git and HTTPS: refuse files whose content digest is not this sha256:...")
	flags.StringVar(&f.CAFile, CAFileFlag, "", "Git over HTTPS and HTTPS: the only roots the server's certificate may chain to")
	flags.StringVar(&f.Branch, BranchFlag, "", "Git: the branch to read")
	flags.StringVar(&f.Tag, TagFlag, "", "Git: the tag to read")
	flags.StringVar(&f.Commit, CommitFlag, "", "Git: the full commit id to read; with --branch or --tag, the commit it named")
	flags.StringVar(&f.KnownHostsFile, KnownHostsFlag, "", "Git over SSH: the server's host keys (required for SSH)")
	format := "HTTPS: how the download carries the schema: file, tar.gz or zip (required for HTTPS)"
	if kind == Directory {
		format = "HTTPS: the archive the migration directory comes in: tar.gz or zip (required for HTTPS)"
	}
	flags.StringVar(&f.Format, FormatFlag, "", format)
	flags.StringVar(&f.ExpectDownloadDigest, ExpectDownloadDigestFlag, "", "HTTPS: the sha256:... of the exact bytes the URL serves (required for HTTPS)")
}

// Source is the kind of reference a pull reads.
type Source int

const (
	// OCI is an oci:// reference, or anything that is neither of the others.
	OCI Source = iota
	// Git is a git+https:// or git+ssh:// reference.
	Git
	// HTTPS is an https:// reference; http:// is one too, for its refusal.
	HTTPS
)

// SourceOf says which source a reference names.
func SourceOf(reference string) Source {
	switch {
	case gitsource.IsReference(reference):
		return Git
	case httpsource.IsReference(reference):
		return HTTPS
	}
	return OCI
}

// The flags each source takes, beside the ones it shares.
var (
	sharedFlags = []string{PathFlag, ExpectDigestFlag, CAFileFlag}
	gitFlags    = []string{BranchFlag, TagFlag, CommitFlag, KnownHostsFlag}
	httpsFlags  = []string{FormatFlag, ExpectDownloadDigestFlag}
)

// Names are every Git and HTTPS flag name.
var Names = slices.Concat(sharedFlags, gitFlags, httpsFlags)

// RefuseFlags names the first flag set that source does not take.
func RefuseFlags(flags *pflag.FlagSet, source Source) error {
	refused := map[Source][]string{
		OCI:   Names,
		Git:   httpsFlags,
		HTTPS: gitFlags,
	}[source]
	for _, name := range refused {
		if flags.Changed(name) {
			return fmt.Errorf("--%s is for %s", name, takenBy(name))
		}
	}
	return nil
}

func takenBy(name string) string {
	switch {
	case slices.Contains(gitFlags, name):
		return "a git+https:// or git+ssh:// reference"
	case slices.Contains(httpsFlags, name):
		return "an https:// reference"
	}
	return "a Git or an https:// reference"
}

// WriteFile writes the one selected file's exact bytes to a new file at out,
// and returns its absolute path.
func WriteFile(files []sourcecontent.File, out string) (string, error) {
	if len(files) != 1 {
		return "", fmt.Errorf("a schema pull selects one file, and %d arrived", len(files))
	}
	output, err := absent(out)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(output), 0o700); err != nil {
		return "", fmt.Errorf("create the output's directory: %w", err)
	}
	root, err := os.OpenRoot(filepath.Dir(output))
	if err != nil {
		return "", fmt.Errorf("open the output's directory: %w", err)
	}
	defer func() { _ = root.Close() }()
	if err := create(root, filepath.Base(output), files[0].Data); err != nil {
		return "", err
	}
	return output, nil
}

// WriteDirectory writes the selected files under a new directory at out, at
// their paths relative to the selected directory, and returns its absolute
// path. Nothing of it is left behind when a file cannot be written.
func WriteDirectory(files []sourcecontent.File, out string) (written string, err error) {
	output, err := absent(out)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(output, 0o700); err != nil {
		return "", fmt.Errorf("create the output directory: %w", err)
	}
	defer func() {
		if err != nil {
			_ = os.RemoveAll(output)
		}
	}()
	root, err := os.OpenRoot(output)
	if err != nil {
		return "", fmt.Errorf("open the output directory: %w", err)
	}
	defer func() { _ = root.Close() }()
	for _, file := range files {
		name := filepath.FromSlash(file.Path)
		if dir := filepath.Dir(name); dir != "." {
			if err := root.MkdirAll(dir, 0o700); err != nil {
				return "", fmt.Errorf("create %s: %w", dir, err)
			}
		}
		if err := create(root, name, file.Data); err != nil {
			return "", err
		}
	}
	return output, nil
}

// absent resolves out and refuses one that exists: a pull never writes over
// anything.
func absent(out string) (string, error) {
	if out == "" {
		return "", errors.New("--out is required")
	}
	output, err := pathguard.ResolveCLIPath(out)
	if err != nil {
		return "", fmt.Errorf("resolve --out: %w", err)
	}
	if _, err := os.Lstat(output); err == nil {
		return "", fmt.Errorf("the output already exists: %s", output)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("stat the output: %w", err)
	}
	return output, nil
}

func create(root *os.Root, name string, data []byte) error {
	file, err := root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create %s: %w", name, err)
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return fmt.Errorf("write %s: %w", name, err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync %s: %w", name, err)
	}
	return file.Close()
}

// ReportFile is one file a pull read.
type ReportFile struct {
	Path   string `json:"path"`
	Size   int    `json:"size"`
	SHA256 string `json:"sha256"`
}

// reportFiles lists files in the order the manifest has, so two pulls of the
// same content print the same document.
func reportFiles(files []sourcecontent.File) []ReportFile {
	listed := make([]ReportFile, 0, len(files))
	for _, file := range files {
		sum := sha256.Sum256(file.Data)
		listed = append(listed, ReportFile{Path: file.Path, Size: len(file.Data), SHA256: hex.EncodeToString(sum[:])})
	}
	slices.SortFunc(listed, func(a, b ReportFile) int { return strings.Compare(a.Path, b.Path) })
	return listed
}
