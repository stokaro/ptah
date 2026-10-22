// Package gitpull is the git+https:// and git+ssh:// side of `ptah schema
// pull` and `ptah migrations pull`: the flags that say which commit and which
// path, the fetch, the files it writes, and the report.
package gitpull

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/spf13/pflag"

	"ptah.run/internal/gitsource"
	"ptah.run/internal/pathguard"
)

// Flag names, shared by both pull commands.
const (
	BranchFlag       = "branch"
	TagFlag          = "tag"
	CommitFlag       = "commit"
	PathFlag         = "path"
	ExpectDigestFlag = "expect-digest"
	CAFileFlag       = "ca-file"
	KnownHostsFlag   = "known-hosts-file"
)

// Flags are the Git-only flags of a pull.
type Flags struct {
	Branch, Tag, Commit string
	Path                string
	ExpectDigest        string
	CAFile              string
	KnownHostsFile      string
}

// Register adds the Git flags. what says what --path selects.
func Register(flags *pflag.FlagSet, f *Flags, what string) {
	flags.StringVar(&f.Branch, BranchFlag, "", "Git: the branch to read")
	flags.StringVar(&f.Tag, TagFlag, "", "Git: the tag to read")
	flags.StringVar(&f.Commit, CommitFlag, "", "Git: the full commit id to read; with --branch or --tag, the commit it named")
	flags.StringVar(&f.Path, PathFlag, "", "Git: the "+what+" in the commit's tree, slash-separated (required for Git)")
	flags.StringVar(&f.ExpectDigest, ExpectDigestFlag, "", "Git: refuse files whose content digest is not this sha256:...")
	flags.StringVar(&f.CAFile, CAFileFlag, "", "Git over HTTPS: the only roots the server's certificate may chain to")
	flags.StringVar(&f.KnownHostsFile, KnownHostsFlag, "", "Git over SSH: the server's host keys (required for SSH)")
}

// Names are the Git flag names, for refusing them on another source.
var Names = []string{BranchFlag, TagFlag, CommitFlag, PathFlag, ExpectDigestFlag, CAFileFlag, KnownHostsFlag}

// RefuseOnOtherSource names the first Git flag set on a reference that is
// not a Git one.
func RefuseOnOtherSource(flags *pflag.FlagSet) error {
	for _, name := range Names {
		if flags.Changed(name) {
			return fmt.Errorf("--%s is for a git+https:// or git+ssh:// reference", name)
		}
	}
	return nil
}

// Fetch reads what the flags select from a Git reference, with the credential
// the environment holds.
func Fetch(ctx context.Context, raw string, f Flags, kind gitsource.Kind) (gitsource.Result, error) {
	reference, err := gitsource.ParseReference(raw)
	if err != nil {
		return gitsource.Result{}, err
	}
	if f.Path == "" {
		return gitsource.Result{}, fmt.Errorf("--%s is required for a Git reference", PathFlag)
	}
	credentials, err := gitsource.CredentialsFromEnvironment(os.Getenv, os.ReadFile)
	if err != nil {
		return gitsource.Result{}, err
	}
	transport, auth, err := gitsource.Dial(reference, credentials, gitsource.Trust{CAFile: f.CAFile, KnownHostsFile: f.KnownHostsFile})
	if err != nil {
		return gitsource.Result{}, err
	}
	return gitsource.Fetch(ctx, gitsource.Options{
		Reference: reference, Transport: transport, Auth: auth,
		Ref:  gitsource.Ref{Branch: f.Branch, Tag: f.Tag, Commit: f.Commit},
		Path: f.Path, Kind: kind, ExpectDigest: f.ExpectDigest,
	})
}

// WriteFile writes the one selected file's exact bytes to a new file at out,
// and returns its absolute path.
func WriteFile(result gitsource.Result, out string) (string, error) {
	if len(result.Files) != 1 {
		return "", fmt.Errorf("a schema pull selects one file, and %d arrived", len(result.Files))
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
	if err := create(root, filepath.Base(output), result.Files[0].Data); err != nil {
		return "", err
	}
	return output, nil
}

// WriteDirectory writes the selected files under a new directory at out, at
// their paths relative to the selected directory, and returns its absolute
// path. Nothing of it is left behind when a file cannot be written.
func WriteDirectory(result gitsource.Result, out string) (written string, err error) {
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
	for _, file := range result.Files {
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

// Report is the --json document of a Git pull.
type Report struct {
	Source        string       `json:"source"`
	Reference     string       `json:"reference"`
	Commit        string       `json:"commit"`
	Path          string       `json:"path"`
	ContentDigest string       `json:"content_digest"`
	Files         []ReportFile `json:"files"`
	Written       []string     `json:"written"`
}

// ReportFile is one file the pull read.
type ReportFile struct {
	Path   string `json:"path"`
	Size   int    `json:"size"`
	SHA256 string `json:"sha256"`
}

// PrintText writes what a pull did for a person.
func PrintText(out io.Writer, reference string, result gitsource.Result, written string) error {
	_, err := fmt.Fprintf(out, "Pulled %s at commit %s to %s\nCommit: %s\nContent digest: %s\n",
		reference, result.Commit, written, result.Commit, result.ContentDigest)
	return err
}

// PrintJSON writes what a pull did as one JSON document.
func PrintJSON(out io.Writer, reference, selected string, result gitsource.Result, written string) error {
	report := Report{
		Source: "git", Reference: reference, Commit: result.Commit, Path: selected,
		ContentDigest: result.ContentDigest, Written: []string{written}, Files: make([]ReportFile, 0, len(result.Files)),
	}
	for _, file := range result.Files {
		sum := sha256.Sum256(file.Data)
		report.Files = append(report.Files, ReportFile{Path: file.Path, Size: len(file.Data), SHA256: hex.EncodeToString(sum[:])})
	}
	// The order the manifest has, so two pulls of the same content print the
	// same document.
	slices.SortFunc(report.Files, func(a, b ReportFile) int { return strings.Compare(a.Path, b.Path) })
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}
