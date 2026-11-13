package sourcepull

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"ptah.run/internal/gitsource"
)

// FetchGit reads what the flags select from a Git reference, with the
// credential the environment holds.
func FetchGit(ctx context.Context, raw string, f Flags, kind Kind) (gitsource.Result, error) {
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
	selects := gitsource.File
	if kind == Directory {
		selects = gitsource.Directory
	}
	return gitsource.Fetch(ctx, gitsource.Options{
		Reference: reference, Transport: transport, Auth: auth,
		Ref:  gitsource.Ref{Branch: f.Branch, Tag: f.Tag, Commit: f.Commit},
		Path: f.Path, Kind: selects, ExpectDigest: f.ExpectDigest,
	})
}

// GitReport is the --json document of a Git pull.
type GitReport struct {
	Source        string       `json:"source"`
	Reference     string       `json:"reference"`
	Commit        string       `json:"commit"`
	Path          string       `json:"path"`
	ContentDigest string       `json:"content_digest"`
	Files         []ReportFile `json:"files"`
	Written       []string     `json:"written"`
}

// PrintGitText writes what a Git pull did for a person.
func PrintGitText(out io.Writer, reference string, result gitsource.Result, written string) error {
	_, err := fmt.Fprintf(out, "Pulled %s at commit %s to %s\nCommit: %s\nContent digest: %s\n",
		reference, result.Commit, written, result.Commit, result.ContentDigest)
	return err
}

// PrintGitJSON writes what a Git pull did as one JSON document.
func PrintGitJSON(out io.Writer, reference, selected string, result gitsource.Result, written string) error {
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(GitReport{
		Source: "git", Reference: reference, Commit: result.Commit, Path: selected,
		ContentDigest: result.ContentDigest, Written: []string{written}, Files: reportFiles(result.Files),
	})
}
