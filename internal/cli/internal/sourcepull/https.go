package sourcepull

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"ptah.run/internal/httpsource"
	"ptah.run/internal/sourcearchive"
)

// FetchHTTPS downloads an https:// reference, holds it to the download
// digest, and reads what the flags select, with the credential the
// environment holds.
func FetchHTTPS(ctx context.Context, raw string, f Flags, kind Kind) (httpsource.Result, error) {
	reference, err := httpsource.ParseReference(raw)
	if err != nil {
		return httpsource.Result{}, err
	}
	for _, required := range []struct{ name, value string }{
		{FormatFlag, f.Format}, {PathFlag, f.Path}, {ExpectDownloadDigestFlag, f.ExpectDownloadDigest},
	} {
		if required.value == "" {
			return httpsource.Result{}, fmt.Errorf("--%s is required for an https:// reference", required.name)
		}
	}
	format, err := sourcearchive.ParseFormat(f.Format)
	if err != nil {
		return httpsource.Result{}, fmt.Errorf("--%s: %w", FormatFlag, err)
	}
	credentials, err := httpsource.CredentialsFromEnvironment(os.Getenv)
	if err != nil {
		return httpsource.Result{}, err
	}
	return httpsource.Fetch(ctx, httpsource.Options{
		Reference: reference, Credentials: credentials, CAFile: f.CAFile,
		ExpectDownloadDigest: f.ExpectDownloadDigest, ExpectDigest: f.ExpectDigest,
		Archive: sourcearchive.Options{Format: format, Path: f.Path, Kind: kind},
	})
}

// HTTPSReport is the --json document of an HTTPS pull.
type HTTPSReport struct {
	Source         string       `json:"source"`
	Reference      string       `json:"reference"`
	Format         string       `json:"format"`
	DownloadDigest string       `json:"download_digest"`
	DownloadSize   int64        `json:"download_size"`
	Path           string       `json:"path"`
	ContentDigest  string       `json:"content_digest"`
	Files          []ReportFile `json:"files"`
	Written        []string     `json:"written"`
}

// PrintHTTPSText writes what an HTTPS pull did for a person.
func PrintHTTPSText(out io.Writer, reference string, result httpsource.Result, written string) error {
	_, err := fmt.Fprintf(out, "Pulled %s to %s\nDownload digest: %s\nContent digest: %s\n",
		reference, written, result.DownloadDigest, result.ContentDigest)
	return err
}

// PrintHTTPSJSON writes what an HTTPS pull did as one JSON document.
func PrintHTTPSJSON(out io.Writer, reference string, f Flags, result httpsource.Result, written string) error {
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(HTTPSReport{
		Source: "https", Reference: reference, Format: f.Format,
		DownloadDigest: result.DownloadDigest, DownloadSize: result.DownloadSize,
		Path: f.Path, ContentDigest: result.ContentDigest,
		Files: reportFiles(result.Files), Written: []string{written},
	})
}
