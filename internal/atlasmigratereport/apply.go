// Package atlasmigratereport renders Atlas-compatible "migrate apply" report
// output from Ptah runtime results, resolving the applied migration filesystem
// and connection details the report templates need.
package atlasmigratereport

import (
	"io"
	"io/fs"
	"os"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/atlasmigrate"
	"github.com/stokaro/ptah/internal/atlasreport"
)

type ApplyFormatOptions struct {
	Conn *dbschema.DatabaseConnection
	// FS is the migration filesystem that was applied. It must be set for
	// non-Atlas source formats, whose executed migrations live only in memory
	// after conversion; when nil, the report reads ResolvedDir from disk.
	FS          fs.FS
	ResolvedDir string
	Dir         string
	URL         string
	Result      atlasmigrate.ApplyResult
}

// WriteApplyFormat renders Atlas migrate apply format output from the runtime
// result produced by internal/atlasmigrate.
func WriteApplyFormat(w io.Writer, format string, opts ApplyFormatOptions) error {
	result := opts.Result
	migrationFS := opts.FS
	if migrationFS == nil {
		migrationFS = os.DirFS(opts.ResolvedDir)
	}
	return atlasreport.WriteMigrateApplyFormat(w, format, atlasreport.MigrateApplyResultOptions{
		Conn:             opts.Conn,
		FS:               migrationFS,
		Dir:              opts.Dir,
		URL:              opts.URL,
		Status:           result.Status,
		Migrations:       result.Migrations,
		SelectedVersions: result.SelectedVersions,
		CurrentVersion:   result.CurrentVersion,
		ErrorText:        result.ErrorText,
		ApplyError:       result.ApplyError,
		Applied:          result.Applied,
		StartedAt:        result.StartedAt,
		EndedAt:          result.EndedAt,
	})
}
