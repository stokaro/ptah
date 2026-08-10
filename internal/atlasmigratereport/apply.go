// Package atlasmigratereport renders Atlas-compatible "migrate apply" report
// output from Ptah runtime results, resolving the applied migration filesystem
// and connection details the report templates need.
package atlasmigratereport

import (
	"fmt"
	"io"
	"io/fs"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/atlasreport"
)

type ApplyFormatOptions struct {
	Conn *dbschema.DatabaseConnection
	// FS is the immutable migration filesystem that was applied.
	FS     fs.FS
	Dir    string
	URL    string
	Result atlasmigrate.ApplyResult
}

// WriteApplyFormat renders Atlas migrate apply format output from the runtime
// result produced by internal/atlasmigrate.
func WriteApplyFormat(w io.Writer, format string, opts ApplyFormatOptions) error {
	if opts.FS == nil {
		return fmt.Errorf("migrate apply format requires migration filesystem")
	}
	result := opts.Result
	return atlasreport.WriteMigrateApplyFormat(w, format, atlasreport.MigrateApplyResultOptions{
		Conn:             opts.Conn,
		FS:               opts.FS,
		Dir:              opts.Dir,
		URL:              opts.URL,
		Status:           result.Status,
		Migrations:       result.Migrations,
		SelectedVersions: result.SelectedVersions,
		SelectedKeys:     result.SelectedKeys,
		CurrentVersion:   result.CurrentVersion,
		CurrentKey:       result.CurrentKey,
		ErrorText:        result.ErrorText,
		ApplyError:       result.ApplyError,
		Applied:          result.Applied,
		StartedAt:        result.StartedAt,
		EndedAt:          result.EndedAt,
	})
}
