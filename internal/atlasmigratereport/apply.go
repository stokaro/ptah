// Package atlasmigratereport renders Atlas-compatible "migrate apply" report
// output from Ptah runtime results, resolving the applied migration filesystem
// and connection details the report templates need.
package atlasmigratereport

import (
	"fmt"
	"io"
	"io/fs"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasmigrate"
	"ptah.run/internal/atlasreport"
)

type ApplyFormatOptions struct {
	Conn *dbschema.DatabaseConnection
	// FS is the immutable migration filesystem that was applied.
	FS fs.FS
	// SourceFiles names the files of a layout whose conversion renamed them;
	// see [atlasreport.MigrateApplyResultOptions.SourceFiles].
	SourceFiles map[int64]atlasreport.MigrateApplySourceFile
	Dir         string
	URL         string
	Result      atlasmigrate.ApplyResult
}

// WriteApplyFormat renders Atlas migrate apply format output from the runtime
// result produced by internal/atlasmigrate.
func WriteApplyFormat(w io.Writer, format string, opts ApplyFormatOptions) error {
	report, err := applyReportOptions(opts)
	if err != nil {
		return err
	}
	return atlasreport.WriteMigrateApplyFormat(w, format, report)
}

// WriteApplyText renders the default `migrate apply` report, the one printed
// without --format, from the same runtime result [WriteApplyFormat] reads.
func WriteApplyText(w io.Writer, opts ApplyFormatOptions) error {
	report, err := applyReportOptions(opts)
	if err != nil {
		return err
	}
	return atlasreport.WriteMigrateApplyText(w, report)
}

func applyReportOptions(opts ApplyFormatOptions) (atlasreport.MigrateApplyResultOptions, error) {
	if opts.FS == nil {
		return atlasreport.MigrateApplyResultOptions{}, fmt.Errorf("migrate apply format requires migration filesystem")
	}
	result := opts.Result
	checksNotRun := result.ChecksDeferred
	if result.ChecksSkipped {
		checksNotRun = result.SelectedVersions
	}
	return atlasreport.MigrateApplyResultOptions{
		Conn:             opts.Conn,
		FS:               opts.FS,
		SourceFiles:      opts.SourceFiles,
		Dir:              opts.Dir,
		URL:              opts.URL,
		Status:           result.Status,
		Migrations:       result.Migrations,
		SelectedVersions: result.SelectedVersions,
		SelectedKeys:     result.SelectedKeys,
		CurrentVersion:   result.CurrentVersion,
		CurrentKey:       result.CurrentKey,
		CurrentKeySet:    result.CurrentKeySet,
		ErrorText:        result.ErrorText,
		ApplyError:       result.ApplyError,
		Applied:          result.Executed,
		ChecksNotRun:     checksNotRun,
		StartedAt:        result.StartedAt,
		EndedAt:          result.EndedAt,
	}, nil
}
