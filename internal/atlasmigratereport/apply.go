package atlasmigratereport

import (
	"io"
	"os"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/atlasmigrate"
	"github.com/stokaro/ptah/internal/atlasreport"
)

type ApplyFormatOptions struct {
	Conn        *dbschema.DatabaseConnection
	ResolvedDir string
	Dir         string
	URL         string
	Result      atlasmigrate.ApplyResult
}

// WriteApplyFormat renders Atlas migrate apply format output from the runtime
// result produced by internal/atlasmigrate.
func WriteApplyFormat(w io.Writer, format string, opts ApplyFormatOptions) error {
	result := opts.Result
	return atlasreport.WriteMigrateApplyFormat(w, format, atlasreport.MigrateApplyResultOptions{
		Conn:             opts.Conn,
		FS:               os.DirFS(opts.ResolvedDir),
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
