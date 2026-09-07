// Package webartifact writes a schema ERD a person can look at, and offers to
// open it.
//
// It is the shared half of `--web` on the Atlas-compatible surface and of
// `--open` on the native one, so the two cannot disagree about what the
// artifact is or where it goes. The document itself is
// [ptah.run/internal/schemadoc]'s, which is also what `ptah schema document`
// writes: one renderer, so a schema looks the same however a reader arrived at
// it.
//
// # Nothing leaves the machine
//
// The document is self-contained by construction -- schemadoc fetches no
// stylesheet, font, script or image -- and this package only writes it to a
// local file and hands that path to the desktop's opener. A schema is the shape
// of somebody's data, so the flag that shows it must not be the flag that
// publishes it. That is a deliberate divergence from a plausible reading of
// Atlas's "open the schema ERD in the browser", and the help text says so
// rather than leaving a reader to assume.
package webartifact

import (
	"context"
	"fmt"
	"io"
	"os"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/fileopen"
	"ptah.run/internal/schemadoc"
)

// Options are what the artifact says about itself and what the caller already
// decided.
type Options struct {
	// Title heads the document.
	Title string
	// Source names where the schema came from, for the line under the title.
	// It is a name and not a path; see [schemadoc.Options.Source].
	Source string
	// Changes marks tables with what a comparison found. Empty renders an
	// unmarked document.
	Changes map[string]schemadoc.ChangeKind
	// Skip suppresses opening. The caller resolves
	// [ptah.run/internal/fileopen.SkipEnvVar] before doing any work, so a
	// malformed value is refused at the start of the command.
	Skip bool
}

// Result is where the artifact went and what happened to it.
type Result struct {
	Path   string
	Opened bool
	// Reason names why nothing opened. Empty exactly when Opened is true.
	Reason string
}

// WriteBytes puts an already-rendered document where a browser can open it,
// and returns the path.
//
// It exists for the native export, whose document is written by the export
// pipeline and may have gone to standard output -- which is nothing a browser
// can be pointed at. Both callers land in the same directory under the same
// name pattern, so "where did the ERD go" has one answer.
func WriteBytes(data []byte) (string, error) {
	file, err := os.CreateTemp("", "ptah-schema-*.html")
	if err != nil {
		return "", fmt.Errorf("create the schema document: %w", err)
	}
	path := file.Name()
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return "", fmt.Errorf("write %s: %w", path, err)
	}
	// Closed before the caller opens it: a reader on Windows cannot open a file
	// this process still holds.
	if err := file.Close(); err != nil {
		return "", fmt.Errorf("write %s: %w", path, err)
	}
	return path, nil
}

// Write renders the schema, writes it, and offers it to the desktop.
//
// A failure to open is not a failure: the artifact is the deliverable and it is
// on disk before anything is opened, so a headless run, a machine with no
// opener and an operator who asked for neither all exit successfully with a
// path. See [ptah.run/internal/fileopen] for why that is structural.
func Write(ctx context.Context, db *schemamodel.Database, opts Options) (Result, []string, error) {
	rendered, err := schemadoc.Render(db, schemadoc.Options{
		Title:   opts.Title,
		Source:  opts.Source,
		Changes: opts.Changes,
	})
	if err != nil {
		return Result{}, nil, err
	}
	path, err := WriteBytes(rendered.Data)
	if err != nil {
		return Result{}, nil, err
	}

	opened := fileopen.Open(ctx, path, fileopen.Options{Skip: opts.Skip})
	return Result{Path: path, Opened: opened.Opened, Reason: opened.Reason}, rendered.Diagnostics, nil
}

// ReportArtifact names where the document went.
//
// The path goes to the diagnostics stream rather than to standard output,
// because on `schema inspect` standard output is the inspected schema and a
// pipeline reads it. A run that opened a browser still prints the path: the
// window may be on another desktop, and the path is what a reviewer attaches.
//
// It is separate from [ReportOpen] because a caller that already named the file
// owes the operator the second sentence and not the first. `ptah schema export
// --out doc.html` is that caller: it has printed where the document is, and
// printing it again in another vocabulary reads as a second file.
func ReportArtifact(diagnostics io.Writer, path string) {
	fmt.Fprintf(diagnostics, "Schema document written to %s\n", path)
}

// ReportOpen says why nothing opened, and says nothing when something did.
//
// Silence on success is deliberate: a browser that came up is its own report,
// and a line saying so would be the only output of a run that did what was
// asked.
func ReportOpen(diagnostics io.Writer, result Result) {
	if result.Opened {
		return
	}
	fmt.Fprintf(diagnostics, "not opened: %s\n", result.Reason)
}
