package migrationlintreport

import (
	"context"
	"io"
	"strconv"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/schemaselection"
	"ptah.run/migration/lint"
	"ptah.run/migration/migrator"
)

// schemaCapture reads the dev database twice during one replay: once standing
// on the state the first analyzed version starts from, and once after the last
// migration has run. Rendered as HCL, the pair is the before/after schema a
// `migrate lint --format` template reads as `.Schema.Current` and
// `.Schema.Desired`.
//
// Both reads ride the replay that already happens, so nothing is applied twice
// and no second dev database is materialized. The capture is opt-in
// ([Options.CaptureSchema]) because only the templated output has a reader for
// it; the text report never asks, and paying two introspections for output
// nobody prints would be a cost with no answer attached.
type schemaCapture struct {
	devURL  string
	runtime Runtime
	// selected holds the revision-table token of every analyzed up migration.
	// Current is read immediately before the first of them runs, in the order
	// the replay runs them, which is the order the report lists them in.
	selected map[string]struct{}
	// observed reports that Current has been read.
	observed bool
	// hasBase is false when the run analyzes no version at all -- an empty
	// directory, or one where the selector matched nothing. Current is then the
	// same state as Desired, which is what the pinned community binary v1.3.0
	// reports for an empty directory: both halves render the bare schema.
	hasBase bool
	schemas []string

	current string
	desired string
}

func newSchemaCapture(analysis lint.Analysis, devURL string, runtime Runtime) *schemaCapture {
	capture := &schemaCapture{devURL: devURL, runtime: runtime}
	capture.selected = analyzedVersionKeys(analysis)
	capture.hasBase = len(capture.selected) > 0
	if scope := schemaselection.FromURL(devURL).Scope; scope != "" {
		capture.schemas = []string{scope}
	}
	return capture
}

// analyzedVersionKeys returns the keys of the up-migration files the analysis
// selected. The predicate is the one [ptah.run/internal/atlasreport] uses to
// decide which files the report lists, so the schema pair spans exactly the
// versions the report names. The first of them is the one the replay reaches
// first: an Atlas directory runs and is listed by file name, so of 9_a.sql and
// 11_c.sql the replay reaches 11_c.sql first, after 10_b.sql, and a first file
// chosen by number would read Current after 11_c.sql had run.
func analyzedVersionKeys(analysis lint.Analysis) map[string]struct{} {
	keys := make(map[string]struct{})
	for _, file := range analysis.Files() {
		if file.Direction != "up" || file.Ignored || !file.Selected {
			continue
		}
		keys[lintRevisionVersionKey(file)] = struct{}{}
	}
	return keys
}

// observeMigration records the state the first analyzed migration starts from.
func (c *schemaCapture) observeMigration(
	ctx context.Context,
	migration *migrator.Migration,
	conn *dbschema.DatabaseConnection,
) error {
	if c == nil || c.observed || migration == nil {
		return nil
	}
	if _, ok := c.selected[migration.RevisionVersion()]; !ok {
		return nil
	}
	rendered, err := c.render(ctx, conn)
	if err != nil {
		return err
	}
	c.current = rendered
	c.observed = true
	return nil
}

// observeReplayed records the state the directory leaves behind.
func (c *schemaCapture) observeReplayed(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
) error {
	if c == nil {
		return nil
	}
	rendered, err := c.render(ctx, conn)
	if err != nil {
		return err
	}
	c.desired = rendered
	if !c.hasBase {
		c.current = rendered
	}
	return nil
}

func (c *schemaCapture) render(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
) (string, error) {
	// Only the rendering. This capture compares two HCL documents, so the
	// model beside it answers a question nothing here asks.
	result, err := atlasschema.Inspect(ctx, conn, atlasschema.InspectOptions{
		Runtime: c.runtime,
		DevURL:  c.devURL,
		Schemas: c.schemas,
		Format:  "hcl",
		// The rendering is a value inside a report, not a document the operator
		// asked to read, so its advisory diagnostics have no place to go.
		Diagnostics: io.Discard,
		// This surface stands in for the community binary, whose own lint
		// report renders HCL it can itself parse.
		OmitAtlasRefusedBlocks: true,
	})
	if err != nil {
		return "", err
	}
	return result.Rendered, nil
}

func lintRevisionVersionKey(file lint.File) string {
	if file.RevisionVersion != "" {
		return file.RevisionVersion
	}
	if file.Repeatable {
		if file.Version > 0 {
			return strconv.FormatInt(file.Version, 10) + "R"
		}
		return "R"
	}
	return strconv.FormatInt(file.Version, 10)
}
