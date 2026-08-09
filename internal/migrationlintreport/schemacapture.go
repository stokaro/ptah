package migrationlintreport

import (
	"context"
	"io"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/schemaselection"
	"go.5x5.cz/ptah/migration/lint"
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
	devURL string
	// baseVersion is the first analyzed version. Current is read immediately
	// before it runs.
	baseVersion int64
	// hasBase is false when the run analyzes no version at all -- an empty
	// directory, or one where the selector matched nothing. Current is then the
	// same state as Desired, which is what the pinned community binary v1.3.0
	// reports for an empty directory: both halves render the bare schema.
	hasBase bool
	schemas []string

	current string
	desired string
}

func newSchemaCapture(analysis lint.Analysis, devURL string) *schemaCapture {
	capture := &schemaCapture{devURL: devURL}
	capture.baseVersion, capture.hasBase = firstAnalyzedVersion(analysis)
	if scope := schemaselection.FromURL(devURL).Scope; scope != "" {
		capture.schemas = []string{scope}
	}
	return capture
}

// firstAnalyzedVersion returns the lowest version among the up-migration files
// the analysis selected. The predicate is the one
// [go.5x5.cz/ptah/internal/atlasreport] uses to decide which files the report
// lists, so the schema pair spans exactly the versions the report names.
func firstAnalyzedVersion(analysis lint.Analysis) (int64, bool) {
	var lowest int64
	found := false
	for _, file := range analysis.Files() {
		if file.Repeatable || file.Direction != "up" || file.Ignored || !file.Selected {
			continue
		}
		if !found || file.Version < lowest {
			lowest = file.Version
			found = true
		}
	}
	return lowest, found
}

// observeVersion records the state the first analyzed version starts from.
func (c *schemaCapture) observeVersion(
	_ context.Context,
	version int64,
	conn *dbschema.DatabaseConnection,
) error {
	if c == nil || !c.hasBase || version != c.baseVersion {
		return nil
	}
	rendered, err := c.render(conn)
	if err != nil {
		return err
	}
	c.current = rendered
	return nil
}

// observeReplayed records the state the directory leaves behind.
func (c *schemaCapture) observeReplayed(conn *dbschema.DatabaseConnection) error {
	if c == nil {
		return nil
	}
	rendered, err := c.render(conn)
	if err != nil {
		return err
	}
	c.desired = rendered
	if !c.hasBase {
		c.current = rendered
	}
	return nil
}

func (c *schemaCapture) render(conn *dbschema.DatabaseConnection) (string, error) {
	return atlasschema.Inspect(conn, atlasschema.InspectOptions{
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
}
