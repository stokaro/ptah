package migrationlintreport

import (
	"cmp"
	"context"
	"io"
	"strconv"
	"strings"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/schemaselection"
	"go.5x5.cz/ptah/migration/lint"
	"go.5x5.cz/ptah/migration/migrator"
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
	// baseVersionKey is the first analyzed migration's revision-table token.
	// Current is read immediately before it runs.
	baseVersionKey string
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
	capture.baseVersionKey, capture.hasBase = firstAnalyzedVersionKey(analysis)
	if scope := schemaselection.FromURL(devURL).Scope; scope != "" {
		capture.schemas = []string{scope}
	}
	return capture
}

// firstAnalyzedVersionKey returns the first key among the up-migration files
// the analysis selected. The predicate is the one
// [go.5x5.cz/ptah/internal/atlasreport] uses to decide which files the report
// lists, so the schema pair spans exactly the versions the report names.
func firstAnalyzedVersionKey(analysis lint.Analysis) (string, bool) {
	files := analysis.Files()
	maxVersion := lintMaxVersion(files)
	var first lint.File
	found := false
	for _, file := range files {
		if file.Direction != "up" || file.Ignored || !file.Selected {
			continue
		}
		if !found || lintFileOrder(file, first, maxVersion) < 0 {
			first = file
			found = true
		}
	}
	if !found {
		return "", false
	}
	return lintRevisionVersionKey(first), true
}

// observeMigration records the state the first analyzed migration starts from.
func (c *schemaCapture) observeMigration(
	ctx context.Context,
	migration *migrator.Migration,
	conn *dbschema.DatabaseConnection,
) error {
	if c == nil || !c.hasBase || migration == nil || migration.RevisionVersion() != c.baseVersionKey {
		return nil
	}
	rendered, err := c.render(ctx, conn)
	if err != nil {
		return err
	}
	c.current = rendered
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
	return atlasschema.Inspect(ctx, conn, atlasschema.InspectOptions{
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

func lintFileOrder(a, b lint.File, maxVersion int64) int {
	return cmp.Or(
		cmp.Compare(lintVersionRank(a, maxVersion), lintVersionRank(b, maxVersion)),
		strings.Compare(a.Name, b.Name),
	)
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

func lintMaxVersion(files []lint.File) int64 {
	var maxVersion int64
	for _, file := range files {
		if file.Version > maxVersion {
			maxVersion = file.Version
		}
	}
	return maxVersion
}

func lintVersionRank(file lint.File, maxVersion int64) int64 {
	if file.Repeatable && file.Version == 0 {
		return maxVersion + 1
	}
	return file.Version
}
