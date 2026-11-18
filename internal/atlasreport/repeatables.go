package atlasreport

import (
	"fmt"
	"io"
	"slices"
	"strings"

	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// The repeatable migrations of an Atlas directory are a Ptah capability the
// community binary has no reading for: it runs R__<name>.sql once, as the
// version R. A report shows them only when the run read the directory with
// them, and then after everything the community binary's report holds, so a
// pipeline that parses the Atlas-shaped part reads the same bytes. Each
// document carries them under `Repeatables`, a key the community binary never
// writes, left out when there is none.

// atlasRepeatableLines are the states a report line gives a repeatable
// migration a run selected.
const (
	atlasRepeatableApplied    = "applied"
	atlasRepeatableNotApplied = "not applied"
	atlasRepeatableWouldRun   = "would run"
)

// MigrateApplyRepeatable is one repeatable migration a `migrate apply` run
// selected, as the `--format` document carries it.
type MigrateApplyRepeatable struct {
	// Name is the file, as the directory spells it.
	Name string `json:"Name"`
	// Identity is the part of the name between R__ and .sql, the key the
	// record keeps it under.
	Identity string `json:"Identity"`
	// Reason is why the run selected it: new, changed or retry.
	Reason string `json:"Reason"`
	// Checksum is the digest of the content the run executes, and
	// AppliedChecksum the one recorded as applied before the run, empty when
	// none was.
	Checksum        string `json:"Checksum"`
	AppliedChecksum string `json:"AppliedChecksum,omitempty"`
	// Applied reports that the content the run executed is recorded as
	// applied after it.
	Applied bool `json:"Applied"`
}

// MigrateStatusRepeatable is one repeatable migration of the directory, or one
// record no file carries, as `migrate status` reports it.
type MigrateStatusRepeatable struct {
	// Name is the file, as the directory spells it, empty for an orphaned
	// record.
	Name     string `json:"Name,omitempty"`
	Identity string `json:"Identity"`
	// State is pending, changed, applied, failed, running or orphaned.
	State string `json:"State"`
	// Checksum is the digest of the file as it is now, empty for an orphaned
	// record, and AppliedChecksum the one recorded as applied.
	Checksum        string `json:"Checksum,omitempty"`
	AppliedChecksum string `json:"AppliedChecksum,omitempty"`
	Error           string `json:"Error,omitempty"`
}

// migrateApplyRepeatables is the `Repeatables` list of an apply document.
func migrateApplyRepeatables(work []migrator.RepeatableWork, applied []string) []MigrateApplyRepeatable {
	if len(work) == 0 {
		return nil
	}
	out := make([]MigrateApplyRepeatable, 0, len(work))
	for _, item := range work {
		out = append(out, MigrateApplyRepeatable{
			Name:            item.Path,
			Identity:        item.Identity,
			Reason:          item.Reason,
			Checksum:        item.Checksum,
			AppliedChecksum: item.AppliedChecksum,
			Applied:         slices.Contains(applied, item.Identity),
		})
	}
	return out
}

// writeMigrateApplyRepeatablesText writes the repeatable block after the
// Atlas-shaped report: a blank line, a count, and one line per selected
// repeatable migration with why it ran and what came of it.
func writeMigrateApplyRepeatablesText(w io.Writer, repeatables []MigrateApplyRepeatable, dryRun bool) error {
	if len(repeatables) == 0 {
		return nil
	}
	var b strings.Builder
	fmt.Fprintf(&b, "\nRepeatable Migrations: %d\n", len(repeatables))
	for _, item := range repeatables {
		state := atlasRepeatableNotApplied
		switch {
		case dryRun:
			state = atlasRepeatableWouldRun
		case item.Applied:
			state = atlasRepeatableApplied
		}
		fmt.Fprintf(&b, "  %s %s %s %s\n", item.Identity, item.Reason, item.Checksum, state)
	}
	_, err := io.WriteString(w, b.String())
	return err
}

// migrateStatusRepeatables is the `Repeatables` list of a status document.
func migrateStatusRepeatables(records []migrator.RepeatableRecord) []MigrateStatusRepeatable {
	if len(records) == 0 {
		return nil
	}
	out := make([]MigrateStatusRepeatable, 0, len(records))
	for _, record := range records {
		out = append(out, MigrateStatusRepeatable{
			Name:            record.Path,
			Identity:        record.Identity,
			State:           record.State,
			Checksum:        record.Checksum,
			AppliedChecksum: record.AppliedChecksum,
			Error:           record.Error,
		})
	}
	return out
}

// writeMigrateStatusRepeatablesText writes the repeatable block after the
// Atlas-shaped status: the files of the directory first, then the records no
// file carries, as native `ptah migrations status` lists them.
func writeMigrateStatusRepeatablesText(w io.Writer, repeatables []MigrateStatusRepeatable) {
	var files, orphans []MigrateStatusRepeatable
	for _, item := range repeatables {
		if item.Name == "" {
			orphans = append(orphans, item)
			continue
		}
		files = append(files, item)
	}
	if len(files) > 0 {
		fmt.Fprintf(w, "\nRepeatable Migrations: %d\n", len(files))
		for _, item := range files {
			fmt.Fprintf(w, "  %s %s %s\n", item.Identity, item.State, item.Checksum)
		}
	}
	if len(orphans) > 0 {
		fmt.Fprintf(w, "\nRepeatable migration records with no file in the directory: %d\n", len(orphans))
		for _, item := range orphans {
			fmt.Fprintf(w, "  %s %s %s\n", item.Identity, item.State, item.AppliedChecksum)
		}
	}
}

// atlasFiles reads the versioned files of the directory, leaving out the
// repeatable migrations when the report reads the directory with them.
func (opts MigrateStatusOptions) atlasFiles() ([]migrationfile.File, error) {
	if !opts.Repeatables {
		return migrationfile.Discover(opts.FS, migrationfile.DirFormatAtlas)
	}
	files, _, err := migrationfile.DiscoverAtlasRepeatables(opts.FS)
	return files, err
}
