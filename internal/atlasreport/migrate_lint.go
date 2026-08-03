package atlasreport

import (
	"cmp"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"slices"
	"strconv"
	"strings"

	"go.5x5.cz/ptah/internal/atlaslint"
	"go.5x5.cz/ptah/internal/migratesum"
	migrationlint "go.5x5.cz/ptah/migration/lint"
)

type MigrateLintOptions struct {
	Driver    string
	URL       string
	Dir       string
	Analysis  *migrationlint.Analysis
	Integrity MigrateLintIntegrity
	Error     string
}

type MigrateLintIntegrity struct {
	Checked bool
	Error   string
}

type MigrateLint struct {
	Env   atlasEnv          `json:"Env"`
	Steps []MigrateLintStep `json:"Steps,omitempty"`
	Files []MigrateLintFile `json:"Files,omitempty"`
}

type MigrateLintStep struct {
	Name   string           `json:"Name,omitempty"`
	Text   string           `json:"Text,omitempty"`
	Error  string           `json:"Error,omitempty"`
	Result *MigrateLintFile `json:"Result,omitempty"`
}

type MigrateLintFile struct {
	Name       string                  `json:"Name,omitempty"`
	Text       string                  `json:"Text,omitempty"`
	Error      string                  `json:"Error,omitempty"`
	Findings   []migrationlint.Finding `json:"Findings,omitempty"`
	sourcePath string
}

func WriteMigrateLintFormat(w io.Writer, format string, opts MigrateLintOptions) error {
	result, err := NewMigrateLint(opts)
	if err != nil {
		return err
	}
	return renderAtlasGoTemplate(w, "atlas-migrate-lint-format", format, result)
}

func ValidateMigrateLintTemplate(format string) error {
	return validateAtlasGoTemplate("atlas-migrate-lint-format", format)
}

func NewMigrateLint(opts MigrateLintOptions) (MigrateLint, error) {
	result := MigrateLint{
		Env: atlasEnv{
			Driver: opts.Driver,
			URL:    atlasRedactedURL(opts.URL),
			Dir:    opts.Dir,
		},
	}
	if opts.Integrity.Failed() {
		result.Steps = migrateLintSteps(nil, 0, 0, opts.Integrity, "")
		result.Files = []MigrateLintFile{
			{
				Name:  migratesum.AtlasFileName,
				Error: opts.Integrity.Error,
			},
		}
		return result, nil
	}

	if opts.Analysis == nil {
		return MigrateLint{}, fmt.Errorf("migration lint analysis is required")
	}
	files, total, changes := migrateLintFiles(*opts.Analysis)
	files = attachMigrateLintFindings(files, opts.Analysis.Findings())
	result.Steps = migrateLintSteps(files, total, changes, opts.Integrity, opts.Error)
	result.Files = files
	return result, nil
}

// InspectMigrateLintIntegrity verifies the atlas.sum fsys carries over exactly
// the file set covered names, in the order given, and reports the outcome as
// report content rather than as a command error — `migrate lint` prints a
// checksum failure inside its analysis report.
//
// The covered set is a parameter and not rediscovered here because it is not a
// property of the filesystem alone: a directory read through `?format=` or
// `--dir-format` covers the file set THAT layout's atlas.sum covers, which
// differs per layout in both membership and order. Measured against the pinned
// community binary v1.3.0 on a golang-migrate directory, editing the covered
// `1_init.up.sql` exits 1 with `L2: 1_init.up.sql was edited` while editing the
// uncovered `1_init.down.sql` exits 0 — a set rediscovered under the Atlas rule
// would cover both and refuse the second (stokaro/ptah#1013 section 1).
// Callers get the set from [atlasmigrateimport.SumFileNames], the same rule
// `migrate hash` writes from and the apply-time gate verifies against.
func InspectMigrateLintIntegrity(fsys fs.FS, covered []string) (MigrateLintIntegrity, error) {
	_, err := fs.Stat(fsys, migratesum.AtlasFileName)
	if errors.Is(err, fs.ErrNotExist) {
		return MigrateLintIntegrity{}, nil
	}
	if err != nil {
		return MigrateLintIntegrity{}, fmt.Errorf("stat %s: %w", migratesum.AtlasFileName, err)
	}
	result, err := migratesum.VerifyAtlasFiles(fsys, covered)
	if errors.Is(err, migratesum.ErrCoveredEntryUnreadable) {
		// A covered entry that is a directory (#991). The directory could not be
		// hashed at all, so calling it a mismatch would name the wrong fault and
		// send the reader looking for an edited migration that does not exist.
		return MigrateLintIntegrity{Checked: true, Error: err.Error()}, nil
	}
	if err != nil {
		return MigrateLintIntegrity{Checked: true, Error: "checksum mismatch"}, nil
	}
	if !result.OK() {
		return MigrateLintIntegrity{Checked: true, Error: "checksum mismatch"}, nil
	}
	return MigrateLintIntegrity{Checked: true}, nil
}

func (i MigrateLintIntegrity) Failed() bool {
	return i.Checked && i.Error != ""
}

// migrateLintFiles returns the selected up-migration files, the count of new
// up-migration files considered, and the total number of semantic schema
// changes those selected files express. The change count is sourced from the
// dialect-aware replay/planning pipeline (see migrationlint.SchemaChange), not
// from re-parsing SQL here, so one statement can contribute zero, one, or
// several changes.
func migrateLintFiles(analysis migrationlint.Analysis) (files []MigrateLintFile, total, changes int) {
	prepared := analysis.Files()
	slices.SortStableFunc(prepared, func(a, b migrationlint.File) int {
		return cmp.Or(cmp.Compare(a.Version, b.Version), strings.Compare(a.Name, b.Name))
	})
	files = make([]MigrateLintFile, 0, len(prepared))
	for _, file := range prepared {
		if file.Repeatable || file.Direction != "up" || file.Ignored {
			continue
		}
		total++
		if !file.Selected {
			continue
		}
		changes += len(file.Changes)
		files = append(files, MigrateLintFile{
			Name:       file.Name,
			Text:       file.Source,
			sourcePath: file.Path,
		})
	}
	return files, total, changes
}

func attachMigrateLintFindings(
	files []MigrateLintFile,
	findings []migrationlint.Finding,
) []MigrateLintFile {
	for _, finding := range findings {
		for i := range files {
			if sameMigrateLintFile(files[i].sourcePath, finding.File) {
				files[i].Findings = append(files[i].Findings, atlasMigrateLintFinding(finding))
			}
		}
	}
	return files
}

func sameMigrateLintFile(sourcePath, findingPath string) bool {
	if findingPath == "" {
		return false
	}
	cleanFinding := strings.TrimPrefix(path.Clean(findingPath), "./")
	return cleanFinding == strings.TrimPrefix(path.Clean(sourcePath), "./")
}

func atlasMigrateLintFinding(finding migrationlint.Finding) migrationlint.Finding {
	finding.Rule = atlaslint.RuleForNativeCode(finding.Rule).Code
	return finding
}

func migrateLintSteps(
	files []MigrateLintFile,
	total int,
	changes int,
	integrity MigrateLintIntegrity,
	errText string,
) []MigrateLintStep {
	steps := make([]MigrateLintStep, 0, len(files)+3)
	if integrity.Checked {
		step := MigrateLintStep{
			Name: "Migration Integrity Check",
			Text: "File atlas.sum is valid",
		}
		if integrity.Failed() {
			step.Text = "File atlas.sum is invalid"
			step.Error = integrity.Error
			return append(steps, step)
		}
		steps = append(steps, step)
	}
	steps = append(steps, MigrateLintStep{
		Name: "Detect New Migration Files",
		Text: fmt.Sprintf("Found %d new migration files (from %d total)", len(files), total),
	})
	if errText != "" {
		return append(steps, MigrateLintStep{
			Name:  "Replay Migration Files",
			Text:  "Failed loading changes on dev database",
			Error: errText,
		})
	}
	steps = append(steps, MigrateLintStep{
		Name: "Replay Migration Files",
		Text: fmt.Sprintf("Loaded %d changes on dev database", changes),
	})
	for _, file := range files {
		steps = append(steps, MigrateLintStep{
			Name:   "Analyze " + file.Name,
			Text:   strconv.Itoa(len(file.Findings)) + " reports were found in analysis",
			Result: &file,
		})
	}
	return steps
}
