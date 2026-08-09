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
	// Schema carries the dev-database state the analyzed versions start from
	// and end at, rendered as HCL. The zero value renders as two empty strings,
	// which is what a run that never reached a dev database has to report.
	Schema MigrateLintSchema
}

type MigrateLintIntegrity struct {
	Checked bool
	Error   string
}

type MigrateLint struct {
	Env atlasEnv `json:"Env"`
	// Schema is the before/after HCL of the dev database the analyzed versions
	// were replayed on. It is a value and not a pointer so that a template
	// naming `.Schema.Current` evaluates on every run: the pinned community
	// binary v1.3.0 populates the field whenever it reaches a dev database --
	// measured non-empty even on an empty migration directory -- and a nil
	// pointer here would fail template execution and exit 1 where that binary
	// exits 0, which is the whole subject of stokaro/ptah#1241.
	Schema MigrateLintSchema `json:"Schema"`
	Steps  []MigrateLintStep `json:"Steps,omitempty"`
	Files  []MigrateLintFile `json:"Files,omitempty"`
}

// MigrateLintSchema is the dev-database state a lint run analyzed across:
// Current is the schema the first analyzed version starts from, Desired the
// schema left after the last one was replayed. Both are HCL.
type MigrateLintSchema struct {
	Current string `json:"Current"`
	Desired string `json:"Desired"`
}

type MigrateLintStep struct {
	Name   string           `json:"Name,omitempty"`
	Text   string           `json:"Text,omitempty"`
	Error  string           `json:"Error,omitempty"`
	Result *MigrateLintFile `json:"Result,omitempty"`
}

type MigrateLintFile struct {
	Name string `json:"Name,omitempty"`
	Text string `json:"Text,omitempty"`
	// Reports is the analyzer-grouped view of this file's diagnostics, the
	// shape the pinned community binary v1.3.0 exposes to a `--format`
	// template. Findings below is Ptah's own richer per-finding record and is
	// kept alongside it: compatibility adds the documented model, it does not
	// remove a capability (AGENTS.md).
	Reports    []MigrateLintReport     `json:"Reports,omitempty"`
	Error      string                  `json:"Error,omitempty"`
	Findings   []migrationlint.Finding `json:"Findings,omitempty"`
	sourcePath string
}

// MigrateLintReport groups one file's diagnostics by the analyzer that raised
// them. Text is the analyzer's headline, the same sentence the text report
// prints above the group.
type MigrateLintReport struct {
	Text        string                  `json:"Text,omitempty"`
	Diagnostics []MigrateLintDiagnostic `json:"Diagnostics,omitempty"`
}

// MigrateLintDiagnostic is one diagnostic inside a report.
//
// Pos is a byte offset into the file's Text, not a line number: that is what
// the model being matched carries, and a template that slices Text with it has
// to get an offset. It is derived from the finding's line as the offset of that
// line's first byte, because Ptah's analyzers report a line. Measured against
// the pinned community binary v1.3.0 on a `DROP TABLE` migration, where both
// tools report the same line, the two offsets agree.
type MigrateLintDiagnostic struct {
	Pos            int                       `json:"Pos"`
	Text           string                    `json:"Text"`
	Code           string                    `json:"Code"`
	SuggestedFixes []MigrateLintSuggestedFix `json:"SuggestedFixes,omitempty"`
}

// MigrateLintSuggestedFix is one remediation a diagnostic offers.
type MigrateLintSuggestedFix struct {
	Message string `json:"Message"`
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
		Schema: opts.Schema,
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
	owned := make([][]migrationlint.Finding, len(files))
	for _, finding := range findings {
		for i := range files {
			if sameMigrateLintFile(files[i].sourcePath, finding.File) {
				files[i].Findings = append(files[i].Findings, atlasMigrateLintFinding(finding))
				owned[i] = append(owned[i], finding)
			}
		}
	}
	for i := range files {
		files[i].Reports = migrateLintReports(files[i].Text, owned[i])
	}
	return files
}

// migrateLintReports renders one file's findings as the analyzer-grouped report
// model a `--format` template reads.
//
// The grouping, the analyzer ordering and the per-diagnostic wording are the
// ones the text report already uses -- [compatibilityDiagnostic] and
// [groupDiagnostics] -- so the two renderings of a run cannot disagree about
// which analyzer raised what. A finding whose Atlas wording has not been
// measured keeps Ptah's own sentence, exactly as the text report keeps it.
func migrateLintReports(text string, findings []migrationlint.Finding) []MigrateLintReport {
	if len(findings) == 0 {
		return nil
	}
	diags := make([]migrateLintTextDiag, 0, len(findings))
	for _, finding := range findings {
		diags = append(diags, compatibilityDiagnostic(finding))
	}
	offsets := lineByteOffsets(text)
	groups := groupDiagnostics(diags)
	reports := make([]MigrateLintReport, 0, len(groups))
	for _, group := range groups {
		report := MigrateLintReport{Text: group.label + " detected"}
		for _, diag := range group.diags {
			report.Diagnostics = append(report.Diagnostics, MigrateLintDiagnostic{
				Pos:            lineByteOffset(offsets, diag.line),
				Text:           diag.message,
				Code:           diag.code,
				SuggestedFixes: migrateLintSuggestedFixes(diag.fix),
			})
		}
		reports = append(reports, report)
	}
	return reports
}

func migrateLintSuggestedFixes(fix migrateLintTextFix) []MigrateLintSuggestedFix {
	if fix.text == "" {
		return nil
	}
	return []MigrateLintSuggestedFix{{Message: fix.text}}
}

// lineByteOffsets returns the byte offset each 1-based line of text starts at,
// indexed from zero. A file with no trailing newline still contributes its last
// line.
func lineByteOffsets(text string) []int {
	offsets := []int{0}
	for i := 0; i < len(text); i++ {
		if text[i] == '\n' {
			offsets = append(offsets, i+1)
		}
	}
	return offsets
}

// lineByteOffset resolves a 1-based line number to a byte offset. A line the
// file does not have -- a finding whose line is zero, or one past the end after
// a converted layout rewrote the body -- reports offset 0 rather than an
// invented position inside the text.
func lineByteOffset(offsets []int, line int) int {
	if line < 1 || line > len(offsets) {
		return 0
	}
	return offsets[line-1]
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
