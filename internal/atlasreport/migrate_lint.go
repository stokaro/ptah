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

	"ptah.run/internal/atlaslint"
	"ptah.run/internal/migratesum"
	migrationlint "ptah.run/migration/lint"
)

type MigrateLintOptions struct {
	Driver    string
	URL       string
	Dir       string
	Analysis  *migrationlint.Analysis
	Integrity MigrateLintIntegrity
	Error     string
	// ReplayFailure is the file a failed dev-database replay stopped at, nil
	// when the replay did not stop inside a migration. When set, the report
	// carries that one file and the failed replay step in place of the
	// analysis, and Error is not read.
	ReplayFailure *MigrateLintReplayFailure
	// RevisionVersions maps converted execution-order keys to exact source
	// identities for the version labels in the text report.
	RevisionVersions map[int64]string
	// FileNames maps converted execution-order keys to the name the file has in
	// the operator's directory, for a layout whose conversion renames its files.
	// A Flyway `V1.5__b.sql` is analyzed as a numeric Atlas file, and a template
	// that prints `.Name` has to get the file the repository holds. A key the
	// map lacks keeps the analyzed name.
	FileNames map[int64]string
	// Schema carries the dev-database state the analyzed versions start from
	// and end at, rendered as HCL. The report carries it only when the replay
	// finished: a run that stopped at an integrity failure or inside the replay
	// reports an empty schema whatever this holds.
	Schema MigrateLintSchema
}

type MigrateLintIntegrity struct {
	Checked bool
	Error   string
}

type MigrateLint struct {
	// URL is the address of a report published elsewhere. Ptah publishes no
	// report, so it is always empty. It exists because the pinned community
	// binary v1.3.0 hands a template the same empty field, and a template that
	// names `.URL` must evaluate rather than fail the run.
	URL string   `json:"URL,omitempty"`
	Env atlasEnv `json:"Env"`
	// Schema is the before/after HCL of the dev database the analyzed versions
	// were replayed on. It is a value and not a pointer so that a template
	// naming `.Schema.Current` evaluates on every run: the pinned community
	// binary v1.3.0 populates the field whenever a replay finishes -- measured
	// non-empty even on an empty migration directory -- and a nil pointer here
	// would fail template execution and exit 1 where that binary exits 0, which
	// is the whole subject of stokaro/ptah#1241.
	Schema MigrateLintSchema `json:"Schema"`
	Steps  []MigrateLintStep `json:"Steps,omitempty"`
	Files  []MigrateLintFile `json:"Files,omitempty"`
}

// MigrateLintSchema is the dev-database state a lint run analyzed across:
// Current is the schema the first analyzed version starts from, Desired the
// schema left after the last one was replayed. Both are HCL without Ptah's
// generated-code marker, framed the way `schema inspect` frames its document
// on this surface. Both are empty when the replay did not finish, and JSON
// leaves an empty field out: the pinned community binary v1.3.0 prints `{}`
// for `{{ json .Schema }}` after an atlas.sum failure and after a replay that
// failed.
type MigrateLintSchema struct {
	Current string `json:"Current,omitempty"`
	Desired string `json:"Desired,omitempty"`
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
	Reports []MigrateLintReport `json:"Reports,omitempty"`
	// Error says why the file failed: the replay error a failed replay stopped
	// at, or the headline of every report that holds an error-severity
	// diagnostic, joined with "; ". The second is how the pinned community
	// binary v1.3.0 words an analyzer error: `destructive changes detected` for
	// a DROP TABLE.
	Error      string                  `json:"Error,omitempty"`
	Findings   []migrationlint.Finding `json:"Findings,omitempty"`
	sourcePath string
	// changes is the number of schema changes the analysis found in the file,
	// counted per statement as the text report's summary counts them.
	changes int
	// checksum marks the atlas.sum entry of an integrity failure, which is not
	// a migration file.
	checksum bool
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
	// TextEdit is the change the fix makes to the file. Every fix Ptah suggests
	// is advice and makes none, so it is nil. The field is part of the model a
	// template reads: `{{ .SuggestedFixes }}` prints it as `<nil>` on the pinned
	// community binary v1.3.0, and a template that names `.TextEdit` evaluates.
	TextEdit *MigrateLintTextEdit `json:"TextEdit,omitempty"`
}

// MigrateLintTextEdit is the change a suggested fix makes to a file: lines Line
// to End, replaced with NewText. No fix Ptah suggests carries one; see
// [MigrateLintSuggestedFix.TextEdit].
type MigrateLintTextEdit struct {
	Line    int    `json:"Line"`
	End     int    `json:"End"`
	NewText string `json:"NewText"`
}

// TotalFiles reports how many migration files the report covers: the analyzed
// files, or the one file a failed replay stopped at. The atlas.sum entry of an
// integrity failure is not a migration file and is not counted. Measured on the
// pinned community binary v1.3.0, it prints 0 after an integrity failure and 1
// after a failed replay.
func (r MigrateLint) TotalFiles() int {
	total := 0
	for _, file := range r.Files {
		if !file.checksum {
			total++
		}
	}
	return total
}

// TotalChanges reports how many schema changes the analyzed files carry,
// counted per statement. It is the count the text report's summary prints, so
// the two renderings of a run agree. A file a failed replay stopped at counts
// none.
func (r MigrateLint) TotalChanges() int {
	total := 0
	for _, file := range r.Files {
		total += file.changes
	}
	return total
}

// DiagnosticsCount reports how many diagnostics the reports of every file hold.
func (r MigrateLint) DiagnosticsCount() int {
	total := 0
	for _, file := range r.Files {
		for _, report := range file.Reports {
			total += len(report.Diagnostics)
		}
	}
	return total
}

// Line reports the 1-based line of Text that the byte offset pos falls on,
// which is how a template turns a diagnostic's Pos into a line number:
// `{{ $file.Line .Pos }}`. An offset of len(Text) is the end of the text and
// is accepted. An offset outside Text is an error, so the template fails and
// the run exits 1, as on the pinned community binary v1.3.0. That binary words
// the failure as the Go runtime's slice-bounds panic; this one names the
// offset and the file.
func (f MigrateLintFile) Line(pos int) (int, error) {
	if pos < 0 || pos > len(f.Text) {
		return 0, fmt.Errorf("position %d is outside the %d bytes of %s", pos, len(f.Text), f.Name)
	}
	return strings.Count(f.Text[:pos], "\n") + 1, nil
}

// SuggestedFixes returns every fix the file's diagnostics suggest, in report
// and diagnostic order, or nil when none does. A nil result prints as `[]` and
// encodes as `null`, which is what the pinned community binary v1.3.0 prints
// for a file with no fix.
func (f MigrateLintFile) SuggestedFixes() []MigrateLintSuggestedFix {
	var fixes []MigrateLintSuggestedFix
	for _, report := range f.Reports {
		for _, diagnostic := range report.Diagnostics {
			fixes = append(fixes, diagnostic.SuggestedFixes...)
		}
	}
	return fixes
}

func WriteMigrateLintFormat(w io.Writer, format string, opts MigrateLintOptions) error {
	result, err := NewMigrateLint(opts)
	if err != nil {
		return err
	}
	return renderAtlasGoTemplate(w, atlasMigrateVerbTemplateWording, format, result)
}

func ValidateMigrateLintTemplate(format string) error {
	return validateAtlasGoTemplate(atlasMigrateVerbTemplateWording, format)
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
		result.Steps = migrateLintSteps(nil, 0, opts.Integrity, "")
		result.Files = []MigrateLintFile{
			{
				Name:     migratesum.AtlasFileName,
				Error:    opts.Integrity.Error,
				checksum: true,
			},
		}
		return result, nil
	}

	if opts.Analysis == nil {
		return MigrateLint{}, fmt.Errorf("migration lint analysis is required")
	}
	if failure := opts.ReplayFailure; failure != nil {
		selected, total := migrateLintFiles(*opts.Analysis, opts.FileNames)
		result.Steps = migrateLintSteps(selected, total, opts.Integrity, failure.Error)
		result.Files = []MigrateLintFile{{Name: failure.reportedName(opts.FileNames), Error: failure.Error}}
		return result, nil
	}
	files, total := migrateLintFiles(*opts.Analysis, opts.FileNames)
	files = attachMigrateLintFindings(files, opts.Analysis.Findings())
	result.Steps = migrateLintSteps(files, total, opts.Integrity, opts.Error)
	result.Files = files
	if opts.Error == "" {
		result.Schema = MigrateLintSchema{
			Current: frameCompatibilityHCL(opts.Schema.Current),
			Desired: frameCompatibilityHCL(opts.Schema.Desired),
		}
	}
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

// migrateLintFiles returns the selected up-migration files and the count of
// new up-migration files considered. names renames a file whose layout was
// converted; see [MigrateLintOptions.FileNames].
func migrateLintFiles(
	analysis migrationlint.Analysis,
	names map[int64]string,
) (files []MigrateLintFile, total int) {
	prepared := analysis.Files()
	maxVersion := migrateLintMaxVersion(prepared)
	slices.SortStableFunc(prepared, func(a, b migrationlint.File) int {
		return migrateLintFileOrder(a, b, maxVersion)
	})
	files = make([]MigrateLintFile, 0, len(prepared))
	for _, file := range prepared {
		if file.Direction != "up" || file.Ignored {
			continue
		}
		total++
		if !file.Selected {
			continue
		}
		files = append(files, MigrateLintFile{
			Name:       migrateLintFileName(file, names),
			Text:       file.Source,
			sourcePath: file.Path,
			changes:    len(file.Changes),
		})
	}
	return files, total
}

// migrateLintFileName is the name a report gives an analyzed file: the name it
// has in the operator's directory when its layout was converted, the analyzed
// name otherwise.
func migrateLintFileName(file migrationlint.File, names map[int64]string) string {
	if name, ok := names[file.Version]; ok {
		return name
	}
	return file.Name
}

func migrateLintVersionKey(file migrationlint.File, revisionVersions map[int64]string) string {
	if mapped, ok := revisionVersions[file.Version]; ok {
		return mapped
	}
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

func migrateLintFileOrder(a, b migrationlint.File, maxVersion int64) int {
	return cmp.Or(
		cmp.Compare(migrateLintVersionRank(a, maxVersion), migrateLintVersionRank(b, maxVersion)),
		strings.Compare(a.Name, b.Name),
	)
}

func migrateLintMaxVersion(files []migrationlint.File) int64 {
	var maxVersion int64
	for _, file := range files {
		if file.Version > maxVersion {
			maxVersion = file.Version
		}
	}
	return maxVersion
}

func migrateLintVersionRank(file migrationlint.File, maxVersion int64) int64 {
	if file.Repeatable && file.Version == 0 {
		return maxVersion + 1
	}
	return file.Version
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
		files[i].Error = migrateLintAnalyzerError(owned[i])
	}
	return files
}

// migrateLintAnalyzerError words the analyzer errors of one file: the headline
// of every report group that holds an error-severity finding, in report order,
// joined with "; ". It is empty when every finding is a warning.
//
// The headline is the analyzer's error because that is how the pinned community
// binary v1.3.0 reports one. An analyzer configured to fail returns its report
// text as the error, the runner joins the errors of one file with "; ", and a
// template reads them in `.Files[].Error` and in the Result of the file's step.
// Measured on a `DROP TABLE` migration, both read `destructive changes
// detected` and the run exits 1.
func migrateLintAnalyzerError(findings []migrationlint.Finding) string {
	diags := make([]migrateLintTextDiag, 0, len(findings))
	for _, finding := range findings {
		diags = append(diags, compatibilityDiagnostic(finding))
	}
	var errs []string
	for _, group := range groupDiagnostics(diags) {
		if slices.ContainsFunc(group.diags, func(diag migrateLintTextDiag) bool {
			return diag.severity == migrationlint.SeverityError
		}) {
			errs = append(errs, group.label+" detected")
		}
	}
	return strings.Join(errs, "; ")
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
	// The community binary counts the files it replayed here, not their
	// schema changes: a version that creates two tables loads one.
	steps = append(steps, MigrateLintStep{
		Name: "Replay Migration Files",
		Text: fmt.Sprintf("Loaded %d changes on dev database", len(files)),
	})
	for _, file := range files {
		// The community binary counts the reports here, one per analyzer that
		// raised anything, not the diagnostics inside them.
		steps = append(steps, MigrateLintStep{
			Name:   "Analyze " + file.Name,
			Text:   strconv.Itoa(len(file.Reports)) + " reports were found in analysis",
			Result: &file,
		})
	}
	return steps
}
