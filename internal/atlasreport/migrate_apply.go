package atlasreport

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"slices"
	"strconv"
	"strings"
	"text/template"
	"time"

	"ptah.run/core/sqlutil"
	"ptah.run/dbschema"
	"ptah.run/internal/sourcestatement"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

type MigrateApplyResultOptions struct {
	Conn *dbschema.DatabaseConnection
	FS   fs.FS
	// SourceFiles maps converted execution-order keys to the file each one
	// came from, for a layout whose conversion renames its files. A Flyway
	// `V1.5__b.sql` executes as a numeric Atlas file, and the report names it
	// `V1.5__b.sql` with the description Flyway gives it, as the community
	// binary does. A key the map lacks keeps the converted file's name and the
	// description read from it.
	SourceFiles      map[int64]MigrateApplySourceFile
	Dir              string
	URL              string
	Status           *migrator.MigrationStatus
	Migrations       []*migrator.Migration
	SelectedVersions []int64
	SelectedKeys     []string
	CurrentVersion   int64
	CurrentKey       string
	CurrentKeySet    bool
	ErrorText        string
	ApplyError       error
	StartedAt        time.Time
	EndedAt          time.Time
	// Applied reports that the run reached the selected migrations: it
	// executed them, or a dry run went through them without writing. The
	// report lists them under `.Applied` only then, and a dry run lists every
	// statement, as the community binary does.
	Applied bool
	// ChecksNotRun is the versions whose pre-migration checks the run did not
	// evaluate: a dry run defers them, and PTAH_SKIP_CHECKS bypasses them.
	// Their checks are left out of the report, which would otherwise show
	// them as passed.
	ChecksNotRun []int64
}

// MigrateApplySourceFile is a migration file as the operator's directory holds
// it.
type MigrateApplySourceFile struct {
	// Name is the file's path relative to the migration directory, such as
	// `V1.5__b.sql` or `sub/V2__c.sql`.
	Name string
	// Description is the description the directory's layout gives the file.
	Description string
}

type atlasMigrateApplyEnv struct {
	Driver string           `json:"Driver,omitempty"`
	URL    atlasTemplateURL `json:"URL,omitzero"`
	Dir    string           `json:"Dir,omitempty"`
}

type atlasMigrateApplyResult struct {
	atlasMigrateApplyEnv
	Env     atlasMigrateApplyEnv            `json:"-"`
	Pending []atlasMigrateApplyFile         `json:"Pending,omitempty"`
	Applied []*atlasMigrateApplyAppliedFile `json:"Applied,omitempty"`
	Current string                          `json:"Current,omitempty"`
	Target  string                          `json:"Target,omitempty"`
	Start   time.Time
	End     time.Time
	Error   string `json:"Error,omitempty"`
}

// atlasMigrateApplyFile is a migration file a report names. A Flyway
// repeatable has no version, and its entry leaves Version out, as the
// community binary's does.
type atlasMigrateApplyFile struct {
	Name        string `json:"Name,omitempty"`
	Version     string `json:"Version,omitempty"`
	Description string `json:"Description,omitempty"`
}

// atlasMigrateApplyAppliedFile is one entry of `.Applied`. An entry leaves out
// a count, a list and an error it has none of, as the community binary's
// entries do.
type atlasMigrateApplyAppliedFile struct {
	atlasMigrateApplyFile
	Start   time.Time
	End     time.Time
	Skipped int                              `json:"Skipped,omitempty"`
	Applied []string                         `json:"Applied,omitempty"`
	Checks  []*atlasMigrateApplyFileChecks   `json:"Checks,omitempty"`
	Error   *atlasMigrateApplyStatementError `json:"Error,omitempty"`
	// errorListed reports that Applied ends with the statement Error names,
	// because the statement reached the database before it failed.
	errorListed bool
}

type atlasMigrateApplyFileChecks struct {
	Name  string                           `json:"Name,omitempty"`
	Stmts []*atlasMigrateApplyCheck        `json:"Stmts,omitempty"`
	Error *atlasMigrateApplyStatementError `json:"Error,omitempty"`
	Start time.Time                        `json:"Start,omitzero"`
	End   time.Time                        `json:"End,omitzero"`
}

type atlasMigrateApplyCheck struct {
	Stmt  string  `json:"Stmt,omitempty"`
	Error *string `json:"Error,omitempty"`
}

type atlasMigrateApplyStatementError struct {
	Stmt string `json:"Stmt,omitempty"`
	Text string `json:"Text,omitempty"`
}

func WriteMigrateApplyFormat(w io.Writer, format string, opts MigrateApplyResultOptions) error {
	if err := validateMigrateApplyResultOptions(opts); err != nil {
		return err
	}
	result, err := buildAtlasMigrateApplyResult(opts)
	if err != nil {
		return err
	}
	return renderAtlasGoTemplate(w, atlasMigrateApplyTemplateWording, format, result)
}

func validateMigrateApplyResultOptions(opts MigrateApplyResultOptions) error {
	if opts.Conn == nil {
		return errors.New("migrate apply format requires database connection")
	}
	if opts.FS == nil {
		return errors.New("migrate apply format requires migration filesystem")
	}
	if opts.Status == nil && opts.CurrentVersion <= 0 && !opts.CurrentKeySet {
		return errors.New("migrate apply format requires migration status or current version")
	}
	return nil
}

func buildAtlasMigrateApplyResult(opts MigrateApplyResultOptions) (atlasMigrateApplyResult, error) {
	filesByVersion, err := atlasMigrateApplyFilesByVersion(opts.FS)
	if err != nil {
		return atlasMigrateApplyResult{}, err
	}
	files := atlasMigrateApplyFiles{byVersion: filesByVersion, sources: opts.SourceFiles}
	migrationsByVersion := atlasMigrateApplyMigrationsByVersion(opts.Migrations)
	env := atlasMigrateApplyEnv{
		Driver: opts.Conn.Info().Dialect,
		URL:    atlasRedactedURL(opts.URL),
		Dir:    opts.Dir,
	}
	result := atlasMigrateApplyResult{
		atlasMigrateApplyEnv: env,
		Env:                  env,
		Pending:              atlasMigrateApplyPendingFiles(files, opts.SelectedVersions, opts.SelectedKeys),
		Current:              atlasMigrateApplyCurrentVersion(opts),
		Target:               atlasMigrateApplyTargetVersion(opts.SelectedVersions, opts.SelectedKeys),
		Start:                opts.StartedAt,
		End:                  opts.EndedAt,
		Error:                opts.ErrorText,
	}
	if opts.Applied {
		result.Applied = atlasMigrateApplyAppliedFiles(files, migrationsByVersion, opts)
	}
	return result, nil
}

func atlasMigrateApplyFilesByVersion(fsys fs.FS) (map[string]atlasMigrateApplyFile, error) {
	discovered, err := migrationfile.Discover(fsys, migrationfile.DirFormatAtlas)
	if err != nil {
		return nil, fmt.Errorf("discover Atlas migration files: %w", err)
	}
	files := make(map[string]atlasMigrateApplyFile, len(discovered))
	for _, file := range discovered {
		if file.Direction != "up" {
			continue
		}
		files[file.RevisionVersion()] = atlasMigrateApplyFile{
			Name:        file.Path,
			Version:     file.RevisionVersion(),
			Description: atlasMigrationFileDescription(file.Path),
		}
	}
	return files, nil
}

func atlasMigrateApplyMigrationsByVersion(input []*migrator.Migration) map[string]*migrator.Migration {
	migrations := make(map[string]*migrator.Migration, len(input))
	for _, migration := range input {
		if migration == nil {
			continue
		}
		migrations[migration.RevisionVersion()] = migration
	}
	return migrations
}

func atlasMigrateApplyPendingFiles(
	files atlasMigrateApplyFiles,
	versions []int64,
	keys []string,
) []atlasMigrateApplyFile {
	pending := make([]atlasMigrateApplyFile, 0, len(versions))
	for index := range versions {
		if file, ok := atlasMigrateApplyFileForVersion(files, versions, keys, index); ok {
			pending = append(pending, file)
		}
	}
	return pending
}

// atlasMigrateApplyAppliedFiles is the `.Applied` list of a run that reached
// its migrations: each selected file up to the one a failure stopped at.
//
// A failure Ptah words is read with the identities of the run, so it names a
// migration as the rest of the report names it. Without that, a Flyway
// `V1.5__b.sql` refused by a check is reported under the numeric key its
// conversion executes it as, while the run's own `.Error` names `1.5`.
func atlasMigrateApplyAppliedFiles(
	files atlasMigrateApplyFiles,
	migrations map[string]*migrator.Migration,
	opts MigrateApplyResultOptions,
) []*atlasMigrateApplyAppliedFile {
	versions, keys := opts.SelectedVersions, opts.SelectedKeys
	dialect := opts.Conn.Info().Dialect
	identities := MigrateApplyRevisionIdentities(versions, keys, opts.Migrations)
	applied := make([]*atlasMigrateApplyAppliedFile, 0, len(versions))
	failedVersion, failedVersionSet := atlasMigrateApplyFailedVersion(opts.ApplyError, versions, keys)
	for index := range versions {
		version := atlasMigrateApplyVersionKeyAt(versions, keys, index)
		file, ok := atlasMigrateApplyFileForVersion(files, versions, keys, index)
		if !ok {
			continue
		}
		appliedFile := &atlasMigrateApplyAppliedFile{
			atlasMigrateApplyFile: file,
			Start:                 opts.StartedAt,
			End:                   opts.EndedAt,
		}
		migration := migrations[version]
		if migration == nil {
			applied = append(applied, appliedFile)
			continue
		}
		appliedFile.Applied = atlasMigrateApplyWrittenStatements(migration.UpSQL, dialect)
		if !slices.Contains(opts.ChecksNotRun, migration.Version) {
			appliedFile.Checks = atlasMigrateApplyChecks(migration, dialect, opts.ApplyError, identities)
		}
		applied = append(applied, appliedFile)
		if failedVersionSet && version == failedVersion {
			recordAtlasMigrateApplyFailure(appliedFile, migration, dialect, opts.ApplyError, identities)
			break
		}
	}
	return applied
}

// recordAtlasMigrateApplyFailure marks the file a failed run stopped at with
// the failure, and cuts its statements down to the ones the run sent.
func recordAtlasMigrateApplyFailure(
	appliedFile *atlasMigrateApplyAppliedFile,
	migration *migrator.Migration,
	dialect string,
	applyErr error,
	identities map[int64]string,
) {
	execErr := atlasMigrateApplyExecutionError(applyErr)
	if execErr == nil {
		execErr = &migrator.MigrationExecutionError{
			Err:       applyErr,
			Statement: "",
		}
	}
	appliedFile.Error = &atlasMigrateApplyStatementError{
		Stmt: execErr.Statement,
		Text: NameRevisionIdentities(execErr.Unwrap().Error(), identities),
	}
	// A statement the database answered is reported as the file spells it,
	// with the database's answer alone, as the line the run prints names it.
	if executed := NewApplyStatementError(applyErr, []*migrator.Migration{migration}, dialect); executed != nil {
		appliedFile.Error = &atlasMigrateApplyStatementError{
			Stmt: executed.Statement,
			Text: executed.Answer.Error(),
		}
	}
	appliedFile.Applied = atlasMigrateApplySentStatements(appliedFile.Applied, execErr, appliedFile.Error.Stmt)
	appliedFile.errorListed = execErr.Sent()
}

// atlasMigrateApplyFailedVersion is the revision identity of the migration a
// failed run stopped at, and the last selected one when the failure names
// none.
func atlasMigrateApplyFailedVersion(err error, versions []int64, keys []string) (string, bool) {
	if err == nil || len(versions) == 0 {
		return "", false
	}
	if failed, ok := errors.AsType[*migrator.FailedMigrationError](err); ok {
		return failed.VersionKey, true
	}
	return atlasMigrateApplyVersionKeyAt(versions, keys, len(versions)-1), true
}

func atlasMigrateApplyExecutionError(err error) *migrator.MigrationExecutionError {
	if execErr, ok := errors.AsType[*migrator.MigrationExecutionError](err); ok {
		return execErr
	}
	return nil
}

// atlasMigrateApplySentStatements is the part of a failed file's statements
// the run sent: every statement before the failure, and refused, the failed
// statement as the report's error quotes it, when it reached the database. The
// community binary lists a statement as it sends it, so the statement the
// database refused is the file's last entry. It is the error's text rather
// than the entry at its position, so the two agree where the migrator numbered
// the body's statements differently, as a provider's query units do. A
// statement Ptah refused before sending it, and a failure that names none, add
// nothing the database saw.
func atlasMigrateApplySentStatements(statements []string, failure *migrator.MigrationExecutionError, refused string) []string {
	before := min(max(failure.StatementIndex-1, 0), len(statements))
	listed := slices.Clip(statements[:before])
	if failure.Sent() {
		listed = append(listed, refused)
	}
	if len(listed) == 0 {
		return nil
	}
	return listed
}

// atlasMigrateApplyWrittenStatements is each statement of a migration body as
// the file spells it, terminator included where the file wrote one, which is
// how the community binary lists the statements a file executed. A comment
// above a statement is left out and one inside it is kept.
func atlasMigrateApplyWrittenStatements(body, dialect string) []string {
	return sourcestatement.Each(body, dialect, atlasMigrateApplySplitStatements(body, dialect))
}

func atlasMigrateApplySplitStatements(sql, dialect string) []string {
	if strings.TrimSpace(dialect) == "" {
		normalized := sqlutil.NormalizeClientDelimiters(sql)
		return sqlutil.SplitSQLStatements(sqlutil.StripComments(normalized))
	}
	statements := sqlutil.SplitSQLStatementsForDialect(sql, dialect)
	filtered := statements[:0]
	for _, stmt := range statements {
		stmt = strings.TrimSpace(sqlutil.StripCommentsForDialect(stmt, dialect))
		if stmt != "" {
			filtered = append(filtered, stmt)
		}
	}
	return filtered
}

func atlasMigrateApplyCurrentVersion(opts MigrateApplyResultOptions) string {
	if opts.CurrentKeySet {
		return opts.CurrentKey
	}
	if opts.CurrentVersion > 0 {
		return atlasMigrateApplyVersionString(opts.CurrentVersion)
	}
	if opts.Status == nil {
		return ""
	}
	if opts.Status.CurrentVersionKeySet {
		return opts.Status.CurrentVersionKey
	}
	return atlasMigrateApplyVersionString(opts.Status.CurrentVersion)
}

// atlasMigrateApplyFiles is the migration files a report can name: the
// converted files by revision identity, and the source file of each converted
// execution-order key where conversion renamed it.
type atlasMigrateApplyFiles struct {
	byVersion map[string]atlasMigrateApplyFile
	sources   map[int64]MigrateApplySourceFile
}

func atlasMigrateApplyFileForVersion(
	files atlasMigrateApplyFiles,
	versions []int64,
	keys []string,
	index int,
) (atlasMigrateApplyFile, bool) {
	key := atlasMigrateApplyVersionKeyAt(versions, keys, index)
	file, ok := files.byVersion[key]
	if !ok {
		file, ok = files.byVersion[strconv.FormatInt(versions[index], 10)]
	}
	if !ok {
		return file, false
	}
	file.Version = key
	if source, renamed := files.sources[versions[index]]; renamed {
		file.Name = source.Name
		file.Description = source.Description
	}
	return file, true
}

// atlasMigrateApplyTargetVersion is the version the run moves to: the last
// migration it selected, and none when it selected nothing, as the community
// binary reports a run with nothing to execute.
func atlasMigrateApplyTargetVersion(selectedVersions []int64, selectedKeys []string) string {
	if len(selectedVersions) == 0 {
		return ""
	}
	return atlasMigrateApplyVersionKeyAt(selectedVersions, selectedKeys, len(selectedVersions)-1)
}

func atlasMigrateApplyVersionString(version int64) string {
	if version <= 0 {
		return ""
	}
	return strconv.FormatInt(version, 10)
}

func atlasMigrateApplyVersionKeyAt(versions []int64, keys []string, index int) string {
	if index < len(keys) {
		return keys[index]
	}
	return atlasMigrateApplyVersionString(versions[index])
}

func renderAtlasGoTemplate(w io.Writer, wording templateWording, format string, data any) error {
	tmpl, err := newAtlasGoTemplate(wording, format)
	if err != nil {
		return err
	}
	var out bytes.Buffer
	if err := tmpl.Execute(&out, data); err != nil {
		return wording.executeError(err)
	}
	_, err = w.Write(out.Bytes())
	return err
}

func ValidateMigrateApplyTemplate(format string) error {
	return validateAtlasGoTemplate(atlasMigrateApplyTemplateWording, format)
}

func validateAtlasGoTemplate(wording templateWording, format string) error {
	_, err := newAtlasGoTemplate(wording, format)
	return err
}

// atlasTemplateFuncs is the helper set every Atlas Go-template surface in this
// package exposes. It is shared rather than duplicated so a template that works
// on one compat verb works on the next: the drop-in promise is that an Atlas
// pipeline's template keeps running here, and a helper missing on one verb
// breaks that quietly.
func atlasTemplateFuncs() template.FuncMap {
	return template.FuncMap{
		"json":         atlasTemplateJSON,
		"json_merge":   atlasTemplateJSONMerge,
		"add":          func(a, b int) int { return a + b },
		"indent_ln":    atlasTemplateIndentLines,
		"upper":        strings.ToUpper,
		"cyan":         atlasTemplateIdentity,
		"green":        atlasTemplateIdentity,
		"red":          atlasTemplateIdentity,
		"yellow":       atlasTemplateIdentity,
		"redBgWhiteFg": atlasTemplateIdentity,
	}
}

func newAtlasGoTemplate(wording templateWording, format string) (*template.Template, error) {
	tmpl, err := template.New(wording.name).Funcs(atlasTemplateFuncs()).Parse(format)
	if err != nil {
		return nil, wording.parseError(err)
	}
	return tmpl, nil
}

// MarshalJSON writes the report as the community binary's `{{ json . }}`
// does, the run's message included. A version the run has none of, such as
// the current version of a database with nothing applied or the target of a
// run with nothing to execute, is left out, and so is the empty version of a
// Flyway repeatable.
func (r atlasMigrateApplyResult) MarshalJSON() ([]byte, error) {
	value := struct {
		atlasMigrateApplyEnv
		Pending []atlasMigrateApplyFile         `json:"Pending,omitempty"`
		Applied []*atlasMigrateApplyAppliedFile `json:"Applied,omitempty"`
		Current string                          `json:"Current,omitempty"`
		Target  string                          `json:"Target,omitempty"`
		Start   time.Time
		End     time.Time
		Error   string `json:"Error,omitempty"`
		Message string `json:"Message,omitempty"`
	}{
		atlasMigrateApplyEnv: r.atlasMigrateApplyEnv,
		Pending:              r.Pending,
		Applied:              r.Applied,
		Current:              r.Current,
		Target:               r.Target,
		Start:                r.Start,
		End:                  r.End,
		Error:                r.Error,
	}
	switch {
	case r.Error != "":
	case len(r.Applied) == 0 && len(r.Pending) == 0:
		value.Message = "No migration files to execute"
	case len(r.Applied) == 0:
	default:
		value.Message = fmt.Sprintf(
			"Migrated to version %s from %s (%d migrations in total)",
			r.Target,
			r.Current,
			len(r.Pending),
		)
	}
	return json.Marshal(value)
}

func atlasTemplateIndentLines(input string, indent int) string {
	pad := strings.Repeat(" ", indent)
	return strings.ReplaceAll(input, "\n", "\n"+pad)
}

func atlasTemplateIdentity(value string) string {
	return value
}

func atlasTemplateJSON(value any, args ...string) (string, error) {
	return templateJSON(value, args, json.Marshal, json.MarshalIndent)
}

// templateJSON is the argument grammar of the `json` template helper over an
// encoder: no argument writes compact JSON, one is the indent, and two are the
// prefix and the indent.
func templateJSON(value any, args []string, marshal func(any) ([]byte, error), marshalIndent func(any, string, string) ([]byte, error)) (string, error) {
	var (
		data []byte
		err  error
	)
	switch len(args) {
	case 0:
		data, err = marshal(value)
	case 1:
		data, err = marshalIndent(value, "", args[0])
	default:
		data, err = marshalIndent(value, args[0], args[1])
	}
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func atlasTemplateJSONMerge(objects ...string) (string, error) {
	merged := make(map[string]any)
	for _, object := range objects {
		values := make(map[string]any)
		if err := json.Unmarshal([]byte(object), &values); err != nil {
			return "", fmt.Errorf("json_merge: %w", err)
		}
		maps.Copy(merged, values)
	}
	data, err := json.Marshal(merged)
	if err != nil {
		return "", fmt.Errorf("json_merge: %w", err)
	}
	return string(data), nil
}

// atlasMigrateApplyChecks reports the pre-migration checks a migration carries,
// in the order the migrator evaluates them, with the one that refused marked.
//
// Atlas's apply report models this per file, and Ptah's modelled it and left it
// nil: the field was never assigned anywhere, so a pipeline reading the report
// to learn which assertion refused a migration got an empty list and a prose
// sentence in the file-level Error (stokaro/ptah#3118).
//
// The list is what the migration declares rather than a log of what executed,
// because nothing records the latter. That is the same list for a run that
// applied -- every declared check passed -- and for one a check refused, where
// the refusal names the assertion and this marks it. A check after the refused
// one never ran, and saying so would need execution data this does not have.
func atlasMigrateApplyChecks(
	migration *migrator.Migration,
	dialect string,
	applyErr error,
	identities map[int64]string,
) []*atlasMigrateApplyFileChecks {
	groups := make([]*atlasMigrateApplyFileChecks, 0, len(migration.AtlasCheckFiles())+1)
	for _, file := range migration.AtlasCheckFiles() {
		groups = append(groups, &atlasMigrateApplyFileChecks{
			Name:  file.Name,
			Stmts: atlasMigrateApplyCheckStatements(atlasMigrateApplySplitStatements(file.SQL, dialect)),
		})
	}
	if directives, err := migrator.ParseChecks(migration.UpSQL, dialect); err == nil && len(directives) > 0 {
		asserts := make([]string, 0, len(directives))
		for _, directive := range directives {
			asserts = append(asserts, directive.Assert)
		}
		groups = append(groups, &atlasMigrateApplyFileChecks{
			Stmts: atlasMigrateApplyCheckStatements(asserts),
		})
	}
	if len(groups) == 0 {
		return nil
	}
	markAtlasMigrateApplyCheckFailure(groups, migration.Version, applyErr, identities)
	return groups
}

// atlasMigrateApplyCheckStatements wraps assertion SQL in the report's per-check
// shape, where a nil Error is a check that did not refuse.
func atlasMigrateApplyCheckStatements(statements []string) []*atlasMigrateApplyCheck {
	checks := make([]*atlasMigrateApplyCheck, 0, len(statements))
	for _, statement := range statements {
		checks = append(checks, &atlasMigrateApplyCheck{Stmt: statement})
	}
	return checks
}

// markAtlasMigrateApplyCheckFailure attaches a refusal to the check it names.
//
// A failed assertion identifies its group by name and its statement by text, so
// the mark lands on the exact entry. A group refusal -- an Atlas oneof file
// where nothing passed -- names only the file, so it marks the group. Either
// refusal names its migration by the identity identities gives it.
func markAtlasMigrateApplyCheckFailure(
	groups []*atlasMigrateApplyFileChecks,
	version int64,
	applyErr error,
	identities map[int64]string,
) {
	var checkErr *migrator.CheckFailedError
	if errors.As(applyErr, &checkErr) && checkErr.Version == version {
		group := atlasMigrateApplyCheckGroup(groups, checkErr.Name)
		if group == nil {
			return
		}
		text := NameRevisionIdentities(checkErr.Error(), identities)
		group.Error = &atlasMigrateApplyStatementError{Stmt: checkErr.Assert, Text: text}
		for _, check := range group.Stmts {
			if check.Stmt == checkErr.Assert {
				check.Error = &text
			}
		}
		return
	}
	var groupErr *migrator.CheckGroupFailedError
	if errors.As(applyErr, &groupErr) && groupErr.Version == version {
		for _, group := range groups {
			if group.Name == groupErr.Name {
				group.Error = &atlasMigrateApplyStatementError{Text: NameRevisionIdentities(groupErr.Error(), identities)}
				return
			}
		}
	}
}

// atlasMigrateApplyCheckGroup is the group a failed check belongs to. The
// migrator names a txtar check after its file and position, `checks.sql#2`,
// and names a `-- +ptah check` directive as the directive does, so a name
// that no file claims belongs to the unnamed group of directives.
func atlasMigrateApplyCheckGroup(groups []*atlasMigrateApplyFileChecks, name string) *atlasMigrateApplyFileChecks {
	for _, group := range groups {
		if group.Name != "" && (name == group.Name || strings.HasPrefix(name, group.Name+"#")) {
			return group
		}
	}
	for _, group := range groups {
		if group.Name == "" {
			return group
		}
	}
	return nil
}
