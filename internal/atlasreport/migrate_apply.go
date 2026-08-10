package atlasreport

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"regexp"
	"strconv"
	"strings"
	"text/template"
	"time"

	"go.5x5.cz/ptah/core/sqlutil"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

var atlasMigrateApplyFailedVersionRe = regexp.MustCompile(`failed to apply migration ([0-9]+)`)

type MigrateApplyResultOptions struct {
	Conn             *dbschema.DatabaseConnection
	FS               fs.FS
	Dir              string
	URL              string
	Status           *migrator.MigrationStatus
	Migrations       []*migrator.Migration
	SelectedVersions []int64
	SelectedKeys     []string
	CurrentVersion   int64
	CurrentKey       string
	ErrorText        string
	ApplyError       error
	Applied          bool
	StartedAt        time.Time
	EndedAt          time.Time
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

type atlasMigrateApplyFile struct {
	Name        string `json:"Name,omitempty"`
	Version     string `json:"Version,omitempty"`
	Description string `json:"Description,omitempty"`
}

type atlasMigrateApplyAppliedFile struct {
	atlasMigrateApplyFile
	Start   time.Time
	End     time.Time
	Skipped int
	Applied []string
	Checks  []*atlasMigrateApplyFileChecks
	Error   *atlasMigrateApplyStatementError
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
	return renderAtlasGoTemplate(w, "atlas-migrate-apply-format", format, result)
}

func validateMigrateApplyResultOptions(opts MigrateApplyResultOptions) error {
	if opts.Conn == nil {
		return errors.New("migrate apply format requires database connection")
	}
	if opts.FS == nil {
		return errors.New("migrate apply format requires migration filesystem")
	}
	if opts.Status == nil && opts.CurrentVersion <= 0 && opts.CurrentKey == "" {
		return errors.New("migrate apply format requires migration status or current version")
	}
	return nil
}

func buildAtlasMigrateApplyResult(opts MigrateApplyResultOptions) (atlasMigrateApplyResult, error) {
	filesByVersion, err := atlasMigrateApplyFilesByVersion(opts.FS)
	if err != nil {
		return atlasMigrateApplyResult{}, err
	}
	migrationsByVersion := atlasMigrateApplyMigrationsByVersion(opts.Migrations)
	env := atlasMigrateApplyEnv{
		Driver: opts.Conn.Info().Dialect,
		URL:    atlasRedactedURL(opts.URL),
		Dir:    opts.Dir,
	}
	result := atlasMigrateApplyResult{
		atlasMigrateApplyEnv: env,
		Env:                  env,
		Pending:              atlasMigrateApplyPendingFiles(filesByVersion, opts.SelectedVersions, opts.SelectedKeys),
		Current:              atlasMigrateApplyCurrentVersion(opts),
		Target:               atlasMigrateApplyTargetVersion(atlasMigrateApplyCurrentVersion(opts), opts.SelectedVersions, opts.SelectedKeys),
		Start:                opts.StartedAt,
		End:                  opts.EndedAt,
		Error:                opts.ErrorText,
	}
	if opts.Applied {
		result.Applied = atlasMigrateApplyAppliedFiles(
			filesByVersion,
			migrationsByVersion,
			opts.SelectedVersions,
			opts.SelectedKeys,
			opts.Conn.Info().Dialect,
			opts.ApplyError,
			opts.StartedAt,
			opts.EndedAt,
		)
	}
	return result, nil
}

func atlasMigrateApplyFilesByVersion(fsys fs.FS) (map[string]atlasMigrateApplyFile, error) {
	discovered, err := migrator.DiscoverMigrationFiles(fsys, migrator.MigrationDirFormatAtlas)
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
	files map[string]atlasMigrateApplyFile,
	versions []int64,
	keys []string,
) []atlasMigrateApplyFile {
	pending := make([]atlasMigrateApplyFile, 0, len(versions))
	for index := range versions {
		if file, ok := files[atlasMigrateApplyVersionKeyAt(versions, keys, index)]; ok {
			pending = append(pending, file)
		}
	}
	return pending
}

func atlasMigrateApplyAppliedFiles(
	files map[string]atlasMigrateApplyFile,
	migrations map[string]*migrator.Migration,
	versions []int64,
	keys []string,
	dialect string,
	applyErr error,
	startedAt time.Time,
	endedAt time.Time,
) []*atlasMigrateApplyAppliedFile {
	applied := make([]*atlasMigrateApplyAppliedFile, 0, len(versions))
	failedVersion := atlasMigrateApplyFailedVersion(applyErr, versions, keys)
	for index := range versions {
		version := atlasMigrateApplyVersionKeyAt(versions, keys, index)
		file, ok := files[version]
		if !ok {
			continue
		}
		appliedFile := &atlasMigrateApplyAppliedFile{
			atlasMigrateApplyFile: file,
			Start:                 startedAt,
			End:                   endedAt,
		}
		if migration := migrations[version]; migration != nil {
			appliedFile.Applied = atlasMigrateApplySplitStatements(migration.UpSQL, dialect)
			if version == failedVersion {
				execErr := atlasMigrateApplyExecutionError(applyErr)
				if execErr == nil {
					execErr = &migrator.MigrationExecutionError{
						Err:       applyErr,
						Statement: "",
					}
				}
				appliedFile.Applied = appliedStatementsBeforeError(appliedFile.Applied, execErr.StatementIndex)
				appliedFile.Error = &atlasMigrateApplyStatementError{
					Stmt: execErr.Statement,
					Text: execErr.Unwrap().Error(),
				}
				applied = append(applied, appliedFile)
				break
			}
		}
		applied = append(applied, appliedFile)
	}
	return applied
}

func atlasMigrateApplyFailedVersion(err error, versions []int64, keys []string) string {
	if err == nil || len(versions) == 0 {
		return ""
	}
	matches := atlasMigrateApplyFailedVersionRe.FindStringSubmatch(err.Error())
	if len(matches) == 2 {
		version, parseErr := strconv.ParseInt(matches[1], 10, 64)
		if parseErr == nil {
			return strconv.FormatInt(version, 10)
		}
	}
	return atlasMigrateApplyVersionKeyAt(versions, keys, len(versions)-1)
}

func atlasMigrateApplyExecutionError(err error) *migrator.MigrationExecutionError {
	var execErr *migrator.MigrationExecutionError
	if errors.As(err, &execErr) {
		return execErr
	}
	return nil
}

func appliedStatementsBeforeError(statements []string, failedIndex int) []string {
	appliedCount := failedIndex - 1
	if appliedCount <= 0 {
		return nil
	}
	if appliedCount > len(statements) {
		appliedCount = len(statements)
	}
	return statements[:appliedCount]
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
	if opts.CurrentKey != "" {
		return opts.CurrentKey
	}
	if opts.CurrentVersion > 0 {
		return atlasMigrateApplyVersionString(opts.CurrentVersion)
	}
	if opts.Status.CurrentVersionKey != "" {
		return opts.Status.CurrentVersionKey
	}
	return atlasMigrateApplyVersionString(opts.Status.CurrentVersion)
}

func atlasMigrateApplyTargetVersion(current string, selectedVersions []int64, selectedKeys []string) string {
	if len(selectedVersions) == 0 {
		return current
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
	if index < len(keys) && keys[index] != "" {
		return keys[index]
	}
	return atlasMigrateApplyVersionString(versions[index])
}

func renderAtlasGoTemplate(w io.Writer, name, format string, data any) error {
	tmpl, err := newAtlasGoTemplate(name, format)
	if err != nil {
		return err
	}
	var out bytes.Buffer
	if err := tmpl.Execute(&out, data); err != nil {
		return fmt.Errorf("execute --format template: %w", err)
	}
	_, err = w.Write(out.Bytes())
	return err
}

func ValidateMigrateApplyTemplate(format string) error {
	return validateAtlasGoTemplate("atlas-migrate-apply-format", format)
}

func validateAtlasGoTemplate(name, format string) error {
	_, err := newAtlasGoTemplate(name, format)
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

func newAtlasGoTemplate(name, format string) (*template.Template, error) {
	tmpl, err := template.New(name).Funcs(atlasTemplateFuncs()).Parse(format)
	if err != nil {
		return nil, fmt.Errorf("parse --format template: %w", err)
	}
	return tmpl, nil
}

func (r atlasMigrateApplyResult) MarshalJSON() ([]byte, error) {
	type alias atlasMigrateApplyResult
	value := struct {
		alias
		Message string `json:"Message,omitempty"`
	}{
		alias: alias(r),
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

func (f *atlasMigrateApplyAppliedFile) MarshalJSON() ([]byte, error) {
	type appliedFile struct {
		Name        string                           `json:"Name,omitempty"`
		Version     string                           `json:"Version,omitempty"`
		Description string                           `json:"Description,omitempty"`
		Start       time.Time                        `json:"Start"`
		End         time.Time                        `json:"End"`
		Skipped     int                              `json:"Skipped"`
		Applied     []string                         `json:"Applied"`
		Checks      []*atlasMigrateApplyFileChecks   `json:"Checks"`
		Error       *atlasMigrateApplyStatementError `json:"Error"`
	}
	return json.Marshal(appliedFile{
		Name:        f.Name,
		Version:     f.Version,
		Description: f.Description,
		Start:       f.Start,
		End:         f.End,
		Skipped:     f.Skipped,
		Applied:     f.Applied,
		Checks:      f.Checks,
		Error:       f.Error,
	})
}

func atlasTemplateIndentLines(input string, indent int) string {
	pad := strings.Repeat(" ", indent)
	return strings.ReplaceAll(input, "\n", "\n"+pad)
}

func atlasTemplateIdentity(value string) string {
	return value
}

func atlasTemplateJSON(value any, args ...string) (string, error) {
	var (
		data []byte
		err  error
	)
	switch len(args) {
	case 0:
		data, err = json.Marshal(value)
	case 1:
		data, err = json.MarshalIndent(value, "", args[0])
	default:
		data, err = json.MarshalIndent(value, args[0], args[1])
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
