package atlas

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/stokaro/ptah/core/sqlutil"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
)

var atlasMigrateApplyFailedVersionRe = regexp.MustCompile(`failed to apply migration ([0-9]+)`)

type atlasMigrateApplyResultOptions struct {
	conn             *dbschema.DatabaseConnection
	fsys             fs.FS
	dir              string
	url              string
	status           *migrator.MigrationStatus
	migrations       []*migrator.Migration
	selectedVersions []int64
	currentVersion   int64
	errorText        string
	applyError       error
	applied          bool
	startedAt        time.Time
	endedAt          time.Time
}

type atlasMigrateApplyEnv struct {
	Driver string `json:"Driver,omitempty"`
	URL    string `json:"URL,omitempty"`
	Dir    string `json:"Dir,omitempty"`
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

func writeAtlasMigrateApplyFormat(w io.Writer, format string, opts atlasMigrateApplyResultOptions) error {
	result, err := buildAtlasMigrateApplyResult(opts)
	if err != nil {
		return err
	}
	return renderAtlasGoTemplate(w, "atlas-migrate-apply-format", format, result)
}

func buildAtlasMigrateApplyResult(opts atlasMigrateApplyResultOptions) (atlasMigrateApplyResult, error) {
	filesByVersion, err := atlasMigrateApplyFilesByVersion(opts.fsys)
	if err != nil {
		return atlasMigrateApplyResult{}, err
	}
	migrationsByVersion := atlasMigrateApplyMigrationsByVersion(opts.migrations)
	env := atlasMigrateApplyEnv{
		Driver: opts.conn.Info().Dialect,
		URL:    atlasMigrateApplyRedactedURL(opts.url),
		Dir:    opts.dir,
	}
	result := atlasMigrateApplyResult{
		atlasMigrateApplyEnv: env,
		Env:                  env,
		Pending:              atlasMigrateApplyPendingFiles(filesByVersion, opts.selectedVersions),
		Current:              atlasMigrateApplyVersionString(atlasMigrateApplyCurrentVersion(opts)),
		Target:               atlasMigrateApplyTargetVersion(atlasMigrateApplyCurrentVersion(opts), opts.selectedVersions),
		Start:                opts.startedAt,
		End:                  opts.endedAt,
		Error:                opts.errorText,
	}
	if opts.applied {
		result.Applied = atlasMigrateApplyAppliedFiles(
			filesByVersion,
			migrationsByVersion,
			opts.selectedVersions,
			opts.conn.Info().Dialect,
			opts.applyError,
			opts.startedAt,
			opts.endedAt,
		)
	}
	return result, nil
}

func atlasMigrateApplyFilesByVersion(fsys fs.FS) (map[int64]atlasMigrateApplyFile, error) {
	discovered, err := migrator.DiscoverMigrationFiles(fsys, migrator.MigrationDirFormatAtlas)
	if err != nil {
		return nil, fmt.Errorf("discover Atlas migration files: %w", err)
	}
	files := make(map[int64]atlasMigrateApplyFile, len(discovered))
	for _, file := range discovered {
		if file.Repeatable || file.Direction != "up" {
			continue
		}
		files[file.Version] = atlasMigrateApplyFile{
			Name:        file.Path,
			Version:     atlasMigrateApplyVersionString(file.Version),
			Description: file.Name,
		}
	}
	return files, nil
}

func atlasMigrateApplyMigrationsByVersion(input []*migrator.Migration) map[int64]*migrator.Migration {
	migrations := make(map[int64]*migrator.Migration, len(input))
	for _, migration := range input {
		if migration == nil {
			continue
		}
		migrations[migration.Version] = migration
	}
	return migrations
}

func atlasMigrateApplyPendingFiles(
	files map[int64]atlasMigrateApplyFile,
	versions []int64,
) []atlasMigrateApplyFile {
	pending := make([]atlasMigrateApplyFile, 0, len(versions))
	for _, version := range versions {
		if file, ok := files[version]; ok {
			pending = append(pending, file)
		}
	}
	return pending
}

func atlasMigrateApplyAppliedFiles(
	files map[int64]atlasMigrateApplyFile,
	migrations map[int64]*migrator.Migration,
	versions []int64,
	dialect string,
	applyErr error,
	startedAt time.Time,
	endedAt time.Time,
) []*atlasMigrateApplyAppliedFile {
	applied := make([]*atlasMigrateApplyAppliedFile, 0, len(versions))
	failedVersion := atlasMigrateApplyFailedVersion(applyErr, versions)
	for _, version := range versions {
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

func atlasMigrateApplyFailedVersion(err error, versions []int64) int64 {
	if err == nil || len(versions) == 0 {
		return 0
	}
	matches := atlasMigrateApplyFailedVersionRe.FindStringSubmatch(err.Error())
	if len(matches) == 2 {
		version, parseErr := strconv.ParseInt(matches[1], 10, 64)
		if parseErr == nil {
			return version
		}
	}
	return versions[len(versions)-1]
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
		stmt = strings.TrimSpace(sqlutil.StripComments(stmt))
		if stmt != "" {
			filtered = append(filtered, stmt)
		}
	}
	return filtered
}

func atlasMigrateApplyCurrentVersion(opts atlasMigrateApplyResultOptions) int64 {
	if opts.currentVersion > 0 {
		return opts.currentVersion
	}
	return opts.status.CurrentVersion
}

func atlasMigrateApplyTargetVersion(current int64, selectedVersions []int64) string {
	if len(selectedVersions) == 0 {
		return atlasMigrateApplyVersionString(current)
	}
	return atlasMigrateApplyVersionString(selectedVersions[len(selectedVersions)-1])
}

func atlasMigrateApplyVersionString(version int64) string {
	if version <= 0 {
		return ""
	}
	return strconv.FormatInt(version, 10)
}

func atlasMigrateApplyRedactedURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	if parsed.User != nil {
		username := parsed.User.Username()
		if username != "" {
			parsed.User = url.User(username)
		} else {
			parsed.User = nil
		}
	}
	query := parsed.Query()
	for key := range query {
		if isAtlasMigrateApplySensitiveQueryKey(key) {
			query.Set(key, "xxxxx")
		}
	}
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func isAtlasMigrateApplySensitiveQueryKey(key string) bool {
	switch strings.ToLower(key) {
	case "password", "passwd", "pass", "pwd", "token", "secret":
		return true
	default:
		return false
	}
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

func validateAtlasGoTemplate(name, format string) error {
	_, err := newAtlasGoTemplate(name, format)
	return err
}

func newAtlasGoTemplate(name, format string) (*template.Template, error) {
	tmpl, err := template.New(name).Funcs(template.FuncMap{
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
	}).Parse(format)
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
	case len(r.Applied) == 0:
		value.Message = "No migration files to execute"
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
		Start       time.Time                        `json:"Start,omitzero"`
		End         time.Time                        `json:"End,omitzero"`
		Skipped     int                              `json:"Skipped,omitempty"`
		Applied     []string                         `json:"Applied,omitempty"`
		Error       *atlasMigrateApplyStatementError `json:"Error,omitempty"`
	}
	return json.Marshal(appliedFile{
		Name:        f.Name,
		Version:     f.Version,
		Description: f.Description,
		Start:       f.Start,
		End:         f.End,
		Skipped:     f.Skipped,
		Applied:     f.Applied,
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
