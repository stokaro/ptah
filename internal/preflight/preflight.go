// Package preflight runs migration pre-flight hooks before schema changes.
package preflight

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/stokaro/ptah/core/platform"
)

const defaultWebhookTimeout = 30 * time.Second

// Direction identifies the migration direction guarded by pre-flight hooks.
type Direction string

const (
	// DirectionUp runs before applying pending up migrations.
	DirectionUp Direction = "up"
	// DirectionDown runs before rolling back migrations.
	DirectionDown Direction = "down"
)

// Options describes all configured pre-flight hooks for one migration run.
type Options struct {
	Direction          Direction
	DatabaseURL        string
	DisplayDatabaseURL string
	Dialect            string
	CurrentVersion     int64
	TargetVersion      int64
	Command            string
	PostgresDumpDir    string
	MySQLDumpDir       string
	WebhookURL         string
}

// Enabled reports whether any hook is configured.
func (o Options) Enabled() bool {
	return o.Command != "" || o.PostgresDumpDir != "" || o.MySQLDumpDir != "" || o.WebhookURL != ""
}

// Result describes one successful hook execution.
type Result struct {
	Name     string
	Artifact string
}

// CommandRunner runs an external command and returns its combined output.
type CommandRunner interface {
	Run(ctx context.Context, name string, args []string, env []string) (string, error)
}

// ExecCommandRunner runs commands with os/exec.
type ExecCommandRunner struct{}

// Run executes one command with the supplied extra environment.
func (ExecCommandRunner) Run(ctx context.Context, name string, args []string, env []string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// Runner executes configured pre-flight hooks.
type Runner struct {
	CommandRunner CommandRunner
	HTTPClient    *http.Client
	Now           func() time.Time
	Stdout        io.Writer
}

// Execute runs every configured hook in deterministic order.
func (r Runner) Execute(ctx context.Context, opts Options) ([]Result, error) {
	if !opts.Enabled() {
		return nil, nil
	}
	runner := r.withDefaults()

	env := opts.env()
	results := make([]Result, 0, 4)
	if opts.Command != "" {
		result, err := runner.runCommandHook(ctx, opts, env)
		if err != nil {
			return results, err
		}
		results = append(results, result)
	}
	if opts.PostgresDumpDir != "" {
		result, err := runner.runPostgresDump(ctx, opts)
		if err != nil {
			return results, err
		}
		results = append(results, result)
	}
	if opts.MySQLDumpDir != "" {
		result, err := runner.runMySQLDump(ctx, opts)
		if err != nil {
			return results, err
		}
		results = append(results, result)
	}
	if opts.WebhookURL != "" {
		result, err := runner.runWebhook(ctx, opts)
		if err != nil {
			return results, err
		}
		results = append(results, result)
	}
	return results, nil
}

func (r Runner) withDefaults() Runner {
	if r.CommandRunner == nil {
		r.CommandRunner = ExecCommandRunner{}
	}
	if r.HTTPClient == nil {
		r.HTTPClient = defaultHTTPClient()
	}
	if r.Now == nil {
		r.Now = time.Now
	}
	if r.Stdout == nil {
		r.Stdout = io.Discard
	}
	return r
}

func defaultHTTPClient() *http.Client {
	return &http.Client{
		Timeout: defaultWebhookTimeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func (o Options) env() []string {
	return []string{
		"PTAH_DB_URL=" + o.DatabaseURL,
		"PTAH_DIALECT=" + o.Dialect,
		fmt.Sprintf("PTAH_CURRENT_VERSION=%d", o.CurrentVersion),
		fmt.Sprintf("PTAH_TARGET_VERSION=%d", o.TargetVersion),
	}
}

func (r Runner) runCommandHook(ctx context.Context, opts Options, env []string) (Result, error) {
	name, args := shellCommand(opts.Command)
	output, err := r.CommandRunner.Run(ctx, name, args, env)
	output = sanitizeHookOutput(output, opts)
	if err != nil {
		return Result{}, hookError(opts.Direction, "custom command", err, output)
	}
	r.writeHookOutput("custom command", output)
	return Result{Name: "custom command"}, nil
}

func (r Runner) runPostgresDump(ctx context.Context, opts Options) (Result, error) {
	if !isPostgresDumpDialect(opts.Dialect) {
		return Result{}, fmt.Errorf("%s pre-flight pg_dump hook requires a PostgreSQL-compatible dialect, got %s", opts.Direction, opts.Dialect)
	}
	path, err := r.dumpPath(opts.PostgresDumpDir, ".dump", opts)
	if err != nil {
		return Result{}, err
	}

	args, extraEnv := postgresDumpCommand(opts.DatabaseURL, path)
	output, err := r.CommandRunner.Run(ctx, "pg_dump", args, extraEnv)
	output = sanitizeHookOutput(output, opts)
	if err != nil {
		return Result{}, hookError(opts.Direction, "pg_dump", err, output)
	}
	r.writeHookOutput("pg_dump", output)
	return Result{Name: "pg_dump", Artifact: path}, nil
}

func (r Runner) runMySQLDump(ctx context.Context, opts Options) (Result, error) {
	if !isMySQLDumpDialect(opts.Dialect) {
		return Result{}, fmt.Errorf("%s pre-flight mysqldump hook requires MySQL or MariaDB dialect, got %s", opts.Direction, opts.Dialect)
	}
	path, err := r.dumpPath(opts.MySQLDumpDir, ".sql", opts)
	if err != nil {
		return Result{}, err
	}

	args, extraEnv, err := mysqlDumpCommand(opts.DatabaseURL, path)
	if err != nil {
		return Result{}, err
	}
	output, err := r.CommandRunner.Run(ctx, "mysqldump", args, extraEnv)
	output = sanitizeHookOutput(output, opts)
	if err != nil {
		return Result{}, hookError(opts.Direction, "mysqldump", err, output)
	}
	r.writeHookOutput("mysqldump", output)
	return Result{Name: "mysqldump", Artifact: path}, nil
}

func (r Runner) dumpPath(dir, ext string, opts Options) (string, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("%s pre-flight backup directory %s: %w", opts.Direction, dir, err)
	}
	name := fmt.Sprintf(
		"ptah_pre_v%d_to_v%d_%s%s",
		opts.CurrentVersion,
		opts.TargetVersion,
		r.Now().UTC().Format("20060102T150405.000000000Z"),
		ext,
	)
	return filepath.Join(dir, name), nil
}

type webhookPayload struct {
	Direction          Direction `json:"direction"`
	Dialect            string    `json:"dialect"`
	CurrentVersion     int64     `json:"current_version"`
	TargetVersion      int64     `json:"target_version"`
	DisplayDatabaseURL string    `json:"database_url,omitempty"`
}

func (r Runner) runWebhook(ctx context.Context, opts Options) (Result, error) {
	payload := webhookPayload{
		Direction:          opts.Direction,
		Dialect:            opts.Dialect,
		CurrentVersion:     opts.CurrentVersion,
		TargetVersion:      opts.TargetVersion,
		DisplayDatabaseURL: opts.DisplayDatabaseURL,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return Result{}, fmt.Errorf("%s pre-flight webhook payload: %w", opts.Direction, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, opts.WebhookURL, bytes.NewReader(body))
	if err != nil {
		return Result{}, fmt.Errorf("%s pre-flight webhook request: %w", opts.Direction, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.HTTPClient.Do(req)
	if err != nil {
		return Result{}, webhookError(opts.Direction, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Result{}, fmt.Errorf("%s pre-flight webhook failed: expected HTTP 200, got %d", opts.Direction, resp.StatusCode)
	}
	return Result{Name: "webhook"}, nil
}

func shellCommand(command string) (string, []string) {
	if runtime.GOOS == "windows" {
		return "cmd", []string{"/C", command}
	}
	return "/bin/sh", []string{"-c", command}
}

func hookError(direction Direction, name string, err error, output string) error {
	output = strings.TrimSpace(output)
	if output == "" {
		return fmt.Errorf("%s pre-flight %s hook failed: %w", direction, name, err)
	}
	return fmt.Errorf("%s pre-flight %s hook failed: %w\n%s", direction, name, err, output)
}

func webhookError(direction Direction, err error) error {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		redactedURL := redactSecretURL(urlErr.URL)
		cause := urlErr.Err
		for {
			nested, ok := cause.(*url.Error)
			if !ok {
				break
			}
			if nested.URL != "" {
				redactedURL = redactSecretURL(nested.URL)
			}
			cause = nested.Err
		}
		return fmt.Errorf("%s pre-flight webhook failed for %s: %w", direction, redactedURL, cause)
	}
	return fmt.Errorf("%s pre-flight webhook failed: %w", direction, err)
}

func sanitizeHookOutput(output string, opts Options) string {
	if output == "" {
		return ""
	}
	replacements := hookOutputReplacements(opts)
	for _, replacement := range replacements {
		if replacement.old == "" {
			continue
		}
		output = strings.ReplaceAll(output, replacement.old, replacement.new)
	}
	return output
}

type hookOutputReplacement struct {
	old string
	new string
}

func hookOutputReplacements(opts Options) []hookOutputReplacement {
	displayURL := opts.DisplayDatabaseURL
	if displayURL == "" {
		displayURL = redactedDatabaseURL(opts.DatabaseURL)
	}
	replacements := []hookOutputReplacement{
		{old: opts.DatabaseURL, new: displayURL},
	}
	password := databaseURLPassword(opts.DatabaseURL)
	if password != "" {
		replacements = append(
			replacements,
			hookOutputReplacement{old: "PGPASSWORD=" + password, new: "PGPASSWORD=redacted"},
			hookOutputReplacement{old: "MYSQL_PWD=" + password, new: "MYSQL_PWD=redacted"},
			hookOutputReplacement{old: password, new: "redacted"},
		)
	}
	return replacements
}

func redactSecretURL(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	if parsed.User != nil {
		if _, ok := parsed.User.Password(); ok {
			parsed.User = url.User(parsed.User.Username())
		}
	}
	parsed.RawQuery = redactSecretQuery(parsed.Query()).Encode()
	return parsed.String()
}

func stripSecretURLQuery(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	if parsed.User != nil {
		if _, ok := parsed.User.Password(); ok {
			parsed.User = url.User(parsed.User.Username())
		}
	}
	parsed.RawQuery = stripSecretQuery(parsed.Query()).Encode()
	return parsed.String()
}

func redactSecretQuery(query url.Values) url.Values {
	redacted := make(url.Values, len(query))
	for key, values := range query {
		copied := append([]string(nil), values...)
		if isSecretQueryKey(key) {
			for idx := range copied {
				copied[idx] = "redacted"
			}
		}
		redacted[key] = copied
	}
	return redacted
}

func stripSecretQuery(query url.Values) url.Values {
	stripped := make(url.Values, len(query))
	for key, values := range query {
		if isSecretQueryKey(key) {
			continue
		}
		stripped[key] = append([]string(nil), values...)
	}
	return stripped
}

func isSecretQueryKey(key string) bool {
	switch strings.ToLower(key) {
	case "access_token",
		"api_key",
		"apikey",
		"aws_secret_access_key",
		"aws_session_token",
		"client_secret",
		"id_token",
		"password",
		"passwd",
		"private_key",
		"pwd",
		"refresh_token",
		"secret",
		"sslcert",
		"sslkey",
		"sslpassword",
		"token":
		return true
	default:
		return false
	}
}

func redactedDatabaseURL(dbURL string) string {
	parsed, err := parseDatabaseURL(dbURL)
	if err != nil {
		return dbURL
	}
	if parsed.User != nil {
		if _, ok := parsed.User.Password(); ok {
			parsed.User = url.User(parsed.User.Username())
		}
	}
	parsed.RawQuery = redactSecretQuery(parsed.Query()).Encode()
	return parsed.String()
}

func databaseURLPassword(dbURL string) string {
	parsed, err := parseDatabaseURL(dbURL)
	if err != nil || parsed.User == nil {
		return ""
	}
	password, ok := parsed.User.Password()
	if !ok {
		return ""
	}
	return password
}

func parseDatabaseURL(dbURL string) (*url.URL, error) {
	if strings.HasPrefix(dbURL, "mysql://") || strings.HasPrefix(dbURL, "mariadb://") {
		return parseMySQLURL(dbURL)
	}
	return url.Parse(dbURL)
}

func (r Runner) writeHookOutput(name, output string) {
	output = strings.TrimSpace(output)
	if output == "" {
		return
	}
	fmt.Fprintf(r.Stdout, "%s output:\n%s\n", name, output)
}

func isPostgresDumpDialect(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB:
		return true
	default:
		return false
	}
}

func isMySQLDumpDialect(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.MySQL, platform.MariaDB:
		return true
	default:
		return false
	}
}

func mysqlDumpCommand(dbURL, outputPath string) (args []string, env []string, err error) {
	parsed, err := parseMySQLURL(dbURL)
	if err != nil {
		return nil, nil, err
	}
	dbName := strings.TrimPrefix(parsed.Path, "/")
	if dbName == "" {
		return nil, nil, fmt.Errorf("mysqldump pre-flight hook requires a database name in %s", parsed.Redacted())
	}

	args = []string{"--result-file", outputPath}
	if parsed.Hostname() != "" {
		args = append(args, "--protocol=TCP", "--host", parsed.Hostname())
	}
	if parsed.Port() != "" {
		args = append(args, "--port", parsed.Port())
	}
	if parsed.User != nil {
		if user := parsed.User.Username(); user != "" {
			args = append(args, "--user", user)
		}
		if password, ok := parsed.User.Password(); ok {
			env = append(env, "MYSQL_PWD="+password)
		}
	}
	args = append(args, dbName)
	return args, env, nil
}

func postgresDumpCommand(dbURL, outputPath string) (args []string, env []string) {
	dumpURL := postgresDumpURL(dbURL)
	parsed, err := url.Parse(dumpURL)
	if err == nil && parsed.User != nil {
		if password, ok := parsed.User.Password(); ok {
			displayURL := *parsed
			displayURL.User = url.User(parsed.User.Username())
			dumpURL = displayURL.String()
			env = append(env, "PGPASSWORD="+password)
		}
	}
	dumpURL = stripSecretURLQuery(dumpURL)
	args = []string{"--format=custom", "--file", outputPath, dumpURL}
	return args, env
}

func postgresDumpURL(dbURL string) string {
	parsed, err := url.Parse(dbURL)
	if err != nil {
		return dbURL
	}
	switch platform.NormalizeDialect(parsed.Scheme) {
	case platform.CockroachDB, platform.YugabyteDB:
		parsed.Scheme = platform.Postgres
	}
	return parsed.String()
}

func parseMySQLURL(dbURL string) (*url.URL, error) {
	if strings.Contains(dbURL, "@tcp(") {
		normalized := strings.Replace(dbURL, "@tcp(", "@", 1)
		normalized = strings.Replace(normalized, ")", "", 1)
		return url.Parse(normalized)
	}
	return url.Parse(dbURL)
}
