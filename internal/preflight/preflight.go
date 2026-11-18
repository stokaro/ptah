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

	"ptah.run/core/platform"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/dburldisplay"
	"ptah.run/internal/execpolicy"
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
	// CurrentVersionKey and TargetVersionKey name the same migrations by
	// their version keys; empty names no migration.
	CurrentVersionKey string
	TargetVersionKey  string
	// KeyedByPosition reports that the numeric versions are positions in
	// file-name order, which move when a file is added before them and name
	// no migration outside this run. The hooks then receive the keys alone:
	// PTAH_CURRENT_VERSION_KEY and PTAH_TARGET_VERSION_KEY, the webhook
	// members current_version_key and target_version_key, and a dump named by
	// the keys.
	KeyedByPosition bool
	Command         string
	// CommandOrigin names the project file construct that supplied Command,
	// such as `ptah.yaml migration.pre_up_hook`, so the binary's program
	// policy can refuse it; empty is a command the operator passed.
	CommandOrigin   string
	PostgresDumpDir string
	MySQLDumpDir    string
	WebhookURL      string
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
	Run(ctx context.Context, name string, args, env []string) (string, error)
}

// ExecCommandRunner runs commands with os/exec.
type ExecCommandRunner struct{}

// Run executes one command with the supplied extra environment.
func (ExecCommandRunner) Run(ctx context.Context, name string, args, env []string) (string, error) {
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
	env := []string{"PTAH_DB_URL=" + o.DatabaseURL, "PTAH_DIALECT=" + o.Dialect}
	if o.KeyedByPosition {
		return append(env,
			"PTAH_CURRENT_VERSION_KEY="+o.CurrentVersionKey,
			"PTAH_TARGET_VERSION_KEY="+o.TargetVersionKey,
		)
	}
	return append(env,
		fmt.Sprintf("PTAH_CURRENT_VERSION=%d", o.CurrentVersion),
		fmt.Sprintf("PTAH_TARGET_VERSION=%d", o.TargetVersion),
	)
}

// dumpVersions is the part of a dump's file name that names the migrations
// it sits between: v<current>_to_v<target> for numbers, and
// <current>_to_<target> for keys, which are spelled as their files spell them
// and may start with a v of their own. A key comes from a file name, so it is
// a file name's bytes; an empty key, which names no migration, is spelled
// none.
func (o Options) dumpVersions() string {
	if !o.KeyedByPosition {
		return fmt.Sprintf("v%d_to_v%d", o.CurrentVersion, o.TargetVersion)
	}
	key := func(key string) string {
		if key == "" {
			return "none"
		}
		return key
	}
	return key(o.CurrentVersionKey) + "_to_" + key(o.TargetVersionKey)
}

func (r Runner) runCommandHook(ctx context.Context, opts Options, env []string) (Result, error) {
	name, args := shellCommand(opts.Command)
	if err := execpolicy.Authorize(ctx, opts.CommandOrigin, append([]string{name}, args...)); err != nil {
		return Result{}, err
	}
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
		"ptah_pre_%s_%s%s",
		opts.dumpVersions(),
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

// keyedWebhookPayload is the payload where the versions are positions (see
// [Options.KeyedByPosition]): the same members, with the keys in place of
// the numbers.
type keyedWebhookPayload struct {
	Direction          Direction `json:"direction"`
	Dialect            string    `json:"dialect"`
	CurrentVersionKey  string    `json:"current_version_key"`
	TargetVersionKey   string    `json:"target_version_key"`
	DisplayDatabaseURL string    `json:"database_url,omitempty"`
}

func (o Options) webhookPayload() any {
	if o.KeyedByPosition {
		return keyedWebhookPayload{
			Direction:          o.Direction,
			Dialect:            o.Dialect,
			CurrentVersionKey:  o.CurrentVersionKey,
			TargetVersionKey:   o.TargetVersionKey,
			DisplayDatabaseURL: o.DisplayDatabaseURL,
		}
	}
	return webhookPayload{
		Direction:          o.Direction,
		Dialect:            o.Dialect,
		CurrentVersion:     o.CurrentVersion,
		TargetVersion:      o.TargetVersion,
		DisplayDatabaseURL: o.DisplayDatabaseURL,
	}
}

func (r Runner) runWebhook(ctx context.Context, opts Options) (Result, error) {
	body, err := json.Marshal(opts.webhookPayload())
	if err != nil {
		return Result{}, fmt.Errorf("%s pre-flight webhook payload: %w", opts.Direction, err)
	}

	// Parsed here first: the error of the request below quotes the URL whole.
	if _, err := dburldisplay.ParseURL(opts.WebhookURL); err != nil {
		return Result{}, fmt.Errorf("%s pre-flight webhook request: %w", opts.Direction, err)
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

// Shell is the program a command hook runs under, named in a refusal before
// the hook would start.
func Shell() string {
	name, _ := shellCommand("")
	return name
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
	if urlErr, ok := errors.AsType[*url.Error](err); ok {
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
	for _, secret := range dburldisplay.Secrets(opts.DatabaseURL) {
		replacements = append(
			replacements,
			hookOutputReplacement{old: "PGPASSWORD=" + secret, new: "PGPASSWORD=redacted"},
			hookOutputReplacement{old: "MYSQL_PWD=" + secret, new: "MYSQL_PWD=redacted"},
			hookOutputReplacement{old: secret, new: "redacted"},
		)
	}
	return replacements
}

func redactSecretURL(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return dburldisplay.Format(rawURL)
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
		if dburldisplay.IsSecretParameter(key) {
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
		if dburldisplay.IsSecretParameter(key) {
			continue
		}
		stripped[key] = append([]string(nil), values...)
	}
	return stripped
}

// redactedDatabaseURL reads the address from what the display shows, so that what
// a parser misreads in a password never reaches the result.
func redactedDatabaseURL(dbURL string) string {
	display := dburldisplay.Format(dbURL)
	parsed, err := parseDatabaseURL(display)
	if err != nil {
		return display
	}
	if parsed.User != nil {
		if _, ok := parsed.User.Password(); ok {
			parsed.User = url.User(parsed.User.Username())
		}
	}
	parsed.RawQuery = redactSecretQuery(parsed.Query()).Encode()
	return parsed.String()
}

// parseDatabaseURL reads a database URL for display and for its password. A
// MySQL-family URL is read by atlasurl.ParseMySQLURL, the parser the connection
// uses, because net/url refuses its go-sql-driver form and reads a socket URL's
// path where the database is not.
func parseDatabaseURL(dbURL string) (*url.URL, error) {
	mysqlURL, err := atlasurl.ParseMySQLURL(dbURL)
	switch {
	case err == nil:
		return mysqlURL.URL(), nil
	case !errors.Is(err, atlasurl.ErrNotMySQLURL):
		return nil, err
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

// mysqlDumpCommand points mysqldump at the server and the database the
// migration connects to. Both are read by atlasurl.ParseMySQLURL, which the
// connection reads the URL with too, so the port and the host are the driver's
// even where the URL leaves them out, and a socket URL reaches the socket
// rather than a host named after its path.
//
// A URL with a CA bundle is refused: mysqldump is given no TLS settings, so it
// would send the credentials to a server whose certificate nothing verified.
func mysqlDumpCommand(dbURL, outputPath string) (args, env []string, err error) {
	parsed, err := atlasurl.ParseMySQLURL(dbURL)
	if err != nil {
		return nil, nil, err
	}
	if parsed.Database() == "" {
		return nil, nil, fmt.Errorf("mysqldump pre-flight hook requires a database name in %s", parsed.URL().Redacted())
	}
	if parsed.CAFile() != "" {
		return nil, nil, errors.New("the mysqldump pre-flight hook is given no TLS settings, and the database URL sets ssl-ca, " +
			"so mysqldump would connect without verifying the server's certificate; remove the hook or ssl-ca")
	}

	args = []string{"--result-file", outputPath}
	if host, port, tcp := parsed.HostPort(); tcp {
		args = append(args, "--protocol=TCP", "--host", host, "--port", port)
	} else {
		args = append(args, "--protocol=SOCKET", "--socket", parsed.Address())
	}
	if user := parsed.User(); user != "" {
		args = append(args, "--user", user)
	}
	if password := parsed.Password(); password != "" {
		env = append(env, "MYSQL_PWD="+password)
	}
	args = append(args, parsed.Database())
	return args, env, nil
}

func postgresDumpCommand(dbURL, outputPath string) (args, env []string) {
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
