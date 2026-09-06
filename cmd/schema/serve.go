package schema

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/spf13/cobra"

	"ptah.run/cmd/internal/cmdutil"
	"ptah.run/cmd/internal/dbcli"
	"ptah.run/cmd/internal/schemaserve"
	"ptah.run/config/projectconfig"
)

const (
	serveRootDirFlag = "root-dir"
	serveDBURLFlag   = "db-url"
	serveAddrFlag    = "addr"
	serveRefreshFlag = "refresh"
	serveTitleFlag   = "title"

	// serveShutdownGrace bounds how long a stop waits for a request in flight.
	// The page is cheap to render, so a request that outlives this is one that
	// is stuck rather than one that is busy.
	serveShutdownGrace = 5 * time.Second
)

type schemaServeOptions struct {
	rootDirs   []string
	dbURL      string
	addr       string
	refresh    time.Duration
	title      string
	schemasRaw string
	configPath string
}

// NewSchemaServeCommand returns the native `schema serve` command.
func NewSchemaServeCommand() *cobra.Command {
	return newSchemaServeCommand()
}

func newSchemaServeCommand() *cobra.Command {
	opts := schemaServeOptions{}
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Serve a live view of the schema and the database it describes",
		Long: `Serve a page showing the declared schema and how the live database differs
from it, refreshed while somebody is watching.

The schema itself is rendered by the same code as ` + "`schema export --to html`" + `,
so the served view and the exported document cannot disagree about what a
schema looks like. What this adds is what only means something over time:
drift, and when it was last measured.

It is read-only. Every route answers GET and HEAD and nothing else, and no code
path here writes to the database. Nothing about it becomes a dependency of a
migration -- ` + "`ptah migrate`" + ` works identically with this never started.

There is no account, no login and no upload. It binds to a local address and
reads a database you already have credentials for.

  ptah schema serve --db-url "$DATABASE_URL" --root-dir ./models
  ptah schema serve --db-url "$DATABASE_URL" --root-dir ./models --addr 127.0.0.1:7070 --refresh 15s

--refresh 0 serves a page that does not reload itself.

The declared schema comes from --root-dir. --schema-file is deliberately absent
here: a schema file may name an oci:// artifact, and what pulling one means for
a process that re-reads on a timer is a design question rather than an
oversight. ` + "`schema drift`" + ` takes it and answers once, which is a different
bargain.`,
		Args:          cmdutil.NoPositionalArgs,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSchemaServe(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringSliceVar(&opts.rootDirs, serveRootDirFlag, nil, "Directory to scan for Go annotations (repeatable)")
	flags.StringVar(&opts.dbURL, serveDBURLFlag, "", "Database URL to compare against")
	flags.StringVar(&opts.addr, serveAddrFlag, "127.0.0.1:7070", "Address to listen on")
	flags.DurationVar(&opts.refresh, serveRefreshFlag, 30*time.Second, "How often the page reloads itself; 0 disables")
	flags.StringVar(&opts.title, serveTitleFlag, "", "Title for the page")
	flags.StringVar(&opts.schemasRaw, dbcli.SchemasFlagName, "", "Comma-separated schemas to read")
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func runSchemaServe(cmd *cobra.Command, opts schemaServeOptions) error {
	projectCfg, err := dbcli.LoadProjectConfig(cmd, opts.configPath)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	dbURL := dbcli.EffectiveString(cmd, serveDBURLFlag, opts.dbURL,
		projectCfg.StringValue(projectconfig.StringDatabaseURL))
	if dbURL == "" {
		return cmdutil.Fail(cmd, errors.New("database URL is required"))
	}
	schemasValue := dbcli.EffectiveString(cmd, dbcli.SchemasFlagName, opts.schemasRaw,
		dbcli.JoinSchemasValue(projectCfg.SchemasValue()))

	handler, err := schemaserve.Handler(schemaserve.Options{
		DatabaseURL: dbURL,
		RootDirs:    opts.rootDirs,
		Schemas:     dbcli.ParseSchemas(schemasValue),
		Title:       opts.title,
		Refresh:     opts.refresh,
	})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	listener, err := net.Listen("tcp", opts.addr)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("listen on %s: %w", opts.addr, err))
	}
	// The address is printed from the listener rather than from the flag,
	// because port 0 is a legitimate request for whichever port is free and the
	// flag does not say which one that turned out to be.
	fmt.Fprintf(cmd.OutOrStdout(), "Serving a read-only schema view on http://%s\n", listener.Addr())

	return serveUntilDone(cmd.Context(), listener, handler)
}

// serveUntilDone runs the server until the context is cancelled, then stops it
// without dropping a request that is already being answered.
func serveUntilDone(ctx context.Context, listener net.Listener, handler http.Handler) error {
	server := &http.Server{
		Handler: handler,
		// A dashboard reads from a database on every request, so the read
		// header timeout is the one that guards the socket rather than the
		// handler.
		ReadHeaderTimeout: 10 * time.Second,
	}
	errs := make(chan error, 1)
	go func() { errs <- server.Serve(listener) }()

	select {
	case err := <-errs:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), serveShutdownGrace)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	}
}
