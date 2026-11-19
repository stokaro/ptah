package atlasschema

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"ptah.run/core/difftypes"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/atlascheckform"
	"ptah.run/internal/atlassource"
	"ptah.run/internal/devclean"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/devlock"
	"ptah.run/internal/readscope"
)

// DevCheckSpellings creates the tables of desired that declare a CHECK diff
// writes on the dev database devConn reaches, adds each such CHECK to its
// table there, and returns the form the dev database reports for each, as the
// pinned community binary prints it; see [atlascheckform]. diff is the
// comparison of desired with the current state.
//
// The caller holds the dev database's lock, as `migrate diff` does across its
// whole run. The dev database is reset before the tables are created and
// after the expressions are read, as every materialization on it is. Where
// the dev server's descriptor prints a CHECK as declared, or diff writes no
// CHECK desired declares, nothing is created and the zero Spellings is
// returned.
//
// diag receives what the creation reports, such as a role the dev server
// already has; nil discards it.
func DevCheckSpellings(
	ctx context.Context,
	service renderer.SchemaService,
	devConn *dbschema.DatabaseConnection,
	desired *schemamodel.Database,
	diff *difftypes.SchemaDiff,
	diag io.Writer,
) (atlascheckform.Spellings, error) {
	dialect := devConn.Info().Dialect
	probes := atlascheckform.Plan(dialect, desired, diff)
	if len(probes) == 0 {
		return atlascheckform.Spellings{}, nil
	}
	if err := schemaext.RequireRuntime(ctx, service); err != nil {
		return atlascheckform.Spellings{}, err
	}
	var spellings atlascheckform.Spellings
	err := materializeOnLockedDev(ctx, service, devConn, atlascheckform.Tables(desired, probes), diag,
		func(conn *dbschema.DatabaseConnection, _ devclean.Baseline) error {
			created, err := dbschema.ReadSchemaWithSchemasContext(readscope.Authored(ctx), conn, nil)
			if err != nil {
				return fmt.Errorf("read dev database tables: %w", err)
			}
			statements, err := atlascheckform.Statements(dialect, probes, created)
			if err != nil {
				return err
			}
			for i, statement := range statements {
				// Sent past the plan's executor, whose error carries the
				// statement, and the probe's statement names nothing the author
				// wrote.
				if _, err := conn.ExecContext(ctx, statement); err != nil {
					return &atlascheckform.Refusal{Probe: probes[i], Err: err}
				}
			}
			probed, err := dbschema.ReadSchemaWithSchemasContext(readscope.Authored(ctx), conn, nil)
			if err != nil {
				return fmt.Errorf("read dev database CHECK constraints: %w", err)
			}
			spellings, err = atlascheckform.Read(dialect, probes, probed)
			return err
		})
	if err != nil {
		return atlascheckform.Spellings{}, fmt.Errorf("read CHECK expressions on dev database: %w", err)
	}
	return spellings, nil
}

// diffCheckSpellings answers the form the dev database of a schema diff
// reports for each CHECK of the --to declaration that diff writes, when the
// diff asked for it ([DiffOptions.DevCheckSpelling]), names a dev database,
// and --to is a declaration rather than a database or a migration directory,
// whose state the server already wrote. Otherwise it answers the zero
// Spellings.
//
// It runs once the comparison is made and every connection of it is
// released, so a --from directory replayed on the same dev database has
// finished. It connects and locks the dev database the way a materialization
// of --from does, and refuses a dev database that is one of the sources.
func diffCheckSpellings(
	ctx context.Context,
	opts DiffOptions,
	dialect string,
	toKind atlassource.Kind,
	desired *schemamodel.Database,
	diff *difftypes.SchemaDiff,
	protected []devlock.Protected,
) (atlascheckform.Spellings, error) {
	// The dialect the diff is pinned to decides before anything connects: a
	// MySQL-family URL may reach MariaDB or MySQL, and both rewrite a CHECK, so
	// a dialect that needs no probe needs no dev connection either.
	if !opts.DevCheckSpelling || readsServer(toKind) || strings.TrimSpace(opts.DevURL) == "" ||
		len(atlascheckform.Plan(dialect, desired, diff)) == 0 {
		return atlascheckform.Spellings{}, nil
	}
	devURL, releaseDev, err := devdocker.Resolve(ctx, opts.DevURL, devdocker.Options{
		DeclaredDisposable: opts.DevServerDisposable,
	})
	if err != nil {
		return atlascheckform.Spellings{}, err
	}
	defer releaseDev()
	devConn, err := connectInspectSource(ctx, devURL, opts.ConnectTimeout, dbschema.ConnectToServer)
	if err != nil {
		return atlascheckform.Spellings{}, fmt.Errorf("connect to --dev-url: %w", err)
	}
	defer dbschema.CloseAndWarn(devConn)
	if len(atlascheckform.Plan(devConn.Info().Dialect, desired, diff)) == 0 {
		return atlascheckform.Spellings{}, nil
	}
	if err := devlock.EnsureDistinct(ctx, devConn, protected...); err != nil {
		return atlascheckform.Spellings{}, err
	}
	lock, err := devlock.Acquire(ctx, devConn, 0)
	if err != nil {
		return atlascheckform.Spellings{}, fmt.Errorf("acquire schema diff dev database lock: %w", err)
	}
	guarded, settle := lock.Guard(ctx)
	spellings, err := DevCheckSpellings(guarded, opts.Runtime, devConn, desired, diff, opts.Diagnostics)
	err = settle(err)
	if releaseErr := lock.Release(); releaseErr != nil {
		err = errors.Join(err, fmt.Errorf("release schema diff dev database lock: %w", releaseErr))
	}
	if err != nil {
		return atlascheckform.Spellings{}, err
	}
	return spellings, nil
}
