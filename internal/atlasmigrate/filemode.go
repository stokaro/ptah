package atlasmigrate

import (
	"context"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasmigrateimport"
	"ptah.run/migration/migrator"
)

// markFileModeRefusals tags `-- atlas:txmode none` on each planned file the
// migrator would refuse to run in transaction mode file, so `migrate apply`
// runs every file this verb writes with its default --tx-mode
// (stokaro/ptah#4013).
//
// The migrator answers, through [migrator.FileModeRefusal], against conn
// holding the replayed directory, which is the catalog the new file is applied
// on top of. Only MySQL and MariaDB refuse anything; their DDL commits on its
// own, so the tag gives up nothing a transaction kept, and the community binary
// honors it too.
//
// Only the Atlas layout is tagged. A foreign layout is applied by the tool that
// owns it, which does not run Ptah's check, and [validateForeignTransactionMode]
// would refuse a marker that layout has no spelling for.
func markFileModeRefusals(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	opts DiffOptions,
	contents []MigrationFileContent,
) ([]MigrationFileContent, error) {
	if !opts.MarkFileModeRefusals || opts.dirFormat() != atlasmigrateimport.FormatAtlas {
		return contents, nil
	}
	for i, content := range contents {
		if content.NoTransaction {
			continue
		}
		refusal := migrator.FileModeRefusal(ctx, conn, content.SQL)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if refusal != nil {
			contents[i] = withTxModeNoneDirective(content)
		}
	}
	return contents, nil
}
