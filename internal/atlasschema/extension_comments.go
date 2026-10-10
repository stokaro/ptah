package atlasschema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
)

// extensionVersion names one version of one extension, the unit a control
// file describes.
type extensionVersion struct {
	name, version string
}

// extensionControlComments reads, from the dev server, the comment CREATE
// EXTENSION sets for each extension of current that carries a comment: the
// one the control file of the version the rebuild installs gives it. An
// extension the dev server does not offer gets no entry, so its comment is
// kept. A target without extension comments reads nothing.
func extensionControlComments(
	ctx context.Context, devConn *dbschema.DatabaseConnection, current *catalog.Database,
) (map[extensionVersion]string, error) {
	defaults := make(map[extensionVersion]string)
	if !devConn.Info().Capabilities.Has(capability.ExtensionComments) {
		return defaults, nil
	}
	for _, extension := range current.Extensions {
		if extension.Comment == nil {
			continue
		}
		key := extensionVersion{name: extension.Name, version: extension.Version}
		var comment sql.NullString
		err := extensionControlComment(ctx, devConn, key).Scan(&comment)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read the control file comment of extension %q on the dev database: %w", extension.Name, err)
		}
		if comment.Valid {
			defaults[key] = comment.String
		}
	}
	return defaults, nil
}

// extensionControlComment is the query for one extension's control file
// comment: of the version the rebuild names, or of the default version when it
// names none, which is what CREATE EXTENSION installs then.
func extensionControlComment(ctx context.Context, devConn *dbschema.DatabaseConnection, key extensionVersion) *sql.Row {
	if key.version == "" {
		return devConn.QueryRowContext(ctx,
			"SELECT comment FROM pg_catalog.pg_available_extensions WHERE name = $1", key.name)
	}
	return devConn.QueryRowContext(ctx,
		"SELECT comment FROM pg_catalog.pg_available_extension_versions WHERE name = $1 AND version = $2", key.name, key.version)
}

// omitControlComments clears, in target, each comment of an extension the
// rebuild creates that equals the comment its control file gives, so the
// rebuild writes no COMMENT ON EXTENSION for it: CREATE EXTENSION already sets
// that comment, and the statement would be refused on a dev server the run
// does not own, since the cleanup does not restore a comment. A comment that
// differs from the control file's is kept and meets the baseline guard, and so
// is the comment of an extension the dev database already holds, which the
// rebuild does not create.
func omitControlComments(target *schemamodel.Database, held []catalog.Extension, defaults map[extensionVersion]string) {
	if target == nil || len(defaults) == 0 {
		return
	}
	target.Extensions = slices.Clone(target.Extensions)
	for i, extension := range target.Extensions {
		if extension.Comment == "" || slices.ContainsFunc(held, func(dev catalog.Extension) bool { return dev.Name == extension.Name }) {
			continue
		}
		if comment, found := defaults[extensionVersion{name: extension.Name, version: extension.Version}]; found && comment == extension.Comment {
			target.Extensions[i].Comment = ""
		}
	}
}
