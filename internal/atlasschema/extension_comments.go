package atlasschema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"ptah.run/catalog"
	"ptah.run/core/dataext"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/feature/pgextension"
)

// extensionVersion names one version of one extension, the unit a control
// file describes.
type extensionVersion struct {
	name, version string
}

// extensionControlComments reads, from the dev server, the comment CREATE
// EXTENSION sets for each extension of current that carries a comment: the one
// the control file of the version the rebuild installs gives it. An extension the dev server does
// not offer gets no entry, so its comment is kept. A target without extension
// comments reads nothing.
func extensionControlComments(
	ctx context.Context, devConn *dbschema.DatabaseConnection, current *catalog.Database,
) (map[extensionVersion]string, error) {
	defaults := make(map[extensionVersion]string)
	if !devConn.Info().Capabilities.Has(capability.ExtensionComments) {
		return defaults, nil
	}
	commented, err := commentedExtensions(current)
	if err != nil {
		return nil, err
	}
	for _, key := range commented {
		var comment sql.NullString
		err := extensionControlComment(ctx, devConn, key).Scan(&comment)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read the control file comment of extension %q on the dev database: %w", key.name, err)
		}
		if comment.Valid {
			defaults[key] = comment.String
		}
	}
	return defaults, nil
}

// commentedExtensions lists each of the extension owner's observations in
// current that carries a comment, with its version.
func commentedExtensions(current *catalog.Database) ([]extensionVersion, error) {
	var commented []extensionVersion
	objects, err := current.FeatureObjects.All()
	if err != nil {
		return nil, err
	}
	for _, object := range objects {
		if extension, ok := object.Value.(*pgextension.ObservedExtension); ok && extension.Comment != nil {
			commented = append(commented, extensionVersion{name: pgextension.Name(object.Ref), version: extension.Version})
		}
	}
	return commented, nil
}

// extensionControlComment is the query for one extension's control file
// comment: of the version the rebuild names, or of the default version when it
// names none, which is what CREATE EXTENSION installs then.
func extensionControlComment(ctx context.Context, devConn *dbschema.DatabaseConnection, key extensionVersion) dataext.Row {
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
// is the comment of an extension the dev database already holds, which held
// names and the rebuild does not create.
func omitControlComments(target *schemamodel.Database, held map[string]bool, defaults map[extensionVersion]string) error {
	if target == nil || len(defaults) == 0 {
		return nil
	}
	omitted := func(name, version, comment string) bool {
		if comment == "" || held[name] {
			return false
		}
		control, found := defaults[extensionVersion{name: name, version: version}]
		return found && control == comment
	}
	objects, err := target.FeatureObjects.All()
	if err != nil {
		return err
	}
	for _, object := range objects {
		extension, ok := object.Value.(*pgextension.DesiredExtension)
		if !ok || !omitted(pgextension.Name(object.Ref), extension.Version, extension.Comment) {
			continue
		}
		extension.Comment = ""
		if target.FeatureObjects, err = target.FeatureObjects.Replace(object); err != nil {
			return err
		}
	}
	return nil
}
