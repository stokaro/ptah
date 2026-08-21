package postgres

import (
	"fmt"
	"strconv"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// planExtensionChanges turns a modified extension into the statements the
// target accepts, and refuses the two shapes it would otherwise attempt and
// fail at apply time.
//
// This replaces a blanket refusal. Any non-empty ExtensionsModified used to end
// the whole plan with "extension schema moves are not yet supported", which was
// wrong on the engine the message named: PostgreSQL has both
// `ALTER EXTENSION ... UPDATE TO` and `ALTER EXTENSION ... SET SCHEMA`, and the
// reader already captures everything needed to decide between them
// (stokaro/ptah#1718).
//
// The two refusals are refusals rather than skips because the engine answers
// each with an error, and Ptah's message can name the reason the server's
// cannot. Measured on PostgreSQL 18:
//
//   - a downgrade is `extension "pg_trgm" has no update path from version
//     "1.6" to version "1.5"`. An extension carries update scripts in one
//     direction only, so this is not a version the server happens to lack --
//     there is no path at all.
//   - moving one that does not relocate is `extension "plpgsql" does not
//     support SET SCHEMA`. extrelocatable says so before the statement runs.
//
// Order matters within one extension: the version moves first. An update script
// creates objects in the extension's current schema, so moving first would put
// the new objects where the old ones no longer are.
func (p *Planner) planExtensionChanges(result []ast.Node, diff *types.SchemaDiff) ([]ast.Node, error) {
	if diff == nil {
		return result, nil
	}
	for _, change := range diff.ExtensionsModified {
		if !p.hostsExtensionAlterations() {
			return nil, fmt.Errorf(
				"%w: %s cannot alter PostgreSQL extension %q; this target has no ALTER EXTENSION",
				ptaherr.ErrInvalidSchemaDiff, p.targetDialect(), change.Name,
			)
		}
		if change.ToVersion != "" {
			if isVersionDowngrade(change.FromVersion, change.ToVersion) {
				return nil, fmt.Errorf(
					"%w: cannot move PostgreSQL extension %q from version %q back to version %q; "+
						"an extension carries update scripts in one direction only, and the server answers "+
						"\"has no update path\" rather than downgrading",
					ptaherr.ErrInvalidSchemaDiff, change.Name, change.FromVersion, change.ToVersion,
				)
			}
			result = append(result, ast.NewExtension(change.Name).
				SetVersion(change.ToVersion).
				SetAlteration(ast.ExtensionUpdateVersion).
				SetComment(fmt.Sprintf("Update extension %s from %s to %s",
					change.Name, change.FromVersion, change.ToVersion)))
		}
		if change.FromSchema == change.ToSchema {
			continue
		}
		if !change.Relocatable {
			return nil, fmt.Errorf(
				"%w: cannot move PostgreSQL extension %q from schema %q to schema %q; "+
					"the extension is not relocatable and the server answers "+
					"\"does not support SET SCHEMA\"",
				ptaherr.ErrInvalidSchemaDiff, change.Name, change.FromSchema, change.ToSchema,
			)
		}
		result = append(result, ast.NewExtension(change.Name).
			SetSchema(change.ToSchema).
			SetAlteration(ast.ExtensionSetSchema).
			SetComment(fmt.Sprintf("Move extension %s from schema %s to %s",
				change.Name, change.FromSchema, change.ToSchema)))
	}
	return result, nil
}

// hostsExtensionAlterations reports whether this target speaks ALTER EXTENSION.
//
// The PostgreSQL planner serves four dialects. PostgreSQL and YugabyteDB carry
// the statement; CockroachDB and Spanner host no extension to alter, and their
// declarations are already refused earlier by
// validateExtensionInstallationSchemas, so this is the second gate rather than
// the first.
func (p *Planner) hostsExtensionAlterations() bool {
	switch p.targetDialect() {
	case platform.Postgres, platform.YugabyteDB:
		return true
	default:
		return false
	}
}

// isVersionDowngrade reports whether the declared version is demonstrably
// behind the installed one.
//
// An extension version is any string its control file chooses, so this claims a
// downgrade only when both sides are dot-separated numbers and the declared one
// is lower. Anything else -- "1.0-beta", "unstable", a version scheme nobody
// here anticipated -- is left to the server, which answers "has no update path"
// on its own. Refusing on a comparison that guessed would be worse than the
// error it was trying to improve on.
func isVersionDowngrade(from, to string) bool {
	fromParts, ok := numericVersion(from)
	if !ok {
		return false
	}
	toParts, ok := numericVersion(to)
	if !ok {
		return false
	}
	for i := 0; i < len(fromParts) && i < len(toParts); i++ {
		if toParts[i] != fromParts[i] {
			return toParts[i] < fromParts[i]
		}
	}
	return len(toParts) < len(fromParts)
}

// numericVersion splits a dot-separated numeric version, reporting whether it
// is one at all.
func numericVersion(version string) ([]int, bool) {
	fields := strings.Split(strings.TrimSpace(version), ".")
	parts := make([]int, 0, len(fields))
	for _, field := range fields {
		value, err := strconv.Atoi(field)
		if err != nil {
			return nil, false
		}
		parts = append(parts, value)
	}
	return parts, len(parts) > 0
}
