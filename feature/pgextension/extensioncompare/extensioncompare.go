// Package extensioncompare compares PostgreSQL extensions for the owner of
// package pgextension. It performs no database access.
package extensioncompare

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgextension"
	"ptah.run/internal/pgtypeext"
	"ptah.run/internal/serverobjects"
)

// Service compares extensions. Its zero value is ready for concurrent use.
//
// An extension is identified by its exact name. Its installation schema is
// compared under the target's default schema, so a declaration that names no
// schema and an extension the catalog reports in that default agree. Its
// version is compared only where the declaration names one: an extension
// declared without a version means whatever the server installs, and the
// installed version is no difference from that. Its comment is compared only
// where the declaration states one and the target stores an extension's
// comment, because CREATE EXTENSION gives the extension the comment of its
// control file.
//
// The comparison plans nothing for an extension the run excludes, in either
// direction, and reports nothing about it: the configuration decided. It
// does not drop an extension a declared column's type needs (a column
// declared `vector(384)` needs pgvector), one the server installs in every
// database it creates, or one the desired source could not describe. It does
// not create an extension whose presence the read could not establish, and
// reports it undecided instead.
type Service struct{}

// pair is one extension's declaration and observation, either missing.
type pair struct {
	ref      objectidentity.ID
	desired  *pgextension.DesiredExtension
	observed *pgextension.ObservedExtension
}

// CompareObjects returns one change per extension that differs. Inputs remain
// unchanged.
func (Service) CompareObjects(ctx context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
	if ctx == nil {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: comparison requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	if !platform.IsPostgresFamily(request.Target) {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: PostgreSQL extension comparison on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{pgextension.ExtensionKind}) {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: unsupported extension comparison kinds", schemaext.ErrInvalidValue)
	}
	if len(request.Requests) != 0 {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: extension comparison accepts no change requests", schemaext.ErrInvalidValue)
	}
	pairs, err := extensionPairs(request)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	scope := comparisonScope{request: request, semantics: defaultSchemaSemantics(request.Identifiers),
		excluded: excludedNames(request.Exclusions), needed: neededExtensions(request.DeclaredColumns)}
	result := schemaext.ObjectComparisonResult{Complete: true, Desired: request.Desired}
	for _, pair := range pairs {
		if err := ctx.Err(); err != nil {
			return schemaext.ObjectComparisonResult{}, err
		}
		scope.compare(pair, &result)
	}
	return result, ctx.Err()
}

// comparisonScope is what one comparison decides each pair with.
type comparisonScope struct {
	request   schemaext.ObjectComparisonRequest
	semantics identifier.Semantics
	excluded  map[string]bool
	needed    map[string]bool
}

func (s comparisonScope) compare(pair pair, result *schemaext.ObjectComparisonResult) {
	name := pgextension.Name(pair.ref)
	switch {
	case s.excluded[name]:
	case pair.observed == nil:
		current := s.request.Current.Coverage.Lookup(pgextension.ExtensionKind, pair.ref)
		if unknown(current) {
			result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: pgextension.ExtensionKind, Subject: pair.ref,
				Reason: "whether the extension is installed was not established: " + current.Reason})
			return
		}
		result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: pair.ref, Value: &pgextension.Change{After: pair.desired}})
	case pair.desired == nil:
		desired := s.request.Desired.Coverage.Lookup(pgextension.ExtensionKind, pair.ref)
		if unknown(desired) || desired.State == schemaext.Defaulted || s.needed[name] || serverobjects.IsExtension(s.request.Target, name) {
			return
		}
		result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: pair.ref, Value: &pgextension.Change{Before: pair.observed}})
	default:
		if change, differs := s.difference(pair); differs {
			result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: pair.ref, Value: change})
		}
	}
}

// difference compares a surviving extension: the schema it is installed in,
// the version the declaration names, and the comment it states.
func (s comparisonScope) difference(pair pair) (*pgextension.Change, bool) {
	change := &pgextension.Change{Before: pair.observed, After: pair.desired}
	if to := s.schemaKey(pair.desired.Schema); to != s.schemaKey(pair.observed.Schema) {
		change.Schema = to
	}
	declared := strings.TrimSpace(pair.desired.Comment)
	if declared != "" && s.request.Capabilities.Has(capability.ExtensionComments) {
		held := ""
		if pair.observed.Comment != nil {
			held = strings.TrimSpace(*pair.observed.Comment)
		}
		change.Comment = declared != held
	}
	return change, change.Schema != "" || change.Comment || change.UpdatesVersion()
}

// schemaKey is the identity of the schema an extension is installed in, the
// default schema where none is named.
func (s comparisonScope) schemaKey(schema string) string {
	if schema == "" {
		schema = s.semantics.DefaultSchema
	}
	return s.semantics.TableIdentityKey(schema)
}

// defaultSchemaSemantics returns the request's identifier rules with the
// PostgreSQL default schema filled in where they name none, so an omitted
// placement and an inspected `public` one are one state.
func defaultSchemaSemantics(semantics identifier.Semantics) identifier.Semantics {
	semantics = semantics.Normalize("")
	if semantics.DefaultSchema == "" {
		semantics.DefaultSchema = identifier.ForDialect(platform.Postgres).DefaultSchema
	}
	return semantics
}

// excludedNames are the extension names the run excludes, exactly as
// configured.
func excludedNames(exclusions []schemaext.Exclusion) map[string]bool {
	names := make(map[string]bool, len(exclusions))
	for _, exclusion := range exclusions {
		if exclusion.Kind == coverage.Extension {
			names[exclusion.Name] = true
		}
	}
	return names
}

// neededExtensions are the extensions a declared column's type comes from.
// `vector(384)` declares that pgvector is needed, as a foreign key declares
// that another table is (stokaro/ptah#2389).
func neededExtensions(columns []schemaext.DeclaredColumn) map[string]bool {
	needed := make(map[string]bool)
	for _, column := range columns {
		if extension, found := pgtypeext.ExtensionFor(column.Type); found {
			needed[extension] = true
		}
	}
	return needed
}

// unknown reports knowledge that establishes neither presence nor absence.
func unknown(knowledge schemaext.Knowledge) bool {
	return knowledge.State == schemaext.Uninspected || knowledge.State == schemaext.Unrepresentable
}

// extensionPairs pairs declarations with observations by name, ordered by
// name. A declaration among the observations, or the reverse, is refused.
func extensionPairs(request schemaext.ObjectComparisonRequest) ([]pair, error) {
	pairs := make(map[objectidentity.Key]*pair)
	for _, side := range []struct {
		state          schemaext.ObjectState
		representation schemaext.Representation
	}{{request.Desired, schemaext.Desired}, {request.Current, schemaext.Observed}} {
		objects, err := side.state.Objects.All()
		if err != nil {
			return nil, err
		}
		for _, object := range objects {
			if err := pgextension.ValidateRef(object.Ref); err != nil {
				return nil, err
			}
			entry := pairs[object.Ref.Key()]
			if entry == nil {
				entry = &pair{ref: object.Ref}
				pairs[object.Ref.Key()] = entry
			}
			if err := capture(entry, object.Value, side.representation); err != nil {
				return nil, err
			}
		}
	}
	result := make([]pair, 0, len(pairs))
	for _, key := range slices.SortedFunc(maps.Keys(pairs), func(a, b objectidentity.Key) int {
		return schemaext.CompareRefs(pairs[a].ref, pairs[b].ref)
	}) {
		result = append(result, *pairs[key])
	}
	return result, nil
}

func capture(entry *pair, value schemaext.Value, representation schemaext.Representation) error {
	switch typed := value.(type) {
	case *pgextension.DesiredExtension:
		if representation != schemaext.Desired {
			return fmt.Errorf("%w: an extension declaration among the observations: %s", schemaext.ErrInvalidValue, entry.ref)
		}
		entry.desired = typed
		return pgextension.ValidateDesired(typed)
	case *pgextension.ObservedExtension:
		if representation != schemaext.Observed {
			return fmt.Errorf("%w: an extension observation among the declarations: %s", schemaext.ErrInvalidValue, entry.ref)
		}
		entry.observed = typed
		return pgextension.ValidateObserved(typed)
	default:
		return fmt.Errorf("%w: unexpected extension operand %T", schemaext.ErrInvalidValue, value)
	}
}
