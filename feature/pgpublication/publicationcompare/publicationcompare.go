// Package publicationcompare compares PostgreSQL logical replication
// publications for the owner of package pgpublication. It performs no
// database access.
package publicationcompare

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpublication"
)

// Service compares publications. Its zero value is ready for concurrent use.
//
// A publication is identified by its exact name. A declared table that names
// no schema is the default schema's, so a declaration and the catalog agree
// on it. Membership is compared in its own form: a change between FOR ALL
// TABLES and a list of tables or schemas rebuilds the publication, since
// ALTER PUBLICATION cannot make it; any other membership change adds and drops
// the tables and schemas that differ and leaves the rest alone. The publish
// set and both settings are compared as the server applies them, so a
// declaration that names no publish set agrees with one that publishes all
// four operations. The owner is compared only where the declaration names
// one; an owner left out is not managed.
//
// A kept table whose row filter or column list differs is a change, recorded
// in [pgpublication.Change.Refiltered]: the planner refuses it, because Ptah
// writes neither, and publishing the whole table would publish more than the
// publication does.
//
// A desired schema that declares no publication drops none. Publications are
// managed from the first one the schema declares: until then, one a
// replication consumer created for itself, as Debezium does, or one an image
// ships, as Supabase ships supabase_realtime, is not the schema's to remove.
// A document of the Atlas Community Edition, which has no spelling for a
// publication, leaves them alone as that binary does. The rule reads the
// whole desired schema, so it holds however the schema is split across
// files.
//
// The comparison drops no publication the desired source could not describe,
// and creates none whose presence the read could not establish: it reports it
// undecided instead.
type Service struct{}

// pair is one publication's declaration and observation, either missing.
type pair struct {
	ref      objectidentity.ID
	desired  *pgpublication.DesiredPublication
	observed *pgpublication.ObservedPublication
}

// CompareObjects returns one change per publication that differs, and counts
// the publications it compared: the ones both sides hold, the creations and
// the drops. Inputs remain unchanged.
func (Service) CompareObjects(ctx context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
	if ctx == nil {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: comparison requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	if !platform.IsPostgresFamily(request.Target) {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: PostgreSQL publication comparison on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{pgpublication.PublicationKind}) {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: unsupported publication comparison kinds", schemaext.ErrInvalidValue)
	}
	if len(request.Requests) != 0 {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: publication comparison accepts no change requests", schemaext.ErrInvalidValue)
	}
	pairs, err := publicationPairs(request)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	defaultSchema := DefaultSchema(request.Identifiers)
	manages := pgpublication.Managed(request.Desired.Objects)
	result := schemaext.ObjectComparisonResult{Complete: true, Desired: request.Desired,
		Counts: []schemaext.ObjectCount{{Kind: pgpublication.ChangeKind}}}
	for _, pair := range pairs {
		if err := ctx.Err(); err != nil {
			return schemaext.ObjectComparisonResult{}, err
		}
		switch {
		case pair.observed == nil:
			current := request.Current.Coverage.Lookup(pgpublication.PublicationKind, pair.ref)
			if unknown(current) {
				result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: pgpublication.PublicationKind, Subject: pair.ref,
					Reason: "whether the publication exists was not established: " + current.Reason})
				continue
			}
			result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: pair.ref,
				Value: &pgpublication.Change{After: Qualify(pair.desired, defaultSchema)}})
			result.Counts[0].Added++
		case pair.desired == nil:
			desired := request.Desired.Coverage.Lookup(pgpublication.PublicationKind, pair.ref)
			if !manages || unknown(desired) || desired.State == schemaext.Defaulted {
				continue
			}
			result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: pair.ref, Value: &pgpublication.Change{Before: pair.observed}})
			result.Counts[0].Removed++
		default:
			result.Counts[0].Matched++
			if change, differs := Difference(pair.observed, pair.desired, defaultSchema); differs {
				result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: pair.ref, Value: change})
			}
		}
	}
	return result, ctx.Err()
}

// DefaultSchema is the schema a declared table that names none is in: the
// one semantics names, and PostgreSQL's where it names none.
func DefaultSchema(semantics identifier.Semantics) string {
	if schema := semantics.Normalize("").DefaultSchema; schema != "" {
		return schema
	}
	return identifier.ForDialect(platform.Postgres).DefaultSchema
}

// Qualify returns the declaration with every table qualified by its schema,
// defaultSchema where it names none. The declaration is not changed.
func Qualify(desired *pgpublication.DesiredPublication, defaultSchema string) *pgpublication.DesiredPublication {
	qualified := desired.Copy()
	qualified.Tables = qualifyMembers(qualified.Tables, defaultSchema)
	return qualified
}

func qualifyMembers(members []pgpublication.Member, defaultSchema string) []pgpublication.Member {
	for i := range members {
		if members[i].Schema == "" {
			members[i].Schema = defaultSchema
		}
	}
	return members
}

// Difference compares a surviving publication, with each table either side
// names without a schema in defaultSchema, and reports whether it differs.
func Difference(current *pgpublication.ObservedPublication, declared *pgpublication.DesiredPublication,
	defaultSchema string,
) (*pgpublication.Change, bool) {
	desired := Qualify(declared, defaultSchema)
	observed := current.Copy()
	observed.Tables = qualifyMembers(observed.Tables, defaultSchema)
	change := &pgpublication.Change{Before: observed, After: desired}
	if desired.AllTables != observed.AllTables {
		change.Rebuild = true
	} else {
		held := membersByTable(observed.Tables)
		wanted := membersByTable(desired.Tables)
		for _, key := range sortedKeys(wanted) {
			member, found := held[key]
			switch {
			case !found:
				change.AddTables = append(change.AddTables, bare(wanted[key]))
			case member.RowFilter != wanted[key].RowFilter || !slices.Equal(member.Columns, wanted[key].Columns):
				change.Refiltered = append(change.Refiltered, bare(member))
			}
		}
		for _, key := range sortedKeys(held) {
			if _, found := wanted[key]; !found {
				change.DropTables = append(change.DropTables, bare(held[key]))
			}
		}
		change.AddSchemas = missing(desired.Schemas, observed.Schemas)
		change.DropSchemas = missing(observed.Schemas, desired.Schemas)
	}
	change.Options = !slices.Equal(desired.EffectivePublish(), observed.EffectivePublish()) ||
		desired.ViaPartitionRoot != observed.ViaPartitionRoot || desired.EffectiveGeneratedColumns() != observed.EffectiveGeneratedColumns()
	if desired.Owner != "" && (desired.Owner != observed.Owner || change.Rebuild) {
		change.Owner = desired.Owner
	}
	differs := change.Rebuild || change.ChangesMembership() || len(change.Refiltered) != 0 || change.Options || change.Owner != ""
	return change, differs
}

// bare is the member without its row filter and column list: the table a
// change names.
func bare(member pgpublication.Member) pgpublication.Member {
	return pgpublication.Member{Schema: member.Schema, Table: member.Table}
}

func membersByTable(members []pgpublication.Member) map[[2]string]pgpublication.Member {
	result := make(map[[2]string]pgpublication.Member, len(members))
	for _, member := range members {
		result[[2]string{member.Schema, member.Table}] = member
	}
	return result
}

func sortedKeys(members map[[2]string]pgpublication.Member) [][2]string {
	return slices.SortedFunc(maps.Keys(members), func(a, b [2]string) int {
		return cmp.Or(cmp.Compare(a[0], b[0]), cmp.Compare(a[1], b[1]))
	})
}

// missing returns the names of from that to does not hold, sorted.
func missing(from, to []string) []string {
	var result []string
	for _, name := range from {
		if !slices.Contains(to, name) && !slices.Contains(result, name) {
			result = append(result, name)
		}
	}
	slices.Sort(result)
	return result
}

// unknown reports knowledge that establishes neither presence nor absence.
func unknown(knowledge schemaext.Knowledge) bool {
	return knowledge.State == schemaext.Uninspected || knowledge.State == schemaext.Unrepresentable
}

// publicationPairs pairs declarations with observations by name, ordered by
// name. A declaration among the observations, or the reverse, is refused.
func publicationPairs(request schemaext.ObjectComparisonRequest) ([]pair, error) {
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
			if err := pgpublication.ValidateRef(object.Ref); err != nil {
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
	case *pgpublication.DesiredPublication:
		if representation != schemaext.Desired {
			return fmt.Errorf("%w: a publication declaration among the observations: %s", schemaext.ErrInvalidValue, entry.ref)
		}
		entry.desired = typed
		return pgpublication.ValidateDesired(typed)
	case *pgpublication.ObservedPublication:
		if representation != schemaext.Observed {
			return fmt.Errorf("%w: a publication observation among the declarations: %s", schemaext.ErrInvalidValue, entry.ref)
		}
		entry.observed = typed
		return pgpublication.ValidateObserved(typed)
	default:
		return fmt.Errorf("%w: unexpected publication operand %T", schemaext.ErrInvalidValue, value)
	}
}
