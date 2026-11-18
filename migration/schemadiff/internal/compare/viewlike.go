package compare

import (
	"slices"
	"sort"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/coverage"
	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/objectowner"
	"ptah.run/internal/tableref"
)

// A view and a materialized view are compared by the same two walks.
//
// They were two copies of each walk until both families began carrying their
// operands, which removed the last difference between them -- what went into
// the name list (stokaro/ptah#2315). What is left that genuinely differs is
// this descriptor: the models, the accessors, and where the answers go. Adding
// a third view-like family means writing one of these, not a fifth walk, which
// is the cost item 2 of that issue is about.
type viewLikeFamily[LA ~[]D, LM ~[]M, D, C, M any] struct {
	// Kind is the identity namespace the semantics-aware walk matches in.
	Kind objectidentity.Kind

	// CoverageKind is empty for families without a coverage vocabulary.
	CoverageKind coverage.Kind

	Desired  func(*schemamodel.Database) []D
	Current  func(*catalog.Database) []C
	Added    func(*difftypes.SchemaDiff) *LA
	Modified func(*difftypes.SchemaDiff) *LM
	Removed  func(*difftypes.SchemaDiff) *LA
	// Matched is the pair count the walks add to; see
	// [difftypes.MatchedCounts].
	Matched func(*difftypes.SchemaDiff) *int

	DesiredName          func(D) string
	CurrentName          func(C) string
	CurrentSchema        func(C) string
	CurrentQualifiedName func(C) string
	ModifiedName         func(M) string

	// Compare answers what changed between a declaration and the row it
	// describes; Changed reads that answer.
	Compare func(D, C, string) M
	Changed func(M) bool

	// Carry describes a row the database reported in the shape the diff holds,
	// for the dialect the comparison targets.
	Carry func(C, string) D
}

var plainViewFamily = viewLikeFamily[
	difftypes.ViewChanges, []difftypes.ViewDiff,
	schemamodel.View, catalog.View, difftypes.ViewDiff,
]{
	Kind:         objectidentity.KindView,
	CoverageKind: coverage.View,
	Desired:      func(desired *schemamodel.Database) []schemamodel.View { return desired.Views },
	Current:      func(current *catalog.Database) []catalog.View { return current.Views },
	Added:        func(diff *difftypes.SchemaDiff) *difftypes.ViewChanges { return &diff.ViewsAdded },
	Modified:     func(diff *difftypes.SchemaDiff) *[]difftypes.ViewDiff { return &diff.ViewsModified },
	Removed:      func(diff *difftypes.SchemaDiff) *difftypes.ViewChanges { return &diff.ViewsRemoved },
	Matched:      func(diff *difftypes.SchemaDiff) *int { return &diff.Matched.Views },

	DesiredName:          func(view schemamodel.View) string { return view.Name },
	CurrentName:          func(view catalog.View) string { return view.Name },
	CurrentSchema:        func(view catalog.View) string { return view.Schema },
	CurrentQualifiedName: catalog.View.QualifiedName,
	ModifiedName:         func(viewDiff difftypes.ViewDiff) string { return viewDiff.ViewName },

	Compare: ViewDefinitionsWithDialect,
	Changed: func(viewDiff difftypes.ViewDiff) bool { return len(viewDiff.Changes) > 0 },
	Carry:   viewFromCatalog,
}

var materializedViewFamily = viewLikeFamily[
	difftypes.MaterializedViewChanges, []difftypes.MaterializedViewDiff,
	schemamodel.MaterializedView, catalog.MaterializedView, difftypes.MaterializedViewDiff,
]{
	Kind:    objectidentity.KindMatView,
	Desired: func(desired *schemamodel.Database) []schemamodel.MaterializedView { return desired.MaterializedViews },
	Current: func(current *catalog.Database) []catalog.MaterializedView { return current.MatViews },
	Added: func(diff *difftypes.SchemaDiff) *difftypes.MaterializedViewChanges {
		return &diff.MaterializedViewsAdded
	},
	Modified: func(diff *difftypes.SchemaDiff) *[]difftypes.MaterializedViewDiff {
		return &diff.MaterializedViewsModified
	},
	Removed: func(diff *difftypes.SchemaDiff) *difftypes.MaterializedViewChanges {
		return &diff.MaterializedViewsRemoved
	},
	Matched: func(diff *difftypes.SchemaDiff) *int { return &diff.Matched.MaterializedViews },

	DesiredName:          func(view schemamodel.MaterializedView) string { return view.Name },
	CurrentName:          func(view catalog.MaterializedView) string { return view.Name },
	CurrentSchema:        func(view catalog.MaterializedView) string { return view.Schema },
	CurrentQualifiedName: catalog.MaterializedView.QualifiedName,
	ModifiedName:         func(viewDiff difftypes.MaterializedViewDiff) string { return viewDiff.ViewName },

	Compare: MaterializedViewDefinitionsWithDialect,
	Changed: func(viewDiff difftypes.MaterializedViewDiff) bool { return len(viewDiff.Changes) > 0 },
	Carry:   matViewFromCatalog,
}

// plainViewFamilyFor is the view family comparing bodies against desired's
// declared columns, so a declared `*` matches the columns a server stores in
// its place, and against spelled, the server's spelling of each declared body.
// A target's view rules, where it registers them, replace both.
//
// owners are the owners the read reported, which a declared owner is
// compared with.
func plainViewFamilyFor(
	desired *schemamodel.Database,
	spelled map[string]config.ViewBody,
	rules schemacomparison.Views,
	defaultSchema string,
	owners heldOwners,
) viewLikeFamily[
	difftypes.ViewChanges, []difftypes.ViewDiff,
	schemamodel.View, catalog.View, difftypes.ViewDiff,
] {
	family := plainViewFamily
	bodies := declaredBodies{columns: declaredRelationColumns(desired), spelled: spelled, rules: rules}
	family.Compare = func(genView schemamodel.View, dbView catalog.View, dialect string) difftypes.ViewDiff {
		// A dev-replayed MySQL view is unqualified when compared with a
		// target database, so its plan can move to that database. Its stored
		// body still qualifies relations. Use the session's default only
		// while comparing the body, not in the view's identity.
		if dbView.Schema == "" {
			dbView.Schema = defaultSchema
		}
		viewDiff := viewDefinitions(genView, dbView, dialect, bodies)
		viewDiff.PreviousOwner = owners.compare(viewDiff.Changes, objectowner.View, dbView.Schema, dbView.Name, genView.Owner)
		return viewDiff
	}
	return family
}

// materializedViewFamilyFor is [plainViewFamilyFor] for materialized views.
func materializedViewFamilyFor(
	desired *schemamodel.Database,
	spelled map[string]config.ViewBody,
	rules schemacomparison.Views,
	owners heldOwners,
) viewLikeFamily[
	difftypes.MaterializedViewChanges, []difftypes.MaterializedViewDiff,
	schemamodel.MaterializedView, catalog.MaterializedView, difftypes.MaterializedViewDiff,
] {
	family := materializedViewFamily
	bodies := declaredBodies{columns: declaredRelationColumns(desired), spelled: spelled, rules: rules}
	family.Compare = func(
		genView schemamodel.MaterializedView,
		dbView catalog.MaterializedView,
		dialect string,
	) difftypes.MaterializedViewDiff {
		viewDiff := materializedViewDefinitions(genView, dbView, dialect, bodies)
		viewDiff.PreviousOwner = owners.compare(viewDiff.Changes, objectowner.MaterializedView, dbView.Schema, dbView.Name, genView.Owner)
		return viewDiff
	}
	return family
}

// heldOwners are the owners a read reported for its views and materialized
// views. An object a read reports without a schema is in defaultSchema, and
// two role names are one role as dialect compares role names.
type heldOwners struct {
	observed      objectowner.Observed
	defaultSchema string
	dialect       string
}

// heldOwnersOf indexes the owners database reports, nil for none.
func heldOwnersOf(database *catalog.Database, defaultSchema, dialect string) heldOwners {
	if database == nil {
		return heldOwners{defaultSchema: defaultSchema, dialect: dialect}
	}
	return heldOwners{observed: objectowner.ObservedOwners(database.ObjectOwners), defaultSchema: defaultSchema, dialect: dialect}
}

// compare records in changes the owner change of an object of kind in schema
// named name whose declaration names declared, and returns the owner the read
// reported for it, empty where it reported none.
//
// A declaration that names no owner leaves the object's owner unmanaged, so
// nothing is recorded however the owner reads. One that names an owner the
// read does not report for the object cannot be confirmed as held, and is
// recorded too: the statement that gives the object to the declared role
// changes nothing where the role owns it already.
func (h heldOwners) compare(changes map[string]string, kind objectowner.Kind, schema, name, declared string) string {
	held, known := h.observed.Owner(kind, schema, name, h.defaultSchema)
	if declared == "" || (known && objectowner.SameRole(h.dialect, held, declared)) {
		return held
	}
	if !known {
		changes[difftypes.OwnerChange] = "(unknown) -> " + declared
		return held
	}
	changes[difftypes.OwnerChange] = held + " -> " + declared
	return held
}

// CurrentOwners is the owner database reports for each view and materialized
// view of a kind whose owner target manages ([objectowner.Manages]), in the
// order it reports them, for [difftypes.SchemaDiff.CurrentOwners]. An object
// of the schema the read names objects in is qualified by it.
//
// Only a target that manages an owner has a plan that gives one back, so the
// owners of any other target stay out of the diff. On a provider target they
// would reach a projection that refuses every field it has no wire form for,
// and refuse each plan made from a read that reports a view's owner.
func CurrentOwners(database *catalog.Database, target string) []difftypes.OwnerRef {
	if database == nil {
		return nil
	}
	managed := slices.DeleteFunc(objectowner.Kinds(), func(kind objectowner.Kind) bool { return !objectowner.Manages(target, kind) })
	var owners []difftypes.OwnerRef
	for _, row := range database.ObjectOwners {
		kind, ok := objectowner.KindOfCatalog(row.Kind)
		if !ok || row.Owner == "" || !slices.Contains(managed, kind) {
			continue
		}
		owners = append(owners, difftypes.OwnerRef{Kind: string(kind), Name: catalog.QualifyTableName(row.Schema, row.Name), Owner: row.Owner})
	}
	return owners
}

// Views compares view definitions between generated and database schemas.
func Views(desired *schemamodel.Database, current *catalog.Database, diff *difftypes.SchemaDiff) {
	ViewsWithDialect(desired, current, diff, "")
}

// ViewsWithDialect compares view definitions with dialect-aware normalization
// for catalog readback forms that are semantically equivalent to Ptah-rendered
// view SQL.
func ViewsWithDialect(desired *schemamodel.Database, current *catalog.Database, diff *difftypes.SchemaDiff, dialect string) {
	compareViewLikesByName(plainViewFamilyFor(desired, nil, nil, "", heldOwnersOf(current, "", dialect)), desired, current, diff, dialect)
}

// ViewsWithSemantics compares view identity with the live database's resolved
// default schema while retaining dialect-aware SQL-body normalization.
//
// spelled is [config.CompareOptions.ViewBodies]: a body the text comparison
// finds different still matches when the server's spelling of it equals the
// catalog's. Nil when no server was asked. rules are the target's view rules,
// nil where it registers none.
func ViewsWithSemantics(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	dialect string,
	semantics identifier.Semantics,
	spelled map[string]config.ViewBody,
	rules schemacomparison.Views,
) {
	bodySchema := semantics.Normalize(dialect).DefaultSchema
	compareViewLikesByIdentity(plainViewFamilyFor(desired, spelled, rules, bodySchema, heldOwnersOf(database, bodySchema, dialect)), desired, database, diff, dialect, semantics)
}

// MaterializedViews compares materialized view definitions between generated
// and database schemas.
func MaterializedViews(desired *schemamodel.Database, current *catalog.Database, diff *difftypes.SchemaDiff) {
	MaterializedViewsWithDialect(desired, current, diff, "")
}

// MaterializedViewsWithDialect compares materialized-view definitions with
// dialect-aware body normalization, matching identity the way
// [ViewsWithDialect] does.
//
// The two kinds have to agree about what a name means. A declaration that names
// its object without a schema is the ordinary spelling, and a catalog reports
// every object with one, so matching only on the qualified form makes an
// unchanged object BOTH added and removed. Measured through
// MaterializedViewsWithSemantics with a ClickHouse read, a declaration of
// "user_stats" against a database holding "ptah_test.user_stats":
//
//	MaterializedViewsAdded   = [user_stats]
//	MaterializedViewsRemoved = [ptah_test.user_stats]
//	MaterializedViewsModified = []
//
// The planner answers that with a CREATE before the removal, and ClickHouse
// refuses it -- "Table ... already exists. (TABLE_ALREADY_EXISTS)" -- while the
// plain view beside it, which matches bare names against a uniquely-named
// database view, reports nothing at all.
func MaterializedViewsWithDialect(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	dialect string,
) {
	compareViewLikesByName(materializedViewFamilyFor(desired, nil, nil, heldOwnersOf(database, "", dialect)), desired, database, diff, dialect)
}

// MaterializedViewsWithSemantics compares materialized-view identities using
// the same default-schema semantics as tables and ordinary views, and their
// bodies with the server's spelling as [ViewsWithSemantics] does.
func MaterializedViewsWithSemantics(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	dialect string,
	semantics identifier.Semantics,
	spelled map[string]config.ViewBody,
	rules schemacomparison.Views,
) {
	defaultSchema := semantics.Normalize(dialect).DefaultSchema
	compareViewLikesByIdentity(materializedViewFamilyFor(desired, spelled, rules, heldOwnersOf(database, defaultSchema, dialect)), desired, database, diff, dialect, semantics)
}

// compareViewLikesByName matches a declaration to a database row by its name:
// a qualified declaration matches only the object it names, and a bare one
// matches a database object of that name only when exactly one schema has it.
//
// Two declarations of one name are one object, and the last is the one that
// stands -- which is what a map keyed by name did before this walk was shared.
// The bare database name is read off the row rather than cut out of the
// qualified one: a qualified name is built by tableref, which quotes what needs
// quoting, and the last dot in `"my schema"."my view"` is not a separator.
func compareViewLikesByName[LA ~[]D, LM ~[]M, D, C, M any](
	family viewLikeFamily[LA, LM, D, C, M],
	desired *schemamodel.Database,
	current *catalog.Database,
	diff *difftypes.SchemaDiff,
	dialect string,
) {
	declarations, order := lastDeclarationPerName(family.Desired(desired), family.DesiredName)

	rows := family.Current(current)
	byName := make(map[string][]C, len(rows))
	byQualifiedName := make(map[string]C, len(rows))
	for _, row := range rows {
		byName[family.CurrentName(row)] = append(byName[family.CurrentName(row)], row)
		byQualifiedName[family.CurrentQualifiedName(row)] = row
	}

	matched := make(map[string]struct{}, len(rows))
	for _, name := range order {
		declaration := declarations[name]
		row, exists := findViewLikeRow(name, byName, byQualifiedName)
		if !exists {
			if !current.NotDescribed.Describes(family.CoverageKind, name) {
				continue
			}
			*family.Added(diff) = append(*family.Added(diff), declaration)
			continue
		}
		matched[family.CurrentQualifiedName(row)] = struct{}{}
		appendIfChanged(family, diff, declaration, row, dialect)
	}
	for _, row := range rows {
		if _, ok := matched[family.CurrentQualifiedName(row)]; ok {
			continue
		}
		if desired.NotDescribed.DescribesIn(family.CoverageKind, family.CurrentSchema(row), family.CurrentQualifiedName(row), family.CurrentName(row)) {
			*family.Removed(diff) = append(*family.Removed(diff), family.Carry(row, dialect))
		}
	}

	sortViewLikeAnswers(family, diff)
}

// compareViewLikesByIdentity matches on the identity the target's resolved
// default schema defines, so a declared bare name and a catalog row in the
// default schema are one object.
//
// A target with no default schema has no rule for which schema owns an
// unqualified name, and falls back to name matching. ClickHouse is the dialect
// that reaches that: its connection reports the current database as the schema
// on every object it reads and leaves DefaultSchema empty, so a declaration
// written "user_stats" and a readback of "<database>.user_stats" are the same
// object.
func compareViewLikesByIdentity[LA ~[]D, LM ~[]M, D, C, M any](
	family viewLikeFamily[LA, LM, D, C, M],
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	dialect string,
	semantics identifier.Semantics,
) {
	semantics = semantics.Normalize(dialect)
	if semantics.DefaultSchema == "" {
		compareViewLikesByName(family, desired, database, diff, dialect)
		return
	}

	declared := make(map[objectIdentity]D, len(family.Desired(desired)))
	for _, declaration := range family.Desired(desired) {
		identity := newQualifiedObjectIdentity(family.Kind, family.DesiredName(declaration), semantics)
		declared[identity] = declaration
	}
	reported := make(map[objectIdentity]C, len(family.Current(database)))
	for _, row := range family.Current(database) {
		identity := newObjectIdentity(family.Kind, family.CurrentSchema(row), family.CurrentName(row), semantics)
		reported[identity] = row
	}

	for identity, declaration := range declared {
		row, exists := reported[identity]
		if !exists {
			if !database.NotDescribed.DescribesIn(family.CoverageKind, semantics.DefaultSchema, family.DesiredName(declaration)) {
				continue
			}
			*family.Added(diff) = append(*family.Added(diff), declaration)
			continue
		}
		appendIfChanged(family, diff, declaration, row, dialect)
	}
	for identity, row := range reported {
		if _, exists := declared[identity]; !exists {
			if desired.NotDescribed.DescribesIn(family.CoverageKind, family.CurrentSchema(row), family.CurrentQualifiedName(row), family.CurrentName(row)) {
				*family.Removed(diff) = append(*family.Removed(diff), family.Carry(row, dialect))
			}
		}
	}

	sortViewLikeAnswers(family, diff)
}

// appendIfChanged counts a pair, and records its comparison only when it
// found something: a pair both sides hold alike is still held by both.
func appendIfChanged[LA ~[]D, LM ~[]M, D, C, M any](
	family viewLikeFamily[LA, LM, D, C, M],
	diff *difftypes.SchemaDiff,
	declaration D,
	row C,
	dialect string,
) {
	*family.Matched(diff)++
	if result := family.Compare(declaration, row, dialect); family.Changed(result) {
		*family.Modified(diff) = append(*family.Modified(diff), result)
	}
}

// sortViewLikeAnswers orders all three lists, which is what makes a walk over
// maps produce the same diff on every run.
func sortViewLikeAnswers[LA ~[]D, LM ~[]M, D, C, M any](
	family viewLikeFamily[LA, LM, D, C, M],
	diff *difftypes.SchemaDiff,
) {
	added, modified, removed := family.Added(diff), family.Modified(diff), family.Removed(diff)
	sort.Slice(*added, func(i, j int) bool {
		return family.DesiredName((*added)[i]) < family.DesiredName((*added)[j])
	})
	sort.Slice(*removed, func(i, j int) bool {
		return family.DesiredName((*removed)[i]) < family.DesiredName((*removed)[j])
	})
	sort.Slice(*modified, func(i, j int) bool {
		return family.ModifiedName((*modified)[i]) < family.ModifiedName((*modified)[j])
	})
}

// lastDeclarationPerName keeps one declaration per name, in declaration order.
func lastDeclarationPerName[D any](declarations []D, name func(D) string) (map[string]D, []string) {
	byName := make(map[string]D, len(declarations))
	order := make([]string, 0, len(declarations))
	for _, declaration := range declarations {
		key := name(declaration)
		if _, seen := byName[key]; !seen {
			order = append(order, key)
		}
		byName[key] = declaration
	}
	return byName, order
}

// findViewLikeRow is the name match both view kinds use, and deliberately one
// rule: a qualified declaration matches only the object it names, and a bare
// one matches a database object of that name only when exactly one schema has
// it. Two schemas holding the same name leave the declaration unmatched rather
// than guessing between them.
func findViewLikeRow[C any](
	declaredName string,
	byName map[string][]C,
	byQualifiedName map[string]C,
) (C, bool) {
	var absent C
	ref, ok := tableref.Parse(declaredName)
	if !ok {
		return absent, false
	}
	if ref.Qualified {
		row, ok := byQualifiedName[tableref.Canonical(ref.Schema, ref.Name)]
		return row, ok
	}
	candidates := byName[ref.Name]
	if len(candidates) != 1 {
		return absent, false
	}
	return candidates[0], true
}

// viewFromCatalog carries a view the database reported into the shape the diff
// holds.
//
// Two fields need a rule rather than a copy. The name is one: schemamodel.View
// has no Schema, because a declared schema is folded into the name, so the
// qualified spelling goes in Name -- which is what the name list held. A view
// the reader gave no schema keeps its bare name, because that is what the name
// list held for one.
//
// The check option is the other. The catalog reports a word and the model has
// two bools, and the rules are sqlutil.CheckOptionRequestsCheck and
// sqlutil.CheckOptionIsLocal, shared with the conversion path rather than
// spelled a second time here.
func viewFromCatalog(reported catalog.View, dialect string) schemamodel.View {
	name := spelledQualifiedName(reported.Schema, reported.Name, dialect)
	if reported.Schema == "" {
		name = reported.Name
	}
	return schemamodel.View{
		Name:       name,
		Body:       reported.Body,
		WithCheck:  sqlutil.CheckOptionRequestsCheck(reported.CheckOption),
		LocalCheck: sqlutil.CheckOptionIsLocal(reported.CheckOption),
		Comment:    reported.Comment,
		Attributes: reported.Attributes,
		Options:    slices.Clone(reported.Options),
	}
}

// matViewFromCatalog carries a materialized view the database reported into the
// shape the diff holds.
//
// Only the name needs a rule: schemamodel.MaterializedView has no Schema, so
// the qualified spelling goes in Name -- which is what the name list held. The
// observed facets are not carried: a declaration holds desired settings, and
// only an owner converts one into the other. A rollback that recreates the view
// takes it from the converted pre-change schema instead.
func matViewFromCatalog(reported catalog.MaterializedView, dialect string) schemamodel.MaterializedView {
	return schemamodel.MaterializedView{
		Name:    spelledQualifiedName(reported.Schema, reported.Name, dialect),
		Body:    reported.Body,
		Comment: reported.Comment,
		Options: slices.Clone(reported.Options),
	}
}
