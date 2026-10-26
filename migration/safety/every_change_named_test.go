package safety_test

import (
	"cmp"
	"maps"
	"reflect"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/difftypes"
	"ptah.run/migration/safety"
)

// notChanges names the SchemaDiff fields that can hold an entry and are not a
// change a plan carries out, each with the reason no category counts it. A
// field missing from here that a diff can hold an entry in has to make
// ClassifySchemaDiff report a finding, or a drift report says drift and names
// nothing.
var notChanges = map[string]string{
	"TablePreparation":             "the comparison's own inputs, which a reversal keeps for provenance",
	"SkippedFeatures":              "objects the target does not serve, which a report names beside its result",
	"IdentifierSemantics":          "the identifier rules the comparison ran under",
	"RLSPolicyIdentityConflicts":   "two declared policies with one identity, a defect in the declaration a planner refuses",
	"DeclaredTables":               "a planner input off the wire: every table the declaration holds",
	"DeclaredSchemas":              "a planner input off the wire: every schema the declaration holds",
	"DeclaredForeignKeys":          "a planner input off the wire: every foreign key of the schema the plan runs against",
	"DeclaredConstraintHosts":      "a planner input off the wire: the tables a constraint change names",
	"ObservedConstraintHosts":      "a planner input off the wire: the observed side of those tables",
	"DeclaredTableDependencies":    "a planner input off the wire: the table dependency graph",
	"CurrentGrants":                "a planner input off the wire: every grant the read reported, changed or not",
	"DeclaredIndexes":              "a planner input off the wire: every index the declaration holds, which a view rebuild puts back",
	"DeclaredTriggers":             "a planner input off the wire: every trigger the declaration holds, which a view rebuild puts back",
	"RoutinesKept":                 "a planner input off the wire: the declared routines the database holds unchanged",
	"ForeignKeysRemovedWithTables": "a supplement of constraints_removed, which counts the removal itself",
}

// notTableChanges and notEnumChanges are the same judgment for the fields of a
// modified table and a modified enum type.
var (
	notTableChanges = map[string]string{
		"ColumnKeyNames": "a planner input off the wire: the name a column's new key takes",
	}
	notEnumChanges = map[string]string{
		"Usages": "a planner input off the wire: the columns a value removal converts",
	}
)

// changeSlot is one place a diff can hold a change, and a diff that holds one
// entry there and nothing else.
type changeSlot struct {
	name string
	diff *difftypes.SchemaDiff
}

// changeSlots walks every SchemaDiff field that can hold an entry and is not
// in notChanges. A modified table or enum type is counted by what changed in
// it, so TablesModified and EnumsModified are walked through their own
// fields: each slot is one modified table, or enum type, holding one entry in
// one field.
func changeSlots() []changeSlot {
	var slots []changeSlot
	for field := range reflect.TypeFor[difftypes.SchemaDiff]().Fields() {
		if _, exempt := notChanges[field.Name]; exempt || !holdsEntries(field) {
			continue
		}
		switch field.Name {
		case "TablesModified":
			for name, table := range memberSlots[difftypes.TableDiff](notTableChanges) {
				slots = append(slots, changeSlot{name: field.Name + "." + name, diff: &difftypes.SchemaDiff{
					TablesModified: []difftypes.TableDiff{table},
				}})
			}
		case "EnumsModified":
			for name, enum := range memberSlots[difftypes.EnumDiff](notEnumChanges) {
				slots = append(slots, changeSlot{name: field.Name + "." + name, diff: &difftypes.SchemaDiff{
					EnumsModified: []difftypes.EnumDiff{enum},
				}})
			}
		default:
			diff := &difftypes.SchemaDiff{}
			setOneEntry(reflect.ValueOf(diff).Elem().FieldByIndex(field.Index))
			slots = append(slots, changeSlot{name: field.Name, diff: diff})
		}
	}
	slices.SortFunc(slots, func(a, b changeSlot) int { return cmp.Compare(a.name, b.name) })
	return slots
}

// memberSlots is one T per field of T that can hold an entry and is not in
// exempt, keyed by the field's name, each holding one entry in that field.
func memberSlots[T any](exempt map[string]string) map[string]T {
	slots := make(map[string]T)
	for field := range reflect.TypeFor[T]().Fields() {
		if _, skip := exempt[field.Name]; skip || !holdsEntries(field) {
			continue
		}
		var member T
		setOneEntry(reflect.ValueOf(&member).Elem().FieldByIndex(field.Index))
		slots[field.Name] = member
	}
	return slots
}

// holdsEntries reports whether a field can hold an entry of its own: a list,
// a map or a pointer. A struct or a scalar holds a value, not an entry.
func holdsEntries(field reflect.StructField) bool {
	switch field.Type.Kind() {
	case reflect.Slice, reflect.Map, reflect.Pointer:
		return field.IsExported()
	default:
		return false
	}
}

// setOneEntry puts one zero entry in a list, a map or a pointer. The entry's
// contents do not matter to the property the walk holds: that a change in
// this place is counted at all.
func setOneEntry(value reflect.Value) {
	switch value.Kind() {
	case reflect.Slice:
		value.Set(reflect.MakeSlice(value.Type(), 1, 1))
	case reflect.Map:
		entries := reflect.MakeMapWithSize(value.Type(), 1)
		entries.SetMapIndex(reflect.Zero(value.Type().Key()), reflect.Zero(value.Type().Elem()))
		value.Set(entries)
	case reflect.Pointer:
		value.Set(reflect.New(value.Type().Elem()))
	}
}

// unnamedChanges is the walk: the slots for which classify reports no
// finding, and how many slots it walked.
func unnamedChanges(classify func(*difftypes.SchemaDiff) []safety.Finding) (unnamed []string, walked int) {
	unnamed = make([]string, 0)
	slots := changeSlots()
	for _, slot := range slots {
		if len(classify(slot.diff)) == 0 {
			unnamed = append(unnamed, slot.name)
		}
	}
	return unnamed, len(slots)
}

// TestClassifySchemaDiff_NamesEveryChange is the walk over every place a diff
// holds a change. The places come from reflection over the diff, not from a
// list kept here, so a list added to SchemaDiff that no category counts fails
// this test until it is counted or written into notChanges with a reason.
func TestClassifySchemaDiff_NamesEveryChange(t *testing.T) {
	c := qt.New(t)

	unnamed, walked := unnamedChanges(safety.ClassifySchemaDiff)

	c.Assert(walked > 50, qt.IsTrue, qt.Commentf("the walk reached %d places; reflection is not seeing SchemaDiff", walked))
	c.Assert(unnamed, qt.HasLen, 0)
}

// TestClassifySchemaDiff_TheWalkRefusesAnUncountedChange is the walk's
// rejection control: a classifier that does not read sequence additions, and
// one that does not read a table's comment, each leave the one place the
// walk names. Without it the walk could pass by building diffs nothing reads.
func TestClassifySchemaDiff_TheWalkRefusesAnUncountedChange(t *testing.T) {
	tests := []struct {
		name     string
		classify func(*difftypes.SchemaDiff) []safety.Finding
		want     []string
	}{
		{
			name: "a list of the diff",
			classify: func(diff *difftypes.SchemaDiff) []safety.Finding {
				blind := *diff
				blind.SequencesAdded = nil
				return safety.ClassifySchemaDiff(&blind)
			},
			want: []string{"SequencesAdded"},
		},
		{
			name: "a field of a modified table",
			classify: func(diff *difftypes.SchemaDiff) []safety.Finding {
				blind := *diff
				blind.TablesModified = slices.Clone(diff.TablesModified)
				for i := range blind.TablesModified {
					blind.TablesModified[i].CommentChange = nil
				}
				return safety.ClassifySchemaDiff(&blind)
			},
			want: []string{"TablesModified.CommentChange"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			unnamed, _ := unnamedChanges(test.classify)

			c.Assert(unnamed, qt.DeepEquals, test.want)
		})
	}
}

// TestClassifySchemaDiff_TheExemptFieldsAreNotChanges holds notChanges to its
// reasons: a diff holding an entry in an exempt field and nothing else has no
// change, so no drift report reads it as drift. An exemption that names a
// change fails here rather than hiding one from the walk.
func TestClassifySchemaDiff_TheExemptFieldsAreNotChanges(t *testing.T) {
	c := qt.New(t)
	c.Assert(notChanges, qt.Not(qt.HasLen), 0)

	for _, name := range slices.Sorted(maps.Keys(notChanges)) {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			field, exists := reflect.TypeFor[difftypes.SchemaDiff]().FieldByName(name)
			c.Assert(exists, qt.IsTrue, qt.Commentf("SchemaDiff has no field %s; the reason outlived it", name))
			diff := &difftypes.SchemaDiff{}

			setOneEntry(reflect.ValueOf(diff).Elem().FieldByIndex(field.Index))

			c.Assert(diff.HasChanges(), qt.IsFalse)
		})
	}
}

// TestClassifySchemaDiff_TheExemptMembersAreOffTheWire holds the exemptions
// of a modified table and enum type to their reasons. Their container is a
// change whatever they hold, so HasChanges cannot answer for them; what makes
// one a planner input rather than a change is that the report does not carry
// it.
func TestClassifySchemaDiff_TheExemptMembersAreOffTheWire(t *testing.T) {
	tests := []struct {
		name   string
		member reflect.Type
		exempt map[string]string
	}{
		{name: "TableDiff", member: reflect.TypeFor[difftypes.TableDiff](), exempt: notTableChanges},
		{name: "EnumDiff", member: reflect.TypeFor[difftypes.EnumDiff](), exempt: notEnumChanges},
	}

	for _, test := range tests {
		for _, name := range slices.Sorted(maps.Keys(test.exempt)) {
			t.Run(test.name+"."+name, func(t *testing.T) {
				c := qt.New(t)

				field, exists := test.member.FieldByName(name)

				c.Assert(exists, qt.IsTrue, qt.Commentf("%s has no field %s; the reason outlived it", test.name, name))
				c.Assert(field.Tag.Get("json"), qt.Equals, "-")
			})
		}
	}
}

// TestClassifySchemaDiff_NamesEveryKindOfChange pins the category and the
// severity of each change a diff holds outside a table, an enum type, an
// index's or a constraint's definition, a function, a role and row-level
// security, which have tests of their own. Each row is one change.
func TestClassifySchemaDiff_NamesEveryKindOfChange(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want safety.Finding
	}{
		{
			name: "a sequence the database lacks",
			diff: &difftypes.SchemaDiff{SequencesAdded: difftypes.SequenceChanges{{Name: "order_seq"}}},
			want: safety.Finding{Category: "sequences_added", Count: 1, Severity: safety.Safe},
		},
		{
			name: "a sequence the declaration lacks",
			diff: &difftypes.SchemaDiff{SequencesRemoved: difftypes.SequenceChanges{{Name: "order_seq"}}},
			want: safety.Finding{Category: "sequences_removed", Count: 1, Severity: safety.Destructive},
		},
		{
			name: "a sequence whose options differ",
			diff: &difftypes.SchemaDiff{SequencesModified: []difftypes.SequenceDiff{{SequenceName: "order_seq"}}},
			want: safety.Finding{Category: "sequences_modified", Count: 1, Severity: safety.Warning},
		},
		{
			name: "a view the database lacks",
			diff: &difftypes.SchemaDiff{ViewsAdded: difftypes.ViewChanges{{Name: "active_users"}}},
			want: safety.Finding{Category: "views_added", Count: 1, Severity: safety.Safe},
		},
		{
			name: "a view the declaration lacks",
			diff: &difftypes.SchemaDiff{ViewsRemoved: difftypes.ViewChanges{{Name: "active_users"}}},
			want: safety.Finding{Category: "views_removed", Count: 1, Severity: safety.Destructive},
		},
		{
			name: "a view whose body differs",
			diff: &difftypes.SchemaDiff{ViewsModified: []difftypes.ViewDiff{{ViewName: "active_users"}}},
			want: safety.Finding{Category: "views_modified", Count: 1, Severity: safety.Warning},
		},
		{
			name: "a materialized view the database lacks",
			diff: &difftypes.SchemaDiff{MaterializedViewsAdded: difftypes.MaterializedViewChanges{{Name: "daily"}}},
			want: safety.Finding{Category: "materialized_views_added", Count: 1, Severity: safety.Safe},
		},
		{
			name: "a materialized view the declaration lacks",
			diff: &difftypes.SchemaDiff{MaterializedViewsRemoved: difftypes.MaterializedViewChanges{{Name: "daily"}}},
			want: safety.Finding{Category: "materialized_views_removed", Count: 1, Severity: safety.Destructive},
		},
		{
			name: "a materialized view the plan replaces",
			diff: &difftypes.SchemaDiff{MaterializedViewsModified: []difftypes.MaterializedViewDiff{
				{ViewName: "daily", Changes: map[string]string{"body": "SELECT 1 -> SELECT 2"}},
			}},
			want: safety.Finding{Category: "materialized_views_modified", Count: 1, Severity: safety.Destructive},
		},
		{
			name: "a trigger the database lacks",
			diff: &difftypes.SchemaDiff{TriggersAdded: []difftypes.TriggerRef{{TriggerName: "audit", TableName: "users"}}},
			want: safety.Finding{Category: "triggers_added", Count: 1, Severity: safety.Safe},
		},
		{
			name: "a trigger the declaration lacks",
			diff: &difftypes.SchemaDiff{TriggersRemoved: []difftypes.TriggerRef{{TriggerName: "audit", TableName: "users"}}},
			want: safety.Finding{Category: "triggers_removed", Count: 1, Severity: safety.Destructive},
		},
		{
			name: "a trigger whose definition differs",
			diff: &difftypes.SchemaDiff{TriggersModified: []difftypes.TriggerDiff{{TriggerName: "audit", TableName: "users"}}},
			want: safety.Finding{Category: "triggers_modified", Count: 1, Severity: safety.Warning},
		},
		{
			name: "a privilege the database does not grant",
			diff: &difftypes.SchemaDiff{GrantsAdded: []difftypes.GrantRef{
				{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "users"},
			}},
			want: safety.Finding{Category: "grants_added", Count: 1, Severity: safety.Safe},
		},
		{
			name: "a privilege the declaration does not grant",
			diff: &difftypes.SchemaDiff{GrantsRemoved: []difftypes.GrantRef{
				{Role: "reader", Privilege: "DELETE", ObjectType: "TABLE", ObjectName: "users"},
			}},
			want: safety.Finding{Category: "grants_removed", Count: 1, Severity: safety.Warning},
		},
		{
			name: "a grant option the database lacks",
			diff: &difftypes.SchemaDiff{GrantOptionsAdded: []difftypes.GrantRef{
				{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "users", WithOption: true},
			}},
			want: safety.Finding{Category: "grant_options_added", Count: 1, Severity: safety.Safe},
		},
		{
			name: "a grant option the declaration lacks",
			diff: &difftypes.SchemaDiff{GrantOptionsRevoked: []difftypes.GrantRef{
				{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "users", WithOption: true},
			}},
			want: safety.Finding{Category: "grant_options_removed", Count: 1, Severity: safety.Warning},
		},
		{
			name: "a membership the database lacks",
			diff: &difftypes.SchemaDiff{RoleMembershipsAdded: []difftypes.RoleMembershipRef{{Role: "readers", Member: "app"}}},
			want: safety.Finding{Category: "role_memberships_added", Count: 1, Severity: safety.Safe},
		},
		{
			name: "a membership the declaration lacks",
			diff: &difftypes.SchemaDiff{RoleMembershipsRemoved: []difftypes.RoleMembershipRef{{Role: "readers", Member: "app"}}},
			want: safety.Finding{Category: "role_memberships_removed", Count: 1, Severity: safety.Warning},
		},
		{
			name: "an index the plan renames",
			diff: &difftypes.SchemaDiff{IndexesRenamed: []difftypes.IndexRename{{TableName: "users", From: "old_a", To: "by_a"}}},
			want: safety.Finding{Category: "indexes_renamed", Count: 1, Severity: safety.Warning},
		},
		{
			name: "an index the optimizer stops using",
			diff: &difftypes.SchemaDiff{IndexVisibilityChanged: []difftypes.IndexVisibilityChange{{}}},
			want: safety.Finding{Category: "index_visibility_changed", Count: 1, Severity: safety.Warning},
		},
		{
			name: "a constraint the database holds NOT VALID",
			diff: &difftypes.SchemaDiff{ConstraintsValidated: []difftypes.ConstraintValidation{{TableName: "users", Name: "chk_age"}}},
			want: safety.Finding{Category: "constraints_validated", Count: 1, Severity: safety.Warning},
		},
		{
			name: "a table's comment",
			diff: &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{
				{TableName: "users", CommentChange: &difftypes.CommentChange{Desired: "people"}},
			}},
			want: safety.Finding{Category: "comments_changed", Count: 1, Severity: safety.Safe},
		},
		{
			name: "a view's comment",
			diff: &difftypes.SchemaDiff{ObjectCommentsChanged: []difftypes.ObjectCommentChange{
				{Kind: difftypes.CommentedView, Name: "active_users", Desired: "who logged in"},
			}},
			want: safety.Finding{Category: "comments_changed", Count: 1, Severity: safety.Safe},
		},
		{
			name: "an index's comment",
			diff: &difftypes.SchemaDiff{IndexCommentsChanged: []difftypes.IndexCommentChange{{TableName: "users"}}},
			want: safety.Finding{Category: "comments_changed", Count: 1, Severity: safety.Safe},
		},
		{
			name: "a constraint's comment",
			diff: &difftypes.SchemaDiff{ConstraintCommentsChanged: []difftypes.ConstraintCommentChange{
				{TableName: "users", Name: "chk_age", Desired: "adults"},
			}},
			want: safety.Finding{Category: "comments_changed", Count: 1, Severity: safety.Safe},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			findings := safety.ClassifySchemaDiff(test.diff)

			c.Assert(findings, qt.DeepEquals, []safety.Finding{test.want})
		})
	}
}
