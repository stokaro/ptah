package pgpolicysource

import (
	"cmp"
	"fmt"
	"strconv"
	"strings"

	"ptah.run/core/annotation"
	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
)

// The frontend directives whose PostgreSQL-scoped declarations the owner
// reads.
const (
	PolicyDirective = "ptah:schema:rls:policy"
	EnableDirective = "ptah:schema:rls:enable"
)

// Family are the targets whose row-level security a declaration scoped to
// them declares: the PostgreSQL family, as [platform.IsPostgresFamily] names
// it.
func Family() []string {
	return []string{platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner}
}

// Annotations is the row-security owner's contribution to the Go annotation
// frontend. It reads the frontend's rls:policy and rls:enable declarations
// that name no target, or only PostgreSQL-family targets, as policies and
// a table's switches, and claims that a Go annotation source describes both
// models completely: a policy the source leaves out is absent, and so is the
// row-level security of a table it declares no policy and no switch for.
func Annotations() annotation.Extension {
	scope := func(directive string) annotation.TargetScope {
		return annotation.TargetScope{Directive: directive, Targets: Family(), Unscoped: true, Label: "PostgreSQL-family targets"}
	}
	return annotation.Extension{
		Owner:        pgpolicy.Owner,
		Kinds:        []schemaext.Kind{pgpolicy.PolicyKind, pgpolicy.TableStateKind},
		TargetScopes: []annotation.TargetScope{scope(PolicyDirective), scope(EnableDirective)},
		File:         func() annotation.FileDecoder { return &fileDecoder{} },
		Coverage: func([]coverage.Object) (schemaext.Coverage, error) {
			return pgpolicy.CompleteCoverage(schemaext.Desired)
		},
	}
}

// fileDecoder reads one file's row-level security declarations once the
// file's tables are known, since a declaration names its table by the struct
// it is written on or by a name the file may declare later.
type fileDecoder struct {
	policies, switches []annotation.Declaration
	collector          Collector
}

func (d *fileDecoder) Decode(declaration annotation.Declaration) ([]annotation.Contribution, error) {
	if declaration.Directive == EnableDirective {
		d.switches = append(d.switches, declaration)
	} else {
		d.policies = append(d.policies, declaration)
	}
	return nil, nil
}

// Finish collects the policies, then the switches, in the order the file
// writes them, and refuses two declarations of one policy, or of one table's
// switches, naming both.
func (d *fileDecoder) Finish(tables annotation.Tables) ([]annotation.Contribution, error) {
	contributions := make([]annotation.Contribution, 0, len(d.policies)+len(d.switches))
	for _, declaration := range d.policies {
		contribution, err := d.policy(tables, declaration)
		if err != nil {
			return nil, &annotation.DeclarationError{Declaration: declaration, Err: err}
		}
		contributions = append(contributions, contribution)
	}
	for _, declaration := range d.switches {
		contribution, err := d.enable(tables, declaration)
		if err != nil {
			return nil, &annotation.DeclarationError{Declaration: declaration, Err: err}
		}
		contributions = append(contributions, contribution)
	}
	return contributions, nil
}

func (d *fileDecoder) policy(tables annotation.Tables, declaration annotation.Declaration) (annotation.Contribution, error) {
	schemaName, tableName, err := tables.Reference(declaration.Struct, declaration.Attributes["table"], "a row-level security policy")
	if err != nil {
		return annotation.Contribution{}, err
	}
	ref, policy, err := policyOf(declaration.Attributes, schemaName, tableName, declaration.Struct)
	if err != nil {
		return annotation.Contribution{}, err
	}
	if err := d.collector.AddPolicy(origin(declaration), ref, policy, declaration.Targets); err != nil {
		return annotation.Contribution{}, err
	}
	object, _, err := d.collector.Objects().Get(ref)
	if err != nil {
		return annotation.Contribution{}, err
	}
	return annotation.Contribution{Object: &object, Source: declaration,
		Label: fmt.Sprintf("policy %q on table %s", ref.Name.Source, tableText(pgpolicy.Table(ref)))}, nil
}

func (d *fileDecoder) enable(tables annotation.Tables, declaration annotation.Declaration) (annotation.Contribution, error) {
	kv := declaration.Attributes
	index, err := tables.Owning(declaration.Struct, kv["table"], "row-level security")
	if err != nil {
		return annotation.Contribution{}, err
	}
	table := tables[index]
	state := switchesOf(kv, table.Struct)
	if _, err := d.collector.AddSwitches(origin(declaration), TableRef(table.Schema, table.Name), schemaext.Facets{}, state,
		declaration.Targets); err != nil {
		return annotation.Contribution{}, err
	}
	return annotation.Contribution{Facet: &state, Table: kv["table"], Targets: declaration.Targets, Source: declaration,
		Label: "row-level security switches"}, nil
}

// Cover narrows the file's claim by the policies it declares: the switches
// of a table with policies are left to the owner's default (see [Claim]).
func (d *fileDecoder) Cover(claim schemaext.Coverage) (schemaext.Coverage, error) {
	return Claim(claim, d.collector.Objects())
}

// origin names a declaration in a refusal that has to name two.
func origin(declaration annotation.Declaration) string {
	return "//" + declaration.Directive + " at " + cmp.Or(declaration.File, "the file") + ":" + strconv.Itoa(declaration.Line)
}

// policyOf reads a row-level security policy declaration's attributes, keyed
// as the Go annotation keys them, on the table schemaName.tableName, as the
// policy and its identity. structName is the Go struct the declaration
// belongs to, if any. Both frontends read a policy through it, so one
// declaration decodes to one object from either.
func policyOf(attributes map[string]string, schemaName, tableName, structName string) (objectidentity.ID, pgpolicy.DesiredPolicy, error) {
	policy, err := Attributes{
		For: attributes["for"], To: attributes["to"], Using: attributes["using"], WithCheck: attributes["with_check"],
		Restrictive: strings.EqualFold(strings.TrimSpace(attributes["as"]), "RESTRICTIVE"), Comment: attributes["comment"],
		StructName: structName,
	}.Policy()
	if err != nil {
		return objectidentity.ID{}, pgpolicy.DesiredPolicy{}, err
	}
	return Ref(schemaName, tableName, attributes["name"]), policy, nil
}

// switchesOf reads an enablement's attributes, keyed as the Go annotation
// keys them, as the switches of the table structName maps to.
func switchesOf(attributes map[string]string, structName string) pgpolicy.DesiredTableState {
	return pgpolicy.DesiredTableState{Enabled: true, Forced: attributes["force"] == "true", Comment: attributes["comment"],
		StructName: structName}
}
