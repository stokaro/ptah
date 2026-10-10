package mssqlpolicysource

import (
	"cmp"
	"fmt"
	"strconv"
	"strings"

	"ptah.run/core/annotation"
	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlschema"
)

// The frontend directives whose SQL Server-scoped declarations the owner
// reads.
const (
	policyDirective = "ptah:schema:rls:policy"
	enableDirective = "ptah:schema:rls:enable"
)

// Annotations is the SQL Server owner's contribution to the Go annotation
// frontend. It reads the frontend's rls:policy declarations scoped to SQL
// Server alone as security policies (see [Attributes.Policy]), refuses an
// rls:enable scoped to SQL Server, which has no switch on a table, and claims
// that a Go annotation source describes every security policy.
func Annotations() annotation.Extension {
	scope := func(directive string) annotation.TargetScope {
		return annotation.TargetScope{Directive: directive, Targets: []string{platform.SQLServer}, Label: "SQL Server"}
	}
	return annotation.Extension{
		Owner:        mssqlschema.Owner,
		Kinds:        []schemaext.Kind{mssqlschema.SecurityPolicyKind},
		TargetScopes: []annotation.TargetScope{scope(policyDirective), scope(enableDirective)},
		File:         func() annotation.FileDecoder { return &fileDecoder{} },
		Coverage: func([]coverage.Object) (schemaext.Coverage, error) {
			return mssqlschema.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)
		},
	}
}

// fileDecoder reads one file's SQL Server security policy declarations once
// the file's tables are known, since a declaration names its table by the
// struct it is written on or by a name the file may declare later.
type fileDecoder struct {
	policies []annotation.Declaration
}

// Decode keeps a policy for the end of the file, and refuses an enablement:
// a security policy carries its own state.
func (d *fileDecoder) Decode(declaration annotation.Declaration) ([]annotation.Contribution, error) {
	if declaration.Directive == enableDirective {
		return nil, &annotation.DeclarationError{Attribute: "dialects", Err: fmt.Errorf("%w: SQL Server has no row-level "+
			"security switch on a table; a security policy carries its own state, so remove the enablement scoped to %s",
			ptaherr.ErrInvalidAttributeValue, strings.Join(declaration.Targets, ","))}
	}
	d.policies = append(d.policies, declaration)
	return nil, nil
}

// Finish collects the file's policies. Declarations that name one policy are
// its predicates on several tables, and two that bind one slot are refused,
// naming both (see [Collector]).
func (d *fileDecoder) Finish(tables annotation.Tables) ([]annotation.Contribution, error) {
	var collector Collector
	sources := make(map[objectidentity.Key]annotation.Declaration)
	for _, declaration := range d.policies {
		kv := declaration.Attributes
		schemaName, tableName, err := tables.Reference(declaration.Struct, kv["table"], "a row-level security policy")
		if err != nil {
			return nil, &annotation.DeclarationError{Declaration: declaration, Err: err}
		}
		attributes := Attributes{
			Name: kv["name"], TableSchema: schemaName, Table: tableName, For: kv["for"], To: kv["to"],
			Using: kv["using"], WithCheck: kv["with_check"], Comment: kv["comment"], StructName: declaration.Struct,
			Restrictive: strings.EqualFold(strings.TrimSpace(kv["as"]), "RESTRICTIVE"),
		}
		if err := collector.Add(origin(declaration), attributes, declaration.Targets); err != nil {
			return nil, &annotation.DeclarationError{Declaration: declaration, Err: err}
		}
		if ref, err := attributes.ref(); err == nil {
			if _, seen := sources[ref.Key()]; !seen {
				sources[ref.Key()] = declaration
			}
		}
	}
	objects, err := collector.Objects()
	if err != nil {
		return nil, err
	}
	all, err := objects.All()
	if err != nil {
		return nil, err
	}
	contributions := make([]annotation.Contribution, 0, len(all))
	for _, object := range all {
		contributions = append(contributions, annotation.Contribution{Object: &object, Source: sources[object.Ref.Key()],
			Label: fmt.Sprintf("security policy %s.%s", object.Ref.Schema.Source, object.Ref.Name.Source)})
	}
	return contributions, nil
}

// origin names a declaration in a refusal that has to name two.
func origin(declaration annotation.Declaration) string {
	return "//" + declaration.Directive + " at " + cmp.Or(declaration.File, "the file") + ":" + strconv.Itoa(declaration.Line)
}
