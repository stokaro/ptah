package goschematogo

import (
	"fmt"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/pgpolicysource"
)

// captureSwitches validates the row-level security switches each table
// carries and writes the annotation the Go source reads back for a table whose
// row-level security is on. It records each table under the identity a
// policy names its table by, for [renderContext.capturePolicy]. Switches a Go
// annotation cannot say, and coverage the source could not describe, are
// refused rather than left out.
func (ctx *renderContext) captureSwitches() error {
	if err := pgpolicysource.RequireRepresentable(ctx.db.FeatureCoverage, ctx.db.FeatureObjects); err != nil {
		return err
	}
	builder := objectidentity.NewBuilder(identifier.ForDialect(platform.Postgres))
	ctx.policyTables = make(map[objectidentity.Key]string, len(ctx.db.Tables))
	ctx.policyAnnotations = make(map[string][]string)
	ctx.switchAnnotations = make(map[string]string)
	for _, table := range ctx.db.Tables {
		ctx.policyTables[builder.TableParts(table.Schema, table.Name).Key()] = table.QualifiedName()
		state, found, err := schemaext.FacetAs[*pgpolicy.DesiredTableState](table.Facets, pgpolicy.TableStateKind)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		if err := pgpolicy.ValidateDesiredTableState(state); err != nil {
			return err
		}
		if !state.Enabled && state.Forced {
			return fmt.Errorf("%w: Go annotations cannot force row-level security on table %s without enabling it",
				ptaherr.ErrUnsupportedFeature, table.QualifiedName())
		}
		if !state.Enabled {
			continue
		}
		ctx.switchAnnotations[table.QualifiedName()] = annotation("ptah:schema:rls:enable",
			attr{name: "table", value: table.QualifiedName(), set: true},
			attr{name: "force", value: "true", set: state.Forced},
			attr{name: "comment", value: state.Comment, set: state.Comment != ""},
			dialectsAttr(table.Facets.TargetScope(pgpolicy.TableStateKind)),
		)
	}
	return nil
}

// capturePolicy writes one policy as the annotation the Go source reads back,
// beside the table it belongs to. A policy whose table the export does not
// hold is refused.
func (ctx *renderContext) capturePolicy(object schemaext.Object, policy *pgpolicy.DesiredPolicy) error {
	if err := pgpolicy.ValidatePolicyRef(object.Ref); err != nil {
		return err
	}
	if err := pgpolicy.ValidateDesiredPolicy(policy); err != nil {
		return err
	}
	table, found := ctx.policyTables[pgpolicy.Table(object.Ref).Key()]
	if !found {
		return fmt.Errorf("%w: policy %s names a table the export does not declare", ptaherr.ErrUnsupportedFeature, object.Ref)
	}
	ctx.policyAnnotations[table] = append(ctx.policyAnnotations[table], annotation("ptah:schema:rls:policy",
		attr{name: "name", value: object.Ref.Name.Source, set: true},
		attr{name: "table", value: table, set: true},
		attr{name: "for", value: string(policy.Command), set: policy.Command != ""},
		attr{name: "to", value: pgpolicysource.FormatRoleList(policy.Roles), set: policy.Roles != nil},
		optionalAttr("using", policy.Using),
		optionalAttr("with_check", policy.WithCheck),
		attr{name: "as", value: strings.ToUpper(string(policy.Composition)), set: policy.Composition != ""},
		attr{name: "comment", value: policy.Comment, set: policy.Comment != ""},
		dialectsAttr(object.Targets),
	))
	return nil
}

// optionalAttr writes a clause a declaration may leave out.
func optionalAttr(name string, value *string) attr {
	if value == nil {
		return attr{name: name}
	}
	return attr{name: name, value: *value, set: true}
}
