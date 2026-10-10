package goschema

import (
	"fmt"
	"strconv"
	"strings"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/dialectscope"
	"ptah.run/internal/pgpolicysource"
)

// rlsPolicyDeclaration is one //ptah:schema:rls:policy annotation and where it
// was written. The fields are the annotation's own text until
// [schemaParseState.attachRowSecurity] decides which model holds it.
type rlsPolicyDeclaration struct {
	policy schemamodel.RLSPolicy
	ctx    annotationErrorContext
}

// rlsSwitchDeclaration is one //ptah:schema:rls:enable annotation and where it
// was written.
type rlsSwitchDeclaration struct {
	enabled schemamodel.RLSEnabledTable
	ctx     annotationErrorContext
}

// attachRowSecurity hands the file's row-level security annotations to the
// model that holds them, once every table in the file is known, and returns
// the ones the shared schema model keeps.
//
// A declaration with no dialect scope, or one scoped to PostgreSQL-family
// targets only, is the row-security owner's: a policy becomes an object, and
// an enablement the switches facet of its table. One scoped to other targets
// only stays a shared declaration, which SQL Server and ClickHouse plan. A
// scope naming both is refused (see [pgpolicysource.Owns]), and so are two
// annotations that declare one policy, or one table's switches, naming both
// (stokaro/ptah#2440).
func (s *schemaParseState) attachRowSecurity() ([]schemamodel.RLSPolicy, []schemamodel.RLSEnabledTable, error) {
	var collector pgpolicysource.Collector
	var policies []schemamodel.RLSPolicy
	for _, declared := range s.rlsPolicies {
		owned, err := s.ownedRowSecurity(declared.policy.Dialects, declared.ctx)
		if err != nil {
			return nil, nil, err
		}
		if !owned {
			policies = append(policies, declared.policy)
			continue
		}
		if err := s.collectPolicy(&collector, declared); err != nil {
			return nil, nil, err
		}
	}
	var switches []schemamodel.RLSEnabledTable
	for _, declared := range s.rlsEnabledTables {
		owned, err := s.ownedRowSecurity(declared.enabled.Dialects, declared.ctx)
		if err != nil {
			return nil, nil, err
		}
		if !owned {
			switches = append(switches, declared.enabled)
			continue
		}
		if err := s.collectSwitches(&collector, declared); err != nil {
			return nil, nil, err
		}
	}
	objects, err := s.featureObjects.Merge(collector.Objects())
	if err != nil {
		return nil, nil, err
	}
	s.featureObjects = objects
	s.featureCoverage, err = pgpolicysource.Claim(s.featureCoverage, s.featureObjects)
	return policies, switches, err
}

func (s *schemaParseState) ownedRowSecurity(scope []string, ctx annotationErrorContext) (bool, error) {
	owned, err := pgpolicysource.Owns(scope)
	if err != nil {
		return false, s.rowSecurityError(ctx, dialectscope.Attribute, err)
	}
	return owned, nil
}

func (s *schemaParseState) collectPolicy(collector *pgpolicysource.Collector, declared rlsPolicyDeclaration) error {
	schemaName, tableName, err := s.policyTable(declared)
	if err != nil {
		return err
	}
	written := declared.policy
	policy, err := pgpolicysource.Attributes{
		For: written.PolicyFor, To: written.ToRoles, Using: written.UsingExpression, WithCheck: written.WithCheckExpression,
		Restrictive: written.Restrictive, Comment: written.Comment, StructName: written.StructName,
	}.Policy()
	if err != nil {
		return s.rowSecurityError(declared.ctx, "", err)
	}
	ref := pgpolicysource.Ref(schemaName, tableName, written.Name)
	return collector.AddPolicy(rowSecurityOrigin(declared.ctx), ref, policy, written.Dialects)
}

// policyTable is the table a policy annotation is on: the file's table its
// table attribute names, or the one its struct maps to. A table attribute
// naming no table in this file is used as written, because a struct-attached
// policy may name a table another file declares.
func (s *schemaParseState) policyTable(declared rlsPolicyDeclaration) (schemaName, tableName string, err error) {
	written := declared.policy
	if written.Table == "" {
		index, ownerErr := s.ownerTable(written.StructName, "", declared.ctx, declared.ctx.directive, "a row-level security policy")
		if ownerErr != nil {
			return "", "", ownerErr
		}
		return s.tableDirectives[index].Schema, s.tableDirectives[index].Name, nil
	}
	schemaName, tableName = tableDirectiveName("", written.Table)
	var matches []schemamodel.Table
	for _, declared := range s.tableDirectives {
		if declared.Name == tableName && (schemaName == "" || declared.Schema == schemaName) {
			matches = append(matches, declared)
		}
	}
	if len(matches) == 1 {
		return matches[0].Schema, matches[0].Name, nil
	}
	if len(matches) == 0 {
		return schemaName, tableName, nil
	}
	_, err = s.ownerTable(written.StructName, written.Table, declared.ctx, declared.ctx.directive, "a row-level security policy")
	return "", "", err
}

func (s *schemaParseState) collectSwitches(collector *pgpolicysource.Collector, declared rlsSwitchDeclaration) error {
	written := declared.enabled
	index, err := s.ownerTable(written.StructName, written.Table, declared.ctx, declared.ctx.directive, "row-level security")
	if err != nil {
		return err
	}
	table := &s.tableDirectives[index]
	state := pgpolicy.DesiredTableState{Enabled: true, Forced: written.Forced, Comment: written.Comment, StructName: table.StructName}
	facets, err := collector.AddSwitches(rowSecurityOrigin(declared.ctx), pgpolicysource.TableRef(table.Schema, table.Name), table.Facets, state, written.Dialects)
	if err != nil {
		return err
	}
	table.Facets = facets
	return nil
}

// rowSecurityOrigin names an annotation in a refusal that has to name two.
func rowSecurityOrigin(ctx annotationErrorContext) string {
	return ctx.directive + " at " + ctx.file + ":" + strconv.Itoa(ctx.line)
}

func (s *schemaParseState) rowSecurityError(ctx annotationErrorContext, attribute string, err error) error {
	return &ptaherr.ParseError{
		File:      ctx.file,
		Line:      ctx.line,
		Directive: strings.TrimPrefix(ctx.directive, "//"),
		Attribute: attribute,
		Err:       fmt.Errorf("%w: %w", ptaherr.ErrInvalidAttributeValue, err),
		Message:   fmt.Sprintf("%s at %s: %v", ctx.directive, ctx.location, err),
	}
}
