package goschema

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"ptah.run/core/annotation"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/dialectscope"
	"ptah.run/internal/mssqlpolicysource"
)

// rlsPolicyDeclaration is one //ptah:schema:rls:policy annotation and where it
// was written. The fields are the annotation's own text until
// [schemaParseState.attachRowSecurity] decides which model holds it.
type rlsPolicyDeclaration struct {
	policy     schemamodel.RLSPolicy
	attributes map[string]string
	ctx        annotationErrorContext
}

// rlsSwitchDeclaration is one //ptah:schema:rls:enable annotation and where it
// was written.
type rlsSwitchDeclaration struct {
	enabled    schemamodel.RLSEnabledTable
	attributes map[string]string
	ctx        annotationErrorContext
}

// attachRowSecurity hands the file's row-level security annotations to the
// model that holds them, once every table in the file is known, and returns
// the ones the shared schema model keeps. [schemaParseState.rowSecurityOwner]
// is the one place that decides which model a declaration belongs to.
//
// A declaration a selected owner reads by its target scope goes to that
// owner, as an owner's own directive does: without a scope, or scoped to
// PostgreSQL-family targets only, it is the row-security owner's, a policy
// becoming an object and an enablement the switches facet of its table. A
// policy scoped to SQL Server only is a security policy of the SQL Server
// owner (see [mssqlpolicysource.Attributes.Policy]); SQL Server has no table
// switch, so an enablement scoped to it is refused. One scoped to ClickHouse
// is refused too: a ClickHouse row policy is its owner's own directive,
// //ptah:schema:rowpolicy, and ClickHouse has no table switch. One scoped to
// other targets only stays a shared declaration. A scope naming two of these
// is refused, and so are two annotations that declare one policy, or one
// table's switches, naming both (stokaro/ptah#2440).
func (s *schemaParseState) attachRowSecurity() ([]schemamodel.RLSPolicy, []schemamodel.RLSEnabledTable, error) {
	var securityPolicies mssqlpolicysource.Collector
	var policies []schemamodel.RLSPolicy
	for _, declared := range s.rlsPolicies {
		owner, err := s.rowSecurityOwner(rlsPolicyDirective, declared.policy.Dialects, declared.ctx)
		if err != nil {
			return nil, nil, err
		}
		switch owner {
		case selectedRowSecurity:
			err = s.handToOwner(rlsPolicyDirective, declared.attributes, declared.policy.StructName, declared.policy.Dialects, declared.ctx)
		case sqlServerRowSecurity:
			err = s.collectSecurityPolicy(&securityPolicies, declared)
		case clickHouseRowSecurity:
			err = s.rowSecurityError(declared.ctx, dialectscope.Attribute, fmt.Errorf("%w: a ClickHouse row policy is not a "+
				"row-level security policy; declare it with //ptah:schema:rowpolicy instead", ptaherr.ErrInvalidAttributeValue))
		default:
			policies = append(policies, declared.policy)
		}
		if err != nil {
			return nil, nil, err
		}
	}
	var switches []schemamodel.RLSEnabledTable
	for _, declared := range s.rlsEnabledTables {
		owner, err := s.rowSecurityOwner(rlsEnableDirective, declared.enabled.Dialects, declared.ctx)
		if err != nil {
			return nil, nil, err
		}
		switch owner {
		case selectedRowSecurity:
			err = s.handToOwner(rlsEnableDirective, declared.attributes, declared.enabled.StructName, declared.enabled.Dialects, declared.ctx)
		case sqlServerRowSecurity:
			err = s.rowSecurityError(declared.ctx, dialectscope.Attribute, fmt.Errorf("%w: SQL Server has no row-level "+
				"security switch on a table; a security policy carries its own state, so remove the enablement scoped to %s",
				ptaherr.ErrInvalidAttributeValue, strings.Join(declared.enabled.Dialects, ",")))
		case clickHouseRowSecurity:
			err = s.rowSecurityError(declared.ctx, dialectscope.Attribute, fmt.Errorf("%w: ClickHouse has no row-level "+
				"security switch: a row policy filters rows once it exists; remove the enablement scoped to %s",
				ptaherr.ErrInvalidAttributeValue, strings.Join(declared.enabled.Dialects, ",")))
		default:
			switches = append(switches, declared.enabled)
		}
		if err != nil {
			return nil, nil, err
		}
	}
	owned, err := securityPolicies.Objects()
	if err != nil {
		return nil, nil, err
	}
	s.featureObjects, err = s.featureObjects.Merge(owned)
	return policies, switches, err
}

// The frontend's row-level security directives.
const (
	rlsPolicyDirective = "ptah:schema:rls:policy"
	rlsEnableDirective = "ptah:schema:rls:enable"
)

// rowSecurityOwner names the model a row-level security declaration belongs
// to.
type rowSecurityOwner int

const (
	sharedRowSecurity rowSecurityOwner = iota
	selectedRowSecurity
	sqlServerRowSecurity
	clickHouseRowSecurity
)

// rowSecurityOwner decides which model a declaration of directive scoped to
// scope belongs to: a selected owner that reads it by its target scope first,
// then the targets the frontend still decides for.
func (s *schemaParseState) rowSecurityOwner(directive string, scope []string, ctx annotationErrorContext) (rowSecurityOwner, error) {
	_, owned, err := s.annotations.TargetOwner(directive, scope)
	if err != nil {
		return sharedRowSecurity, s.rowSecurityError(ctx, dialectscope.Attribute, err)
	}
	if owned {
		return selectedRowSecurity, nil
	}
	sqlServer, err := mssqlpolicysource.Owns(scope)
	if err != nil {
		return sharedRowSecurity, s.rowSecurityError(ctx, dialectscope.Attribute, err)
	}
	if sqlServer {
		return sqlServerRowSecurity, nil
	}
	if slices.ContainsFunc(scope, func(target string) bool { return platform.NormalizeDialect(target) == platform.ClickHouse }) {
		return clickHouseRowSecurity, nil
	}
	return sharedRowSecurity, nil
}

// handToOwner hands a declaration a selected owner reads by its target scope
// to that owner, as [schemaParseState.parseOwnerDirective] hands one of the
// owner's own directives.
func (s *schemaParseState) handToOwner(directive string, attributes map[string]string, structName string, scope []string,
	ctx annotationErrorContext,
) error {
	declaration := annotation.Declaration{Directive: directive, Attributes: attributes, Struct: structName, Line: ctx.line,
		File: ctx.file, Targets: scope}
	contributions, err := s.owners.Decode(declaration)
	if err != nil {
		return s.ownerError(declaration, err)
	}
	return s.contribute(declaration, contributions)
}

// collectSecurityPolicy collects a policy scoped to SQL Server as a security
// policy on its table.
func (s *schemaParseState) collectSecurityPolicy(collector *mssqlpolicysource.Collector, declared rlsPolicyDeclaration) error {
	schemaName, tableName, err := s.policyTable(declared)
	if err != nil {
		return err
	}
	written := declared.policy
	err = collector.Add(rowSecurityOrigin(declared.ctx), mssqlpolicysource.Attributes{
		Name: written.Name, TableSchema: schemaName, Table: tableName, For: written.PolicyFor, To: written.ToRoles,
		Using: written.UsingExpression, WithCheck: written.WithCheckExpression, Restrictive: written.Restrictive,
		Comment: written.Comment, StructName: written.StructName,
	}, written.Dialects)
	if err != nil {
		return s.rowSecurityError(declared.ctx, "", err)
	}
	return nil
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
