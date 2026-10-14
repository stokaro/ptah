package pgdefaultprivilegesource

import (
	"fmt"
	"strings"

	"ptah.run/core/annotation"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/internal/dialectscope"
)

// Annotations registers the owner's declaration grammar and export services.
// A document describes the default privileges it declares; omitted identities
// retain built-in defaults unless a managed grantor supplies removal authority.
func Annotations() annotation.Extension {
	return annotation.Extension{Owner: pgdefaultprivilege.Owner, Kinds: []schemaext.Kind{pgdefaultprivilege.DefaultPrivilegeKind},
		Directives: []annotation.Directive{{Name: Directive, Description: "Declares privileges on a named role's future objects.", Scopes: []annotation.Scope{annotation.ScopeStruct}, Attributes: []annotation.Attribute{
			{Name: "for_role", Description: "Role creating the objects.", Value: "string", Required: true},
			{Name: "schema", Description: "Schema of the objects; omitted for global defaults.", Value: "string"},
			{Name: "object_type", Description: "TABLES, SEQUENCES, FUNCTIONS or TYPES; globally also SCHEMAS or LARGE OBJECTS.", Value: "string", Required: true},
			{Name: "grantee", Description: "Role receiving privileges; PUBLIC selects every role.", Value: "string", Required: true},
			{Name: "privileges", Description: "Comma-separated granted privileges. Required unless revoked is set.", Value: "list"},
			{Name: "grantable", Description: "Subset of privileges carrying WITH GRANT OPTION.", Value: "list"},
			{Name: "revoked", Description: "Comma-separated revoked privileges; ALL selects every privilege of the object class.", Value: "list"},
			{Name: "comment", Description: "Documentation for this declaration.", Value: "string"},
			{Name: "dialects", Description: "Comma-separated target dialects for this declaration.", Value: "list"},
		}}}, Decode: decodeAnnotation, Coverage: sourceCoverage, Limits: []string{string(LimitKind)},
		Encoder: annotations{}, Retainer: annotations{}, Encodes: []schemaext.Kind{pgdefaultprivilege.DefaultPrivilegeKind},
	}
}

func completeDesired() (schemaext.Coverage, error) {
	return pgdefaultprivilege.CompleteCoverage(schemaext.Desired)
}

func decodeAnnotation(declaration annotation.Declaration) ([]annotation.Contribution, error) {
	object, err := annotationObject(declaration)
	if err != nil {
		return nil, &annotation.DeclarationError{Declaration: declaration, Err: err}
	}
	return []annotation.Contribution{{Object: &object, Source: declaration, Label: "default privilege " + object.Ref.String()}}, nil
}

func annotationObject(declaration annotation.Declaration) (schemaext.Object, error) {
	if declaration.Directive != Directive {
		return schemaext.Object{}, fmt.Errorf("%w: unexpected default privilege directive %q", schemaext.ErrInvalidValue, declaration.Directive)
	}
	attributes := declaration.Attributes
	targets := declaration.Targets
	if raw, present := attributes["dialects"]; present {
		var err error
		targets, err = dialectscope.Parse(raw)
		if err != nil {
			return schemaext.Object{}, &annotation.DeclarationError{Declaration: declaration, Attribute: "dialects", Err: err}
		}
	}
	return Declare(pgdefaultprivilege.Identity{Grantor: attributes["for_role"], Schema: attributes["schema"], ObjectType: attributes["object_type"], Grantee: attributes["grantee"]},
		strings.Split(attributes["privileges"], ","), strings.Split(attributes["grantable"], ","), strings.Split(attributes["revoked"], ","), attributes["comment"], targets)
}
