package pgdefaultprivilegesource

import (
	"context"
	"fmt"

	"ptah.run/core/coverage"
	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgdefaultprivilege"
)

// HCL declares the unlabeled default_privilege block. Its attributes identify
// the grantor, schema, object class, and grantee. A missing schema is global.
// Repeated identities combine their grants and revokes through the owner.
func HCL() hclext.Extension {
	return hclext.Extension{
		Owner: pgdefaultprivilege.Owner,
		Kinds: []schemaext.Kind{pgdefaultprivilege.DefaultPrivilegeKind},
		Blocks: []hclext.Block{{Site: hclext.TopLevel, Type: "default_privilege", Naming: hclext.Unlabeled,
			Attributes: []hclext.Attribute{
				{Name: "for_role", Kind: hclext.Ref, Refers: "role"},
				{Name: "schema", Kind: hclext.Ref, Refers: "schema", Optional: true},
				{Name: "object_type", Kind: hclext.String},
				{Name: "to", Kind: hclext.Ref, Refers: "role"},
				{Name: "privileges", Kind: hclext.TextList},
				{Name: "grantable", Kind: hclext.TextList},
				{Name: "revoked", Kind: hclext.TextList},
				{Name: "comment", Kind: hclext.String},
			}}},
		Decoder: hclSource{}, Encoder: hclSource{},
		Header: []coverage.Kind{LimitKind}, Coverage: func(request hclext.CoverageRequest) (schemaext.Coverage, error) {
			return sourceCoverage(request.Limits)
		},
	}
}

type hclSource struct{}

func (hclSource) DecodeHCL(ctx context.Context, request hclext.Request) (hclext.Result, error) {
	if ctx == nil {
		return hclext.Result{}, fmt.Errorf("%w: default privilege decoding requires a context", schemaext.ErrInvalidValue)
	}
	var objects schemaext.Objects
	declared := make([]schemaext.Object, len(request.Claims))
	last := make(map[pgdefaultprivilege.Identity]int)
	for i, claim := range request.Claims {
		if err := ctx.Err(); err != nil {
			return hclext.Result{}, err
		}
		object, err := hclObject(claim)
		if err != nil {
			return hclext.Result{}, &hclext.ClaimError{Claim: i, Err: err}
		}
		objects, err = objects.Declare(object)
		if err != nil {
			return hclext.Result{}, &hclext.ClaimError{Claim: i, Err: err}
		}
		identity, err := pgdefaultprivilege.IdentityOf(object.Ref)
		if err != nil {
			return hclext.Result{}, &hclext.ClaimError{Claim: i, Err: err}
		}
		last[identity] = i
		declared[i] = object
	}
	result := hclext.Result{Declarations: make([]hclext.Declaration, len(request.Claims))}
	for _, i := range last {
		object, _, err := objects.Get(declared[i].Ref)
		if err != nil {
			return hclext.Result{}, err
		}
		result.Declarations[i].Objects = []schemaext.Object{object}
	}
	if err := ctx.Err(); err != nil {
		return hclext.Result{}, err
	}
	return result, nil
}

func hclObject(claim hclext.Claim) (schemaext.Object, error) {
	attributes := claim.Attributes
	for _, name := range []string{"for_role", "to"} {
		value := attributes[name]
		if value.Kind != hclext.Ref || value.Reference == nil || value.Reference.Kind != "role" || value.Reference.Schema != "" || value.Reference.Name == "" {
			return schemaext.Object{}, fmt.Errorf("%w: default_privilege requires %s", schemaext.ErrInvalidValue, name)
		}
	}
	identity := pgdefaultprivilege.Identity{Grantor: attributes["for_role"].Reference.Name,
		ObjectType: attributes["object_type"].Text, Grantee: attributes["to"].Reference.Name}
	if identity.ObjectType == "" {
		return schemaext.Object{}, fmt.Errorf("%w: default_privilege requires object_type", schemaext.ErrInvalidValue)
	}
	if schema, present := attributes["schema"]; present {
		if schema.Kind != hclext.Ref || schema.Reference == nil || schema.Reference.Kind != "schema" || schema.Reference.Name == "" {
			return schemaext.Object{}, fmt.Errorf("%w: default_privilege schema must name a schema", schemaext.ErrInvalidValue)
		}
		identity.Schema = schema.Reference.Name
	}
	return Declare(identity, attributes["privileges"].Texts, attributes["grantable"].Texts, attributes["revoked"].Texts, attributes["comment"].Text, nil)
}
