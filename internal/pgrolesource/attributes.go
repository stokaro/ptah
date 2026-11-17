package pgrolesource

import (
	"context"
	"fmt"

	"ptah.run/core/annotation"
	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
	"ptah.run/dialect/postgres/pgrole"
)

// RoleDirective is the Go directive that declares a role, the one the
// attributes join.
const RoleDirective = "ptah:schema:role"

// Annotations registers the two attributes on the role directive and the
// encoder that writes them back.
func Annotations() annotation.Extension {
	return annotation.Extension{
		Owner: pgrole.Owner, Kinds: []schemaext.Kind{pgrole.SettingsKind},
		Attributes: []annotation.DirectiveAttributes{{Directive: RoleDirective, Attributes: []annotation.Attribute{
			{Name: SettingsAttribute, Value: "string", Description: "PostgreSQL configuration defaults the role logs in with, " +
				"`name = value` separated by semicolons, each value written in SQL: \"statement_timeout = '30s'; search_path = '$user', public\". " +
				"A setting the role holds and this list leaves out stays as it is."},
			{Name: ResetAttribute, Value: "list", Description: "Comma-separated PostgreSQL settings to take away from the role " +
				"with ALTER ROLE ... RESET, such as \"work_mem, app.tenant\"."},
		}, Decode: Decode}},
		Coverage: annotation.Unlimited(Coverage), Encoder: annotationEncoder{}, Encodes: []schemaext.Kind{pgrole.SettingsKind},
	}
}

// YAML registers the same keys, with the same values, on role entries.
func YAML() yamlext.Extension {
	return yamlext.Extension{Owner: pgrole.Owner, Kinds: []schemaext.Kind{pgrole.SettingsKind},
		EntryAttributes: []yamlext.EntryAttributes{{Entry: yamlext.EntryRole, Attributes: []string{SettingsAttribute, ResetAttribute}, Decode: Decode}},
		Coverage:        Coverage}
}

type annotationEncoder struct{}

func (annotationEncoder) EncodeAnnotations(ctx context.Context, request annotation.EncodeRequest) (annotation.EncodeResult, error) {
	result := annotation.EncodeResult{}
	for _, slot := range request.Slots {
		if err := ctx.Err(); err != nil {
			return annotation.EncodeResult{}, err
		}
		if slot.Slot.Directive != RoleDirective {
			return annotation.EncodeResult{}, fmt.Errorf("%w: role settings on %s", schemaext.ErrInvalidValue, slot.Slot)
		}
		settings, resets, err := Encode(slot.Facets)
		if err != nil {
			return annotation.EncodeResult{}, err
		}
		var attributes []annotation.EncodedAttribute
		if settings != "" {
			attributes = append(attributes, annotation.EncodedAttribute{Name: SettingsAttribute, Value: settings})
		}
		if resets != "" {
			attributes = append(attributes, annotation.EncodedAttribute{Name: ResetAttribute, Value: resets})
		}
		result.Attributes = append(result.Attributes, annotation.SlotAttributes{Slot: slot.Slot, Attributes: attributes})
	}
	return result, ctx.Err()
}
