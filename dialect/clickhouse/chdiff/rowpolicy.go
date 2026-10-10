package chdiff

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/sqlident"
)

// RowPolicyKind identifies a change to one ClickHouse row policy.
const RowPolicyKind schemaext.Kind = "ptah.run/clickhouse/row-policy-change"

// RowPolicy captures one row policy's transition. A nil Before creates the
// policy and a nil After drops it; both together change it in place, because
// ALTER ROW POLICY sets the filter, the composition and the role selection of
// an existing policy. The policy's database, table and name are the change's
// subject.
//
// Access is the owner's assessment of what the transition does to the rows
// users may read, separate from the lifecycle effect. [NewRowPolicy] computes
// it; the change carries it as data, so it survives encoding.
type RowPolicy struct {
	Before *chschema.ObservedRowPolicy `json:"before"`
	After  *chschema.DesiredRowPolicy  `json:"after"`
	Access schemaext.AccessEffect      `json:"access"`
}

// NewRowPolicy builds a change between two states with its access
// assessment. It does not decide whether the states differ; a caller compares
// them first.
func NewRowPolicy(before *chschema.ObservedRowPolicy, after *chschema.DesiredRowPolicy) *RowPolicy {
	return &RowPolicy{Before: before, After: after, Access: AssessRowPolicyAccess(before, after)}
}

// AssessRowPolicyAccess is the owner's account of what a transition does to
// the rows users may read.
//
// ClickHouse shows a user the rows that pass at least one permissive policy
// applying to the user and every restrictive one; measured on 24.10 and 26.9,
// restrictive policies alone decide for a user no permissive policy names. A
// user no policy names is governed by the server's
// users_without_row_policies_can_read_rows setting, which is configuration and
// not readable from the policies. So creating, dropping or re-targeting a
// policy, and any filter change, has an effect that depends on the sibling
// policies and on that setting, and is reported as unknown.
//
// Two transitions are established from the combination rule alone, because
// they keep the policy's filter and the users it applies to: making a
// permissive policy restrictive moves its filter from an OR into an AND and
// can only narrow, and the reverse can only widen.
func AssessRowPolicyAccess(before *chschema.ObservedRowPolicy, after *chschema.DesiredRowPolicy) schemaext.AccessEffect {
	const depends = "whether that widens or narrows depends on the other policies on the table and on the server's " +
		"users_without_row_policies_can_read_rows setting"
	switch {
	case before == nil && after == nil:
		return schemaext.AccessEffect{}
	case before == nil:
		return schemaext.AccessEffect{Access: schemaext.AccessUnknown, Reason: "creating a row policy changes the rows its users see; " + depends}
	case after == nil:
		return schemaext.AccessEffect{Access: schemaext.AccessUnknown, Reason: "dropping a row policy changes the rows its users see; " + depends}
	}
	declared, err := after.Observed()
	if err != nil {
		return schemaext.AccessEffect{Access: schemaext.AccessUnknown, Reason: "the declared row policy is invalid, so its effect is not established"}
	}
	sameFilter := filterText(declared.Filter) == filterText(before.Filter)
	sameRoles := declared.Roles.Equal(before.Roles)
	switch {
	case sameFilter && sameRoles && before.Composition == chschema.Permissive && declared.Composition == chschema.Restrictive:
		return schemaext.AccessEffect{Access: schemaext.AccessNarrows,
			Reason: "a permissive policy made restrictive keeps its filter and users; its filter now restricts every other policy for them instead of adding rows"}
	case sameFilter && sameRoles && before.Composition == chschema.Restrictive && declared.Composition == chschema.Permissive:
		return schemaext.AccessEffect{Access: schemaext.AccessWidens,
			Reason: "a restrictive policy made permissive keeps its filter and users; its filter now adds rows for them instead of restricting every other policy"}
	}
	var changed []string
	if !sameFilter {
		changed = append(changed, "filter")
	}
	if before.Composition != declared.Composition {
		changed = append(changed, "composition")
	}
	if !sameRoles {
		changed = append(changed, "users and roles")
	}
	return schemaext.AccessEffect{Access: schemaext.AccessUnknown,
		Reason: "changing a row policy's " + strings.Join(changed, " and ") + " changes the rows its users see; " + depends}
}

func filterText(filter *string) string {
	if filter == nil {
		return ""
	}
	return *filter
}

// Kind returns the owned change identity.
func (*RowPolicy) Kind() schemaext.Kind { return RowPolicyKind }

// AccessEffect returns the assessment the change carries.
func (v *RowPolicy) AccessEffect() schemaext.AccessEffect {
	if v == nil {
		return schemaext.AccessEffect{}
	}
	return v.Access
}

// Effect reports the policy's lifecycle. A row policy holds no data: creating
// or changing one affects what readers see, and dropping one removes a
// protection. What either does to access is [RowPolicy.AccessEffect].
func (v *RowPolicy) Effect() schemaext.Effect {
	switch {
	case v == nil:
		return schemaext.Effect{}
	case v.Before == nil:
		return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "creating a row policy changes the rows its users read; it deletes no data"}
	case v.After == nil:
		return schemaext.Effect{Impact: schemaext.Destructive, Reason: "dropping a row policy removes a protection; it deletes no data"}
	default:
		return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "changing a row policy in place changes the rows its users read; it deletes no data"}
	}
}

// String describes the transition, such as
// `permissive TO alice USING tenant = 1 -> restrictive TO alice USING tenant = 1`,
// with `none` for an absent policy.
func (v *RowPolicy) String() string {
	before, after := "none", "none"
	if v != nil && v.Before != nil {
		before = describeRowPolicy(v.Before)
	}
	if v != nil && v.After != nil {
		if declared, err := v.After.Observed(); err == nil {
			after = describeRowPolicy(declared)
		}
	}
	return before + " -> " + after
}

func describeRowPolicy(policy *chschema.ObservedRowPolicy) string {
	parts := []string{string(policy.Composition), "TO " + RoleClause(policy.Roles)}
	if policy.Filter != nil {
		parts = append(parts, "USING "+*policy.Filter)
	}
	return strings.Join(parts, " ")
}

// RoleClause writes a selection as the text after TO, with every name in
// backticks: ALL, ALL EXCEPT followed by the exceptions, the names, or NONE for
// a selection that names nobody. Names keep their order.
func RoleClause(roles chschema.RoleSelection) string {
	switch {
	case roles.All && len(roles.Except) > 0:
		return "ALL EXCEPT " + quoteNames(roles.Except)
	case roles.All:
		return "ALL"
	case len(roles.Names) > 0:
		return quoteNames(roles.Names)
	default:
		return "NONE"
	}
}

func quoteNames(names []string) string {
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = sqlident.Quote(platform.ClickHouse, name)
	}
	return strings.Join(quoted, ", ")
}

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (v *RowPolicy) CloneChange() schemaext.ChangeValue { return v.Copy() }

// Copy is [RowPolicy.CloneChange] without the interface: it shares no operand
// with v, and a nil receiver returns nil.
func (v *RowPolicy) Copy() *RowPolicy {
	if v == nil {
		return nil
	}
	return &RowPolicy{Before: v.Before.Copy(), After: v.After.Copy(), Access: v.Access}
}

// ValidateRowPolicy requires at least one operand, validates each present one,
// and requires a valid access assessment. Invalid changes wrap
// schemaext.ErrInvalidValue.
func ValidateRowPolicy(v *RowPolicy) error {
	if v == nil || v.Before == nil && v.After == nil {
		return fmt.Errorf("%w: a ClickHouse row policy change requires a policy on at least one side", schemaext.ErrInvalidValue)
	}
	if v.Before != nil {
		if err := chschema.ValidateObservedRowPolicy(v.Before); err != nil {
			return err
		}
	}
	if v.After != nil {
		if err := chschema.ValidateDesiredRowPolicy(v.After); err != nil {
			return err
		}
	}
	if err := v.Access.Validate(); err != nil {
		return fmt.Errorf("ClickHouse row policy change: %w", err)
	}
	return nil
}

// RowPolicyCodecs returns the versioned row-policy change codec. Each operand
// uses the strict row policy wire model for its representation; an absent
// policy is null, and a change with null on both sides is refused. The access
// assessment is the record [schemaext.AccessEffectSchema] describes.
func RowPolicyCodecs() []schemaext.Codec {
	return []schemaext.Codec{{
		Prototype: &RowPolicy{}, Representation: schemaext.Change, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(`{"operands":%s,"access":%s,`+
			`"before":"observed row policy, or null for a policy that does not exist yet",`+
			`"after":"desired row policy, or null for a policy the change drops",`+
			`"constraint":"at least one side is non-null; access is required"}`, chschema.RowPolicyWireDefinition(), schemaext.AccessEffectSchema())),
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			v, err := rowPolicyChange(payload)
			if err != nil {
				return nil, err
			}
			return v.CloneChange(), nil
		},
		Encode: encodeRowPolicyChange, Canonical: encodeRowPolicyChange, Decode: decodeRowPolicyChange,
	}}
}

// rowPolicyChange returns payload as a valid row policy change.
func rowPolicyChange(payload schemaext.Payload) (*RowPolicy, error) {
	v, ok := payload.(*RowPolicy)
	if !ok {
		return nil, fmt.Errorf("%w: expected ClickHouse row policy change, got %T", schemaext.ErrInvalidValue, payload)
	}
	return v, ValidateRowPolicy(v)
}

// rowPolicyOperandCodecs returns the row policy codecs of the observed
// operand, before, and of the desired one, after.
func rowPolicyOperandCodecs() (observed, desired schemaext.Codec) {
	for _, codec := range chschema.RowPolicyCodecs() {
		if codec.Representation == schemaext.Observed {
			observed = codec
		} else {
			desired = codec
		}
	}
	return observed, desired
}

func encodeRowPolicyChange(payload schemaext.Payload) (json.RawMessage, error) {
	v, err := rowPolicyChange(payload)
	if err != nil {
		return nil, err
	}
	observed, desired := rowPolicyOperandCodecs()
	fields := map[string]json.RawMessage{"before": json.RawMessage("null"), "after": json.RawMessage("null")}
	if v.Before != nil {
		if fields["before"], err = observed.Canonical(v.Before); err != nil {
			return nil, err
		}
	}
	if v.After != nil {
		if fields["after"], err = desired.Canonical(v.After); err != nil {
			return nil, err
		}
	}
	if fields["access"], err = json.Marshal(v.Access); err != nil {
		return nil, err
	}
	return json.Marshal(fields)
}

func decodeRowPolicyChange(data json.RawMessage) (schemaext.Payload, error) {
	fields, err := schemaext.DecodeJSON[map[string]json.RawMessage](data)
	if err != nil {
		return nil, err
	}
	before, hasBefore := fields["before"]
	after, hasAfter := fields["after"]
	access, hasAccess := fields["access"]
	if len(fields) != 3 || !hasBefore || !hasAfter || !hasAccess {
		return nil, fmt.Errorf("%w: ClickHouse row policy change requires exactly before, after and access", schemaext.ErrInvalidValue)
	}
	result := &RowPolicy{}
	if err := json.Unmarshal(access, &result.Access); err != nil {
		return nil, err
	}
	observed, desired := rowPolicyOperandCodecs()
	if result.Before, err = decodeOperand[*chschema.ObservedRowPolicy](observed, before); err != nil {
		return nil, err
	}
	if result.After, err = decodeOperand[*chschema.DesiredRowPolicy](desired, after); err != nil {
		return nil, err
	}
	if err := ValidateRowPolicy(result); err != nil {
		return nil, err
	}
	return result, nil
}

// decodeOperand decodes one operand through its codec; null is an absent
// policy.
func decodeOperand[T schemaext.Value](codec schemaext.Codec, data json.RawMessage) (T, error) {
	var zero T
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return zero, nil
	}
	decoded, err := codec.Decode(data)
	if err != nil {
		return zero, err
	}
	typed, ok := decoded.(T)
	if !ok {
		return zero, fmt.Errorf("%w: ClickHouse row policy operand decoded as %T", schemaext.ErrInvalidValue, decoded)
	}
	return typed, nil
}
