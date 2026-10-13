package pgtable

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
)

// ExclusionChangeKind identifies a change of one EXCLUDE constraint.
const ExclusionChangeKind schemaext.Kind = "ptah.run/postgres/exclusion-change"

// ExclusionChange captures a creation, removal or replacement. A nil operand
// means absence. CommentOnly retains the constraint and changes its comment.
type ExclusionChange struct {
	Before      *ObservedExclusion `json:"before"`
	After       *DesiredExclusion  `json:"after"`
	CommentOnly bool               `json:"comment_only,omitempty"`
}

// Kind returns the change identity.
func (*ExclusionChange) Kind() schemaext.Kind { return ExclusionChangeKind }

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (c *ExclusionChange) CloneChange() schemaext.ChangeValue { return c.Copy() }

// Copy returns independent operands, or nil for a nil receiver.
func (c *ExclusionChange) Copy() *ExclusionChange {
	if c == nil {
		return nil
	}
	result := &ExclusionChange{CommentOnly: c.CommentOnly}
	if c.Before != nil {
		result.Before = copyObservedExclusion(c.Before)
	}
	if c.After != nil {
		result.After = copyDesiredExclusion(c.After)
	}
	return result
}

// RemovesState reports removal of the constraint. Replacing a definition keeps
// a constraint at this identity; an additive application may retain that change.
func (c *ExclusionChange) RemovesState() bool { return c != nil && c.After == nil }

// ValidateExclusionChange refuses absent operands, malformed definitions,
// unchanged pairs and an incorrect comment-only classification. Errors wrap
// schemaext.ErrInvalidValue. Dependency observations alone are not a SQL change.
func ValidateExclusionChange(c *ExclusionChange) error {
	if c == nil || c.Before == nil && c.After == nil {
		return exclusionModelError(schemaext.Change, "EXCLUDE change requires an operand")
	}
	if c.Before != nil {
		if err := ValidateObservedExclusion(c.Before); err != nil {
			return err
		}
	}
	if c.After != nil {
		if err := ValidateDesiredExclusion(c.After); err != nil {
			return err
		}
	}
	sameDefinition := c.Before != nil && c.After != nil && ExclusionComparisonKey(Exclusion(*c.Before)) == ExclusionComparisonKey(comparedExclusion(c.After))
	if sameDefinition && c.Before.Comment == c.After.Comment {
		return exclusionModelError(schemaext.Change, "EXCLUDE change requires different definitions or comments")
	}
	if c.CommentOnly != sameDefinition {
		return exclusionModelError(schemaext.Change, "EXCLUDE comment-only classification disagrees with its operands")
	}
	return nil
}

// Effect describes the constraint's effect on accepted rows. Invalid changes
// return no assessment. A comment changes documentation alone.
func (c *ExclusionChange) Effect() schemaext.Effect {
	if ValidateExclusionChange(c) != nil {
		return schemaext.Effect{}
	}
	if c.CommentOnly {
		return schemaext.Effect{Impact: schemaext.Additive, Reason: "changes an exclusion constraint comment"}
	}
	return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "changes which rows an exclusion constraint permits"}
}

// ExclusionChangeCodec returns an independent version-one change codec. Both
// operands are required, with null for absence; their own codecs validate the
// nested representations. Dependencies encode in identity order.
func ExclusionChangeCodec() schemaext.Codec {
	return schemaext.ModelCodec[*ExclusionChange]{
		Prototype: &ExclusionChange{}, Representation: schemaext.Change, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(`{"type":"object","required":["before","after"],"additionalProperties":false,"properties":`+
			`{"before":{"anyOf":[{"type":"null"},%s]},"after":{"anyOf":[{"type":"null"},%s]},"comment_only":{"const":true}}}`, exclusionDefinition(schemaext.Observed), exclusionDefinition(schemaext.Desired))),
		Shape: exclusionChangeShape, Validate: ValidateExclusionChange, Canonical: canonicalExclusionChange, Clone: (*ExclusionChange).Copy,
	}.Codec()
}

func exclusionChangeShape(data json.RawMessage) error {
	fields, err := schemaext.DecodeObject(data, schemaext.ObjectShape{Name: "EXCLUDE change", Allowed: []string{"before", "after", "comment_only"}, Required: []string{"before", "after"},
		Nullable: []string{"before", "after"}, NonEmpty: []string{"comment_only"}})
	if err != nil {
		return err
	}
	for _, codec := range ExclusionCodecs() {
		key := "after"
		if codec.Representation == schemaext.Observed {
			key = "before"
		}
		if string(fields[key]) == "null" {
			continue
		}
		if _, err := codec.Decode(fields[key]); err != nil {
			return err
		}
	}
	return nil
}

func canonicalExclusionChange(value *ExclusionChange) *ExclusionChange {
	result := value.Copy()
	if result.Before != nil {
		result.Before = canonicalObservedExclusion(result.Before)
	}
	if result.After != nil {
		result.After = canonicalDesiredExclusion(result.After)
	}
	return result
}
