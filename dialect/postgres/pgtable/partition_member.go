package pgtable

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// PartitionMemberKind identifies a table's place in a PostgreSQL partition
// tree: the partitioned table it belongs to and the rows it takes. The parent's
// strategy and key stay with [PartitionKind] on the parent, and a partition that
// is partitioned itself carries both kinds.
const PartitionMemberKind schemaext.Kind = "ptah.run/postgres/table-partition-member"

// PartitionMember names a partition's parent and its bound.
type PartitionMember struct {
	// ParentSchema is the parent's schema without SQL quotes, and empty for a
	// name the source leaves unqualified.
	ParentSchema string `json:"parent_schema,omitempty"`
	// Parent is the partitioned table's name without SQL quotes.
	Parent string `json:"parent"`
	// Bound is the partition bound as [ParsePartitionBound] returns it:
	// FOR VALUES with its values, or DEFAULT.
	Bound string `json:"bound"`
}

// DesiredPartitionMember declares a table as a partition of another table. It
// defines its table: PostgreSQL gives a partition its parent's columns, so a
// declaration holds no column list and a comparison reads none.
type DesiredPartitionMember struct {
	PartitionMember
	// Normalized holds the server's spelling of the bound; source encoders omit
	// it.
	Normalized *NormalizedPartitionBound `json:"normalized,omitempty"`
}

// NormalizedPartitionBound binds the server's bound to the declaration it
// normalizes. SourceKey and Key are produced by [PartitionBoundKey]. A probe sets
// them together, and changing the declaration invalidates the captured answer.
type NormalizedPartitionBound struct {
	// SourceKey identifies the exact bound sent to the server.
	SourceKey string `json:"source_key"`
	// Key identifies the bound the server returned.
	Key string `json:"key"`
}

// ObservedPartitionMember records a partition an inspection read. It does not
// implement [schemaext.TableDefinition]: the catalog lists a partition's columns
// as any table's, and a source with no syntax for a partition still plans the
// removal of a table it leaves out. Dropping one takes its rows, which a plan has
// to show.
type ObservedPartitionMember PartitionMember

// ParentTable names the partitioned table the inspection found.
func (p *ObservedPartitionMember) ParentTable() (schema, name string) {
	return p.ParentSchema, p.Parent
}

// Kind returns the partition member identity.
func (*DesiredPartitionMember) Kind() schemaext.Kind { return PartitionMemberKind }

// Kind returns the partition member identity.
func (*ObservedPartitionMember) Kind() schemaext.Kind { return PartitionMemberKind }

// DefinesTable reports true: the parent, not a column list, gives a declared
// partition its columns.
func (*DesiredPartitionMember) DefinesTable() bool { return true }

// ParentTable names the table a declared partition is created after.
func (p *DesiredPartitionMember) ParentTable() (schema, name string) {
	return p.ParentSchema, p.Parent
}

// Clone returns an independent declaration, preserving a typed nil receiver.
func (p *DesiredPartitionMember) Clone() schemaext.Value {
	if p == nil {
		return (*DesiredPartitionMember)(nil)
	}
	cloned := *p
	if p.Normalized != nil {
		cloned.Normalized = new(*p.Normalized)
	}
	return &cloned
}

// Clone returns an independent observation, preserving a typed nil receiver.
func (p *ObservedPartitionMember) Clone() schemaext.Value {
	if p == nil {
		return (*ObservedPartitionMember)(nil)
	}
	cloned := *p
	return &cloned
}

// Equal compares declarations. Other representations are unequal.
func (p *DesiredPartitionMember) Equal(value schemaext.Value) bool {
	other, ok := value.(*DesiredPartitionMember)
	if !ok || p == nil || other == nil {
		return ok && p == nil && other == nil
	}
	return p.PartitionMember == other.PartitionMember && equalNormalizedBound(p.Normalized, other.Normalized)
}

// Equal compares observations. Other representations are unequal.
func (p *ObservedPartitionMember) Equal(value schemaext.Value) bool {
	other, ok := value.(*ObservedPartitionMember)
	if !ok || p == nil || other == nil {
		return ok && p == nil && other == nil
	}
	return *p == *other
}

func equalNormalizedBound(a, b *NormalizedPartitionBound) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// ValidateDesiredPartitionMember rejects nil, a missing parent, and a bound
// [ParsePartitionBound] refuses. Errors wrap schemaext.ErrInvalidValue. A server
// decides whether the values suit the parent's key.
func ValidateDesiredPartitionMember(p *DesiredPartitionMember) error {
	if p == nil {
		return validatePartitionMember(nil, schemaext.Desired)
	}
	if err := validatePartitionMember(&p.PartitionMember, schemaext.Desired); err != nil {
		return err
	}
	if p.Normalized != nil && (p.Normalized.Key == "" || p.Normalized.SourceKey != PartitionBoundKey(p.Bound)) {
		return fmt.Errorf("%w: normalized partition bound does not describe the declaration", schemaext.ErrInvalidValue)
	}
	return nil
}

// ValidateObservedPartitionMember validates captured data under the structural
// rules of [ValidateDesiredPartitionMember].
func ValidateObservedPartitionMember(p *ObservedPartitionMember) error {
	return validatePartitionMember((*PartitionMember)(p), schemaext.Observed)
}

func validatePartitionMember(p *PartitionMember, representation schemaext.Representation) error {
	fail := func(message string) error {
		return &schemaext.InvalidModelError{Kind: PartitionMemberKind, Representation: representation, Message: message}
	}
	if p == nil {
		return fail("table partition member requires a non-nil value")
	}
	if p.Parent == "" {
		return fail("a partition requires a parent table")
	}
	if _, err := ParsePartitionBound(p.Bound); err != nil {
		return fail(err.Error())
	}
	return nil
}

// PartitionMemberOf reads either representation. Absence returns nil, false, nil.
// Invalid values return no member and an error wrapping schemaext.ErrInvalidValue.
func PartitionMemberOf(facets schemaext.Facets) (*PartitionMember, bool, error) {
	value, found, err := facets.Get(PartitionMemberKind)
	if err != nil || !found {
		return nil, false, err
	}
	switch value := value.(type) {
	case *DesiredPartitionMember:
		if err := ValidateDesiredPartitionMember(value); err != nil {
			return nil, true, err
		}
		member := value.PartitionMember
		return &member, true, nil
	case *ObservedPartitionMember:
		if err := ValidateObservedPartitionMember(value); err != nil {
			return nil, true, err
		}
		member := PartitionMember(*value)
		return &member, true, nil
	default:
		return nil, true, fmt.Errorf("%w: unexpected table partition member value %T", schemaext.ErrInvalidValue, value)
	}
}

// ParsePartitionBound reads a partition bound: FOR VALUES IN (...), FOR VALUES
// FROM (...) TO (...), FOR VALUES WITH (MODULUS n, REMAINDER n), or DEFAULT, in
// the spelling a document writes or pg_get_expr returns. It returns the bound with
// comments removed and each run of whitespace collapsed, and preserves the case
// and every value as written. Other syntax, trailing text and statement separators
// return an error wrapping schemaext.ErrInvalidValue. It evaluates no value.
func ParsePartitionBound(text string) (string, error) {
	bound, rest, err := SplitPartitionBound(text)
	if err != nil {
		return "", err
	}
	if rest != "" {
		return "", partitionBoundSyntax("unexpected text after the bound: " + rest)
	}
	return bound, nil
}

// SplitPartitionBound reads the bound text opens with, as [ParsePartitionBound]
// returns one, and the text that follows it, trimmed, for a reader whose grammar
// continues after the bound.
func SplitPartitionBound(text string) (bound, rest string, err error) {
	tokens, err := partitionBoundTokens(text)
	if err != nil {
		return "", "", err
	}
	var end int
	switch {
	case len(tokens) >= 1 && tokens[0].MatchIdentifierValue("DEFAULT"):
		end = 1
	case len(tokens) >= 2 && tokens[0].MatchIdentifierValue("FOR") && tokens[1].MatchIdentifierValue("VALUES"):
		end, err = partitionBoundValues(tokens, 2)
		if err != nil {
			return "", "", err
		}
	default:
		return "", "", partitionBoundSyntax("expected FOR VALUES or DEFAULT")
	}
	bound = partitionBoundText(text, tokens[:end])
	if end < len(tokens) {
		rest = strings.TrimSpace(text[tokens[end].Start:])
	}
	return bound, rest, nil
}

func partitionBoundValues(tokens []lexer.Token, start int) (int, error) {
	if start >= len(tokens) {
		return 0, partitionBoundSyntax("expected IN, FROM or WITH after FOR VALUES")
	}
	switch {
	case tokens[start].MatchIdentifierValue("IN"), tokens[start].MatchIdentifierValue("WITH"):
		return partitionBoundGroup(tokens, start+1)
	case tokens[start].MatchIdentifierValue("FROM"):
		end, err := partitionBoundGroup(tokens, start+1)
		if err != nil {
			return 0, err
		}
		if end >= len(tokens) || !tokens[end].MatchIdentifierValue("TO") {
			return 0, partitionBoundSyntax("expected TO after FROM (...)")
		}
		return partitionBoundGroup(tokens, end+1)
	default:
		return 0, partitionBoundSyntax("expected IN, FROM or WITH after FOR VALUES")
	}
}

// partitionBoundGroup steps over a nonempty parenthesized group and returns the
// position after it.
func partitionBoundGroup(tokens []lexer.Token, start int) (int, error) {
	if start >= len(tokens) || !tokens[start].MatchOperatorValue("(") {
		return 0, partitionBoundSyntax("expected a parenthesized list")
	}
	depth := 0
	for i := start; i < len(tokens); i++ {
		switch {
		case tokens[i].MatchOperatorValue("("):
			depth++
		case tokens[i].MatchOperatorValue(")"):
			depth--
			if depth == 0 {
				if i == start+1 {
					return 0, partitionBoundSyntax("empty value list")
				}
				return i + 1, nil
			}
		}
	}
	return 0, partitionBoundSyntax("unbalanced value list")
}

func partitionBoundSyntax(reason string) error {
	return fmt.Errorf("%w: PostgreSQL partition bound: %s", schemaext.ErrInvalidValue, reason)
}

func partitionBoundTokens(text string) ([]lexer.Token, error) {
	scan := lexer.NewLexerWithOptions(text, dialectlexer.Options(platform.Postgres))
	var tokens []lexer.Token
	for token := scan.NextToken(); token.Type != lexer.TokenEOF; token = scan.NextToken() {
		if token.Type == lexer.TokenWhitespace || token.Type == lexer.TokenComment {
			continue
		}
		if token.Type == lexer.TokenUnknown || token.Type == lexer.TokenSemicolon {
			return nil, partitionBoundSyntax("unexpected token " + token.Value)
		}
		tokens = append(tokens, token)
	}
	return tokens, nil
}

// partitionBoundText rejoins tokens as the source wrote them, with one space
// wherever the source separated two.
func partitionBoundText(text string, tokens []lexer.Token) string {
	var bound strings.Builder
	for i, token := range tokens {
		if i > 0 && tokens[i-1].End != token.Start {
			bound.WriteByte(' ')
		}
		bound.WriteString(text[token.Start:token.End])
	}
	return bound.String()
}

// PartitionBoundKey encodes a bound for comparison. It folds SQL whitespace and
// unquoted keyword case and preserves every literal. It performs no database
// normalization, so `'2026-01-01'` and the catalog's `'2026-01-01 00:00:00+00'`
// produce different keys.
func PartitionBoundKey(bound string) string {
	return partitionExpressionKey(bound)
}

const partitionMemberDefinition = `{"type":"object","required":["parent","bound"],"additionalProperties":false,
"properties":{"parent_schema":{"type":"string"},"parent":{"type":"string"},"bound":{"type":"string"}}}`

var desiredPartitionMemberDefinition = strings.TrimSuffix(partitionMemberDefinition, "}}") + `,"normalized":{"type":"object","required":["source_key","key"],"additionalProperties":false,
"properties":{"source_key":{"type":"string"},"key":{"type":"string"}}}}}`

// PartitionMemberCodecs returns version-one declaration and observation codecs.
// They reject unknown fields, duplicate JSON keys, and invalid bounds.
func PartitionMemberCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		schemaext.ModelCodec[*DesiredPartitionMember]{Prototype: &DesiredPartitionMember{}, Representation: schemaext.Desired, Version: 1,
			Definition: json.RawMessage(desiredPartitionMemberDefinition), Shape: desiredPartitionMemberShape, Validate: ValidateDesiredPartitionMember}.Codec(),
		schemaext.ModelCodec[*ObservedPartitionMember]{Prototype: &ObservedPartitionMember{}, Representation: schemaext.Observed, Version: 1,
			Definition: json.RawMessage(partitionMemberDefinition), Shape: partitionMemberShape, Validate: ValidateObservedPartitionMember}.Codec(),
	}
}

func partitionMemberShape(data json.RawMessage) error {
	_, err := schemaext.DecodeObject(data, schemaext.ObjectShape{Name: "table partition member",
		Allowed: []string{"parent_schema", "parent", "bound"}, Required: []string{"parent", "bound"}})
	return err
}

func desiredPartitionMemberShape(data json.RawMessage) error {
	fields, err := schemaext.DecodeObject(data, schemaext.ObjectShape{Name: "table partition member",
		Allowed: []string{"parent_schema", "parent", "bound", "normalized"}, Required: []string{"parent", "bound"}})
	if err != nil {
		return err
	}
	if normalized, found := fields["normalized"]; found {
		if _, err := schemaext.DecodeObject(normalized, schemaext.ObjectShape{Name: "normalized partition bound",
			Allowed: []string{"source_key", "key"}, Required: []string{"source_key", "key"}}); err != nil {
			return err
		}
	}
	return nil
}

var partitionMemberCoverage = schemaext.OwnedCoverageSource(Owner, PartitionMemberCodecs)

// PartitionMemberCoverage describes partition membership independently of facet
// values. Complete coverage makes an absent facet mean a table that belongs to no
// partition tree. A source with no record says nothing about membership.
func PartitionMemberCoverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	return partitionMemberCoverage(PartitionMemberKind, representation, knowledge, subjects)
}

// RefuseForeignPartitionMember refuses a declared partition on a target with no
// such statement. Leaving the partition out is not an option a partition's
// parent can stand in for: its rows have nowhere else to go.
func RefuseForeignPartitionMember(target string) error {
	if slices.Contains(PersistenceTargets(), platform.NormalizeDialect(target)) {
		if SupportsPartition(target) {
			return nil
		}
		return fmt.Errorf("%w: CREATE TABLE ... PARTITION OF is a PostgreSQL partition declaration, which %s has no statement for", ptaherr.ErrUnsupportedFeature, target)
	}
	return fmt.Errorf("%w: table partition member has no policy for %q", ptaherr.ErrUnsupportedDialect, target)
}
