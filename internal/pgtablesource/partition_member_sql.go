package pgtablesource

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/internal/lexer"
)

// partitionMemberSQLExtension reads `CREATE TABLE ... PARTITION OF` and `ALTER
// TABLE ... ATTACH PARTITION` on the targets that take them. A document of another
// target reaches the shared grammar's refusal. PARTITION BY and TABLESPACE after the
// bound stay table suffixes, which the frontend reads for this table as it does for
// any other.
func partitionMemberSQLExtension() sqlext.Extension {
	return sqlext.Extension{Owner: pgtable.Owner, Targets: []string{platform.Postgres, platform.YugabyteDB},
		Facets: []schemaext.Kind{pgtable.PartitionMemberKind},
		Patterns: append([]sqlext.Pattern{{Name: "partition-member", Site: sqlext.TableOption,
			Words: []string{"PARTITION", "OF"}, Stops: []string{"PARTITION", "TABLESPACE"}}}, attachPartitionPatterns()...),
		Parser: partitionMemberSQL{},
		Coverage: func(sqlext.CoverageRequest) (schemaext.Coverage, error) {
			return pgtable.PartitionMemberCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)
		},
	}
}

// ParsePartitionMemberSQL reads `PARTITION OF parent bound`, the part of a CREATE
// TABLE that takes the place of its column list. A parent may be qualified and
// quoted. A column or constraint list, and anything between the bound and the end
// of text, are refused by name, because the declaration would not hold them: a
// partition's columns are its parent's, and a constraint of its own is an
// ALTER TABLE statement. Errors wrap schemaext.ErrInvalidValue.
func ParsePartitionMemberSQL(text string) (*pgtable.DesiredPartitionMember, error) {
	tokens, err := partitionTokens(text)
	if err != nil {
		return nil, err
	}
	if len(tokens) < 2 || !tokens[0].MatchIdentifierValue("PARTITION") || !tokens[1].MatchIdentifierValue("OF") {
		return nil, partitionSyntax("expected PARTITION OF")
	}
	member, after, err := partitionParent(tokens, 2)
	if err != nil {
		return nil, err
	}
	if after >= len(tokens) {
		return nil, partitionSyntax("expected FOR VALUES or DEFAULT after the parent table")
	}
	if tokens[after].MatchOperatorValue("(") {
		return nil, partitionSyntax("PARTITION OF with a column or constraint list is not read: a partition's columns come from its parent, " +
			"so declare a default on the parent and a constraint with ALTER TABLE")
	}
	bound, rest, err := pgtable.SplitPartitionBound(text[tokens[after].Start:])
	if err != nil {
		return nil, err
	}
	if rest != "" {
		return nil, partitionSyntax(fmt.Sprintf("%s after the bound of PARTITION OF is not read: Ptah models a partition's parent, bound and key", rest))
	}
	member.Bound = bound
	value := &pgtable.DesiredPartitionMember{PartitionMember: member}
	if err := pgtable.ValidateDesiredPartitionMember(value); err != nil {
		return nil, err
	}
	return value, nil
}

// partitionParent reads a parent table as one name or a schema and a name, with
// each name folded as a server folds it, and returns the position after it.
func partitionParent(tokens []lexer.Token, start int) (pgtable.PartitionMember, int, error) {
	var parts []string
	after := 0
	for i := start; i < len(tokens); i += 2 {
		value := tokens[i].Value
		if tokens[i].Type != lexer.TokenIdentifier && !strings.HasPrefix(value, `"`) {
			return pgtable.PartitionMember{}, 0, partitionSyntax("expected the parent table's name, got " + value)
		}
		part := partitionName(value)
		if part == "" {
			return pgtable.PartitionMember{}, 0, partitionSyntax("empty identifier")
		}
		parts = append(parts, part)
		if i+1 >= len(tokens) || !tokens[i+1].MatchOperatorValue(".") {
			after = i + 1
			break
		}
	}
	switch len(parts) {
	case 1:
		return pgtable.PartitionMember{Parent: parts[0]}, after, nil
	case 2:
		return pgtable.PartitionMember{ParentSchema: parts[0], Parent: parts[1]}, after, nil
	default:
		return pgtable.PartitionMember{}, 0, partitionSyntax("a partition's parent is a table name, optionally qualified by its schema")
	}
}

// partitionMemberSQL reads the whole batch without retaining frontend state.
type partitionMemberSQL struct{}

func (partitionMemberSQL) ParseSQL(ctx context.Context, request sqlext.Request) (sqlext.Result, error) {
	if ctx == nil {
		return sqlext.Result{}, partitionSyntax("SQL requires a context")
	}
	result := sqlext.Result{Values: make([]sqlext.Value, 0, len(request.Claims))}
	for _, claim := range request.Claims {
		if err := ctx.Err(); err != nil {
			return sqlext.Result{}, err
		}
		if claim.Site == sqlext.Statement && strings.HasPrefix(claim.Pattern, attachPartitionPattern) {
			value, err := attachPartitionValue(claim)
			if err != nil {
				return sqlext.Result{}, err
			}
			result.Values = append(result.Values, value)
			continue
		}
		if claim.Pattern != "partition-member" || claim.Site != sqlext.TableOption {
			return sqlext.Result{}, partitionSyntax("unexpected SQL claim")
		}
		value, err := ParsePartitionMemberSQL(claim.Text)
		if err != nil {
			return sqlext.Result{}, ptaherr.At(claim.Offset, fmt.Errorf("table %s: %w", claim.Subject, err))
		}
		facets, err := schemaext.NewFacets(value)
		if err != nil {
			return sqlext.Result{}, err
		}
		result.Values = append(result.Values, sqlext.Value{Facets: facets})
	}
	if err := ctx.Err(); err != nil {
		return sqlext.Result{}, err
	}
	return result, nil
}
