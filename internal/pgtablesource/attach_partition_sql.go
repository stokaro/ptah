package pgtablesource

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/dialect/postgres/pgtable"
)

// attachPartitionPattern prefixes the names of the patterns of `ALTER TABLE
// [ONLY] parent ATTACH PARTITION`, which pg_dump writes after a partition's own
// CREATE TABLE. The statement declares the partition as `CREATE TABLE ...
// PARTITION OF` would: it sets the partition's parent and bound on a table the
// document declared, and the reader then takes the partition's columns from its
// parent.
const attachPartitionPattern = "attach-partition-"

func attachPartitionPatterns() []sqlext.Pattern {
	patterns := make([]sqlext.Pattern, 0, 2)
	for _, prefix := range [][]string{{"ALTER", "TABLE"}, {"ALTER", "TABLE", "ONLY"}} {
		patterns = append(patterns, sqlext.Pattern{Name: attachPartitionPattern + strings.ToLower(strings.Join(prefix, "-")),
			Site: sqlext.Statement, Words: prefix, AfterName: []string{"ATTACH", "PARTITION"}})
	}
	return patterns
}

// AttachedPartition is what an `ALTER TABLE ... ATTACH PARTITION` statement
// declares: the partition, as the statement writes it, and its place under the
// parent the statement alters.
type AttachedPartition struct {
	// Partition is the attached table's name as the statement writes it.
	Partition string
	// Member is the parent and the bound the statement gives the partition.
	Member *pgtable.DesiredPartitionMember
}

// ParseAttachPartitionSQL reads `ALTER TABLE [ONLY] parent ATTACH PARTITION
// partition FOR VALUES ... | DEFAULT`. Names may be qualified and quoted, and
// are folded as a server folds them. Anything after the bound is refused by
// name. Errors wrap schemaext.ErrInvalidValue.
func ParseAttachPartitionSQL(text string) (AttachedPartition, error) {
	tokens, err := partitionTokens(text)
	if err != nil {
		return AttachedPartition{}, err
	}
	position := 2
	if len(tokens) < 2 || !tokens[0].MatchIdentifierValue("ALTER") || !tokens[1].MatchIdentifierValue("TABLE") {
		return AttachedPartition{}, partitionSyntax("expected ALTER TABLE")
	}
	if position < len(tokens) && tokens[position].MatchIdentifierValue("ONLY") {
		position++
	}
	parent, after, err := partitionParent(tokens, position)
	if err != nil {
		return AttachedPartition{}, err
	}
	if after+1 >= len(tokens) || !tokens[after].MatchIdentifierValue("ATTACH") || !tokens[after+1].MatchIdentifierValue("PARTITION") {
		return AttachedPartition{}, partitionSyntax("expected ATTACH PARTITION after the parent table")
	}
	partitionStart := after + 2
	if partitionStart >= len(tokens) {
		return AttachedPartition{}, partitionSyntax("expected the partition's name after ATTACH PARTITION")
	}
	_, after, err = partitionParent(tokens, partitionStart)
	if err != nil {
		return AttachedPartition{}, err
	}
	if after >= len(tokens) {
		return AttachedPartition{}, partitionSyntax("expected FOR VALUES or DEFAULT after the partition's name")
	}
	bound, rest, err := pgtable.SplitPartitionBound(text[tokens[after].Start:])
	if err != nil {
		return AttachedPartition{}, err
	}
	if rest != "" {
		return AttachedPartition{}, partitionSyntax(fmt.Sprintf("%s after the bound of ATTACH PARTITION is not read", rest))
	}
	parent.Bound = bound
	member := &pgtable.DesiredPartitionMember{PartitionMember: parent}
	if err := pgtable.ValidateDesiredPartitionMember(member); err != nil {
		return AttachedPartition{}, err
	}
	return AttachedPartition{Partition: text[tokens[partitionStart].Start:tokens[after-1].End], Member: member}, nil
}

// attachPartitionValue answers one ATTACH PARTITION claim: the statement a
// syntax-only reader keeps, and the parent and bound set on the partition.
func attachPartitionValue(claim sqlext.Claim) (sqlext.Value, error) {
	attached, err := ParseAttachPartitionSQL(claim.Text)
	if err != nil {
		return sqlext.Value{}, ptaherr.At(claim.Offset, err)
	}
	facets, err := schemaext.NewFacets(attached.Member)
	if err != nil {
		return sqlext.Value{}, err
	}
	return sqlext.Value{Statement: attachPartitionStatement{attached},
		TableFacets: &sqlext.TableFacets{Name: attached.Partition, Facets: facets}}, nil
}

// attachPartitionStatement is the statement a syntax-only reader keeps, such as
// a linter reading a migration.
type attachPartitionStatement struct {
	AttachedPartition
}

// Kind returns the partition member identity.
func (attachPartitionStatement) Kind() schemaext.Kind { return pgtable.PartitionMemberKind }

// CloneExtension returns a copy that shares no memory with the statement.
func (s attachPartitionStatement) CloneExtension() ast.ExtensionPayload {
	member := *s.Member
	if s.Member.Normalized != nil {
		normalized := *s.Member.Normalized
		member.Normalized = &normalized
	}
	return attachPartitionStatement{AttachedPartition{Partition: s.Partition, Member: &member}}
}
