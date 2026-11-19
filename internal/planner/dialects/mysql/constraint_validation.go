package mysql

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/descriptortable"
)

// validateConstraints enables each CHECK and foreign key the database holds
// disabled or unvalidated and the declaration holds validated, as one
// statement on its table, which Oracle writes ENABLE VALIDATE CONSTRAINT. The
// comparison reports one only on a target that takes the statement and only
// for a constraint it neither adds nor recreates, so nothing else in the plan
// touches it.
func validateConstraints(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	for _, validation := range diff.ConstraintsValidated {
		result = append(result, &ast.AlterTableNode{
			Name:       validation.TableName,
			Operations: []ast.AlterOperation{&ast.ValidateConstraintOperation{ConstraintName: validation.Name}},
		})
	}
	return result
}

// checksAddedFirst holds each descriptor's CheckAddedFirst.
var checksAddedFirst = descriptortable.New(func(described descriptor.Dialect) bool {
	return described.Grammar.Constraints.CheckAddedFirst
})

// addsCheckFirst reports whether the plan replaces the CHECK add is paired
// with by adding the new one first. modified reports whether the diff removes
// a constraint of add's identity.
func (p *Planner) addsCheckFirst(add difftypes.ConstraintAdditionInfo, modified bool) bool {
	return modified && add.Type == "CHECK" && add.Name != "" && add.CheckExpression != "" &&
		p.capabilities().Has(capability.CheckConstraintsEnforced) && checksAddedFirst.Get(p.targetDialect())
}

// leftoverOfReplacement answers the removal of the CHECK that holds the
// temporary name of add's replacement on add's table.
//
// A replacement that stops after its ADD leaves that CHECK beside the old one.
// The comparison removes it as a CHECK nobody declared, and the removals come
// after the additions, so the next plan adds the new CHECK under a name the
// table already holds and fails with ORA-02264 on every run. Its drop goes
// first instead. The old CHECK is still on the table then, so the table holds
// a CHECK at every step. The removal names no condition, and the plan drops
// the CHECK whatever its condition, so the name alone recognizes it: it is the
// table and the declared name hashed, which nobody writes by hand.
//
// The removal pass asks the same question through
// [Planner.replacementLeftovers], so the CHECK is dropped once.
func leftoverOfReplacement(
	add difftypes.ConstraintAdditionInfo,
	removals map[constraintHostKey]difftypes.ConstraintRemovalInfo,
	semantics identifier.Semantics,
) (difftypes.ConstraintRemovalInfo, bool) {
	identity := add.Identity
	identity.Name = semantics.IndexIdentityKey(replacementName(add.TableName, add.Name))
	info, left := removals[identity]
	return info, left && strings.EqualFold(info.Type, "CHECK")
}

// replacementLeftovers answers the removals the additions drop ahead of their
// replacement; see [leftoverOfReplacement].
func (p *Planner) replacementLeftovers(diff *difftypes.SchemaDiff, semantics identifier.Semantics) map[constraintHostKey]struct{} {
	removals := make(map[constraintHostKey]difftypes.ConstraintRemovalInfo, len(diff.ConstraintsRemoved))
	for _, info := range diff.ConstraintsRemoved {
		removals[info.Identity] = info
	}
	leftovers := make(map[constraintHostKey]struct{})
	for _, add := range diff.ConstraintsAdded {
		_, modified := removals[add.Identity]
		if !p.addsCheckFirst(add, modified) {
			continue
		}
		if info, left := leftoverOfReplacement(add, removals, semantics); left {
			leftovers[info.Identity] = struct{}{}
		}
	}
	return leftovers
}

// replacedCheck answers the statements that replace the named CHECK the
// removal info describes with the one add describes, keeping a CHECK on the
// table at every step: the new CHECK added under a temporary name, the old one
// dropped, and the new one renamed to its declared name. Each statement
// commits apart on a target that sets CheckAddedFirst, so an ADD the rows
// refuse stops the plan with the old CHECK still in place.
func (p *Planner) replacedCheck(
	add difftypes.ConstraintAdditionInfo,
	constraint *ast.ConstraintNode,
	drop ast.Node,
) []ast.Node {
	declared := constraint.Name
	temporary := replacementName(add.TableName, declared)
	constraint.Name = temporary
	return []ast.Node{
		&ast.AlterTableNode{Name: add.TableName, Operations: []ast.AlterOperation{&ast.AddConstraintOperation{Constraint: constraint}}},
		drop,
		&ast.AlterTableNode{Name: add.TableName, Operations: []ast.AlterOperation{
			&ast.RenameConstraintOperation{From: temporary, To: declared},
		}},
	}
}

// replacementName answers the temporary name a replaced CHECK is added under:
// ptah_ and the first 16 hex digits of the SHA-256 of the table and the
// declared name. The same inputs give the same name, so two plans of one
// change render the same text, and no name an author writes by hand is
// likely to take it.
func replacementName(table, name string) string {
	sum := sha256.Sum256([]byte(table + "\x00" + name))
	return "ptah_" + hex.EncodeToString(sum[:8])
}
