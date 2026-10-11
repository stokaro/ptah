package mysqllint

import (
	"fmt"
	"strings"

	"ptah.run/core/lintext"
	"ptah.run/internal/lintkit"
	"ptah.run/internal/lintkit/mytype"
)

// A MySQL ENUM or SET column is defined by an ordered list of members, and
// the server stores each value by its position in that list rather than by
// its text. That one fact decides which changes to the list are cheap and
// which copy the table: a member appended at the end leaves every existing
// position alone, while removing, reordering, or inserting one renumbers the
// rows that follow. The rules in this file compare the list a column has with
// the list a MODIFY or CHANGE gives it, and say which of those it did.
//
// Every claim below was measured rather than read off a manual, on MySQL
// 8.4 and MariaDB 11.8.9, by asking for ALGORITHM=INSTANT and then INPLACE
// and recording which the server refused. Both servers agreed on every
// case (stokaro/ptah#2942):
//
//	change                                   INSTANT   INPLACE
//	ENUM: remove a member                    refused   refused
//	ENUM: reorder members                    refused   refused
//	ENUM: insert a member before the end     refused   refused
//	ENUM: append at the end, 253 -> 254      ok        ok
//	ENUM: append at the end, 255 -> 256      refused   refused
//	SET: remove, reorder, insert             refused   refused
//	SET: append, 6 -> 7                      ok        ok
//	SET: append across 8, 16, 24, 32         refused   refused
//	SET: append, 15 -> 16, 31 -> 32, 63 -> 64  ok      ok
//
// A refusal of both is the server saying the change needs ALGORITHM=COPY:
// the table is rebuilt row by row and writes block for the duration. The two
// boundaries are storage: an ENUM takes one byte up to 255 members and two
// beyond, a SET takes one byte per eight members up to 64, so an append that
// crosses one changes the row format and forces the copy an append normally
// avoids.
//
// The data consequence of a removal was measured the same way. A row still
// holding a removed member makes the copy fail with "Data truncated for
// column" under the strict SQL mode both servers ship with, and is stored as
// the empty value when strict mode is off.
//
// Precision over volume: a rule here fires only when the starting list is
// known, which means the run supplied the schema state the migration starts
// from. Without it the statement is still reported by the generic DS103 and
// MY101, and migration lint names these rules as the ones that could have
// said more.

// parseMemberSpelling reads the type as the server prints it in
// information_schema.COLUMN_TYPE -- enum('a','b','c') or set('x','y') --
// which is what the dev-database read puts in [lintext.BaselineColumn.ColumnType].
// Anything else, including an empty spelling, is not a member list.
func parseMemberSpelling(spelling string) (mytype.MemberList, bool) {
	text := strings.TrimSpace(spelling)
	open := strings.IndexByte(text, '(')
	if open < 0 || !strings.HasSuffix(text, ")") {
		return mytype.MemberList{}, false
	}
	kind, ok := mytype.MemberKindOf(text[:open])
	if !ok {
		return mytype.MemberList{}, false
	}
	members, ok := parseQuotedList(text[open+1 : len(text)-1])
	if !ok {
		return mytype.MemberList{}, false
	}
	return mytype.MemberList{Kind: kind, Members: members}, true
}

// parseQuotedList splits the inside of enum(...) as the server prints it:
// single-quoted values separated by commas, with a quote inside a value
// doubled.
func parseQuotedList(inside string) ([]string, bool) {
	var members []string
	rest := strings.TrimSpace(inside)
	for rest != "" {
		if rest[0] != '\'' {
			return nil, false
		}
		end := quotedMemberEnd(rest)
		if end < 0 {
			return nil, false
		}
		value, ok := mytype.UnquoteMember(rest[:end+1])
		if !ok {
			return nil, false
		}
		members = append(members, value)
		rest = strings.TrimSpace(rest[end+1:])
		if rest == "" {
			break
		}
		if rest[0] != ',' {
			return nil, false
		}
		rest = strings.TrimSpace(rest[1:])
	}
	return members, len(members) > 0
}

// quotedMemberEnd returns the index of the quote closing the literal that
// starts at s[0], honoring the doubled quote and the backslash escape both
// servers accept in DDL.
func quotedMemberEnd(s string) int {
	for i := 1; i < len(s); i++ {
		switch s[i] {
		case '\\':
			i++
		case '\'':
			if i+1 < len(s) && s[i+1] == '\'' {
				i++
				continue
			}
			return i
		}
	}
	return -1
}

// memberTransition is what a new list does to an old one, in the terms the
// server charges for.
type memberTransition struct {
	// removed lists old members the new list no longer has, in old order.
	removed []string
	// reordered reports that the members present in both lists appear in a
	// different order in the new one.
	reordered bool
	// inserted lists new members placed before a surviving old member, in new
	// order. Each one renumbers every member after it.
	inserted []string
	// appended lists new members placed after every surviving old member, in
	// new order. These alone leave existing positions untouched.
	appended []string
	oldCount int
	newCount int
}

// compareMembers classifies the change from old to new. Removal, reordering
// and insertion are three separate facts because a migration can do any
// combination, and the diagnostic names each one the author has to answer
// for.
func compareMembers(old, updated mytype.MemberList) memberTransition {
	transition := memberTransition{oldCount: len(old.Members), newCount: len(updated.Members)}
	newIndex := make(map[string]int, len(updated.Members))
	for i, member := range updated.Members {
		newIndex[member] = i
	}
	oldIndex := make(map[string]int, len(old.Members))
	for i, member := range old.Members {
		oldIndex[member] = i
	}

	lastSurvivor := -1
	previous := -1
	for _, member := range old.Members {
		position, kept := newIndex[member]
		if !kept {
			transition.removed = append(transition.removed, member)
			continue
		}
		if position < previous {
			transition.reordered = true
		}
		previous = position
		if position > lastSurvivor {
			lastSurvivor = position
		}
	}
	for i, member := range updated.Members {
		if _, existed := oldIndex[member]; existed {
			continue
		}
		if i < lastSurvivor {
			transition.inserted = append(transition.inserted, member)
		} else {
			transition.appended = append(transition.appended, member)
		}
	}
	return transition
}

// enumStorageBytes is what one ENUM value occupies: one byte up to 255
// members, two beyond. Measured: 253 -> 254 applies in place, 255 -> 256
// copies the table.
func enumStorageBytes(members int) int {
	if members <= 255 {
		return 1
	}
	return 2
}

// setStorageBytes is what one SET value occupies: a bit per member, rounded
// up to whole bytes, with the 5- to 8-byte step the server skips. Measured at
// every boundary: 8 -> 9, 16 -> 17, 24 -> 25 and 32 -> 33 copy the table;
// 15 -> 16, 23 -> 24, 31 -> 32 and 63 -> 64 apply in place.
func setStorageBytes(members int) int {
	switch {
	case members <= 8:
		return 1
	case members <= 16:
		return 2
	case members <= 24:
		return 3
	case members <= 32:
		return 4
	default:
		return 8
	}
}

func storageBytes(kind mytype.MemberKind, members int) int {
	if kind == mytype.Set {
		return setStorageBytes(members)
	}
	return enumStorageBytes(members)
}

// memberChangeSite is one MODIFY or CHANGE clause that gives a column an
// ENUM or SET type, with everything a rule needs to compare it against the
// column's current definition.
type memberChangeSite struct {
	statement int
	table     lintkit.TableReference
	// oldName is the column the clause changes; newName is what CHANGE
	// renames it to, or oldName again for MODIFY.
	oldName string
	newName string
	updated mytype.MemberList
}

// memberChangeSites finds every clause in a file that assigns a member list.
// A clause whose new type is not ENUM or SET is not a site: what such a column
// was before the clause is DS103's question, not this file's.
func memberChangeSites(file *lintkit.File) []memberChangeSite {
	var sites []memberChangeSite
	for index := range file.Statements {
		stmt := &file.Statements[index]
		w := stmt.Words
		if !lintkit.IsAlterTable(w) {
			continue
		}
		table := lintkit.AlterTableReference(w, stmt.SourceWords)
		for _, i := range lintkit.ClauseStarts(w) {
			if i >= len(w) || (w[i] != "MODIFY" && w[i] != "CHANGE") {
				continue
			}
			change := w[i] == "CHANGE"
			j := i + 1
			if j < len(w) && w[j] == "COLUMN" {
				j++
			}
			j = lintkit.SkipIfExists(w, j)
			if j >= len(w) || !lintkit.IdentLike(w[j]) {
				continue
			}
			oldName := lintkit.SourceWordAt(w, stmt.SourceWords, j)
			newName := oldName
			j++
			if change {
				if j >= len(w) || !lintkit.IdentLike(w[j]) {
					continue
				}
				newName = lintkit.SourceWordAt(w, stmt.SourceWords, j)
				j++
			}
			updated, _, ok := mytype.MemberListAt(w, j)
			if !ok {
				continue
			}
			sites = append(sites, memberChangeSite{
				statement: index,
				table:     table,
				oldName:   oldName,
				newName:   newName,
				updated:   updated,
			})
		}
	}
	return sites
}

// memberChangeStatements is the [Rule.BaselineSubjects] predicate the family
// shares: every statement with a site is one the rules need the starting
// state for.
func memberChangeStatements(file *lintkit.File) []int {
	var indexes []int
	seen := -1
	for _, site := range memberChangeSites(file) {
		if site.statement == seen {
			continue
		}
		seen = site.statement
		indexes = append(indexes, site.statement)
	}
	return indexes
}

// resolvedMemberChange is a site joined to the list the column has now. Only
// a site whose column the baseline knows, and whose old and new types are
// the same kind of list, resolves: a column going from ENUM to SET or from
// VARCHAR to ENUM is a type change, which DS103 already reports.
type resolvedMemberChange struct {
	site       memberChangeSite
	old        mytype.MemberList
	transition memberTransition
}

func resolveMemberChanges(file *lintkit.File) []resolvedMemberChange {
	if !file.IsUp {
		return nil
	}
	var resolved []resolvedMemberChange
	for _, site := range memberChangeSites(file) {
		column, ok := file.Baseline.Column(site.table.Normalized, lintkit.NormalizeIdent(site.oldName))
		if !ok {
			continue
		}
		old, ok := parseMemberSpelling(column.ColumnType)
		if !ok || old.Kind != site.updated.Kind {
			continue
		}
		resolved = append(resolved, resolvedMemberChange{
			site:       site,
			old:        old,
			transition: compareMembers(old, site.updated),
		})
	}
	return resolved
}

// memberFinding builds one finding for a resolved change.
func memberFinding(change resolvedMemberChange, message string) lintext.Violation {
	subject := lintext.Subject{
		Kind:     lintext.SubjectColumn,
		Name:     change.site.newName,
		Parent:   change.site.table.Name,
		DataType: strings.ToLower(change.site.updated.Kind.String()),
	}
	return lintext.Violation{Statement: change.site.statement, Message: message, Subjects: []lintext.Subject{subject}}
}

func (c resolvedMemberChange) column() string {
	return c.site.table.Name + "." + c.site.newName
}

func (c resolvedMemberChange) clause() string {
	if c.site.oldName != c.site.newName {
		return "CHANGE COLUMN"
	}
	return "MODIFY COLUMN"
}

// The copy every one of these findings describes, said once so the four
// diagnostics cannot drift into four accounts of the same server behavior.
const memberCopyConsequence = "both MySQL and MariaDB refuse ALGORITHM=INSTANT and INPLACE for it, " +
	"so the server copies the whole table row by row and blocks writes until the copy finishes"

// mysqlMemberRules is the family: four facts about an ENUM list and the same
// four about a SET list, one rule each, so a policy can pick which of them
// to enforce and a reader sees which one a migration did.
func mysqlMemberRules() []lintkit.Rule {
	var rules []lintkit.Rule
	for _, kind := range []mytype.MemberKind{mytype.Enum, mytype.Set} {
		rules = append(rules,
			memberRemovedRule(kind),
			memberReorderedRule(kind),
			memberInsertedRule(kind),
			memberStorageRule(kind),
		)
	}
	return rules
}

// memberRuleCodes are the Atlas identifiers, kept because the analysis is the
// same one Atlas documents under them (the identifier convention in
// internal/lintcatalog).
func memberRuleCode(kind mytype.MemberKind, offset int) string {
	if kind == mytype.Set {
		return fmt.Sprintf("MY12%d", offset)
	}
	return fmt.Sprintf("MY11%d", offset)
}

func memberRule(kind mytype.MemberKind, offset int, title string, check func(resolvedMemberChange) (bool, string)) lintkit.Rule {
	code := memberRuleCode(kind, offset)
	return lintkit.Rule{
		Rule: lintext.Rule{
			Code:     code,
			Title:    title,
			Severity: lintext.Warning,
			Targets:  []string{"mysql", "mariadb"},
			Subsumes: []string{"DS103", "MY101"},
			Input:    lintext.InputBaselineSchema,
		},
		BaselineSubjects: memberChangeStatements,
		File: func(file *lintkit.File) []lintext.Violation {
			var findings []lintext.Violation
			for _, change := range resolveMemberChanges(file) {
				if change.old.Kind != kind {
					continue
				}
				if hit, message := check(change); hit {
					findings = append(findings, memberFinding(change, message))
				}
			}
			return findings
		},
	}
}

func memberRemovedRule(kind mytype.MemberKind) lintkit.Rule {
	return memberRule(kind, 0, kind.String()+" member removed", func(c resolvedMemberChange) (bool, string) {
		if len(c.transition.removed) == 0 {
			return false, ""
		}
		return true, fmt.Sprintf(
			"%s removes %s member%s %s from %s, keeping %d of %d; %s. "+
				"A row that still holds a removed member fails the copy with \"Data truncated\" under the strict "+
				"SQL mode both servers default to, and is stored as the empty value when strict mode is off. "+
				"Keep the member and stop writing it, or move those rows to a surviving member first and run "+
				"the change through an online-DDL tool such as gh-ost or pt-online-schema-change",
			c.clause(), kind, lintkit.Plural(len(c.transition.removed)), mytype.QuoteMembers(c.transition.removed),
			c.column(), c.transition.newCount-len(c.transition.appended)-len(c.transition.inserted),
			c.transition.oldCount, memberCopyConsequence,
		)
	})
}

func memberReorderedRule(kind mytype.MemberKind) lintkit.Rule {
	return memberRule(kind, 1, kind.String()+" members reordered", func(c resolvedMemberChange) (bool, string) {
		if !c.transition.reordered {
			return false, ""
		}
		return true, fmt.Sprintf(
			"%s reorders the %s members of %s (%s becomes %s); the server stores each value by its position "+
				"in the list, so every row is renumbered and %s. Keep the existing order and put new members at the end",
			c.clause(), kind, c.column(), mytype.QuoteMembers(c.old.Members), mytype.QuoteMembers(c.site.updated.Members),
			memberCopyConsequence,
		)
	})
}

func memberInsertedRule(kind mytype.MemberKind) lintkit.Rule {
	return memberRule(kind, 2, kind.String()+" member inserted before the end", func(c resolvedMemberChange) (bool, string) {
		if len(c.transition.inserted) == 0 {
			return false, ""
		}
		return true, fmt.Sprintf(
			"%s inserts %s member%s %s into %s ahead of existing members; the server stores each value by its "+
				"position in the list, so every member after the insertion is renumbered and %s. "+
				"Append new members at the end instead, which both servers apply in place",
			c.clause(), kind, lintkit.Plural(len(c.transition.inserted)), mytype.QuoteMembers(c.transition.inserted), c.column(),
			memberCopyConsequence,
		)
	})
}

func memberStorageRule(kind mytype.MemberKind) lintkit.Rule {
	title := "ENUM crosses the 255-member storage boundary"
	if kind == mytype.Set {
		title = "SET crosses a storage-size boundary"
	}
	return memberRule(kind, 3, title, func(c resolvedMemberChange) (bool, string) {
		before := storageBytes(kind, c.transition.oldCount)
		after := storageBytes(kind, c.transition.newCount)
		if before == after {
			return false, ""
		}
		boundary := "255 members, where an ENUM value grows from one byte to two"
		if kind == mytype.Set {
			boundary = "a multiple of eight members, where a SET value grows by a byte"
		}
		return true, fmt.Sprintf(
			"%s takes %s from %d to %d members, across %s (%d to %d byte%s per value); the row format changes, "+
				"so even an append at the end is not applied in place and %s. "+
				"Stay under the boundary, or plan the copy with an online-DDL tool",
			c.clause(), c.column(), c.transition.oldCount, c.transition.newCount, boundary,
			before, after, lintkit.Plural(after), memberCopyConsequence,
		)
	})
}
