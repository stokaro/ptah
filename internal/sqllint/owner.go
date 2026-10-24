package sqllint

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/lintext"
	"ptah.run/core/platform/capability"
)

// lintOwned lints a source the owner of its dialect splits and judges, which
// is how a source of an executable provider's engine is read: the parser has
// no grammar for it, and the provider's split carries what the statement is.
//
// The rules are those [lintYQL] runs, reading the owner's facts instead of
// the YQL reading:
//
//   - SQL002 for a statement of class [lintext.ClassOther], which this linter
//     does not lint, named by the owner's kind;
//   - DDL001 for each [lintext.FactMissingPrimaryKey];
//   - CAP001 for each [lintext.FactRequiredCapability] the target's
//     capabilities answer false;
//   - SQL004 for the schema statements no judged fact examines, named by their
//     kind, and for each requirement whose key the target's capabilities do
//     not answer. A target the host knows no release line of answers no key,
//     and reading that silence as "unavailable" would report every
//     requirement against a server nobody described.
//
// A CREATE TABLE is examined where the split judges a missing primary key or
// a required capability, an ALTER TABLE where it judges a required
// capability, and another schema statement where it carries a requirement.
// A split that does not judge statement kinds is refused, because every rule
// here starts from the kind.
func lintOwned(ctx context.Context, source Source, opts Options, caps capability.Capabilities) ([]Finding, error) {
	split, err := opts.Owner.SplitStatements(ctx, lintext.SplitRequest{Target: opts.Dialect,
		Sources: []lintext.Source{{Name: source.Name, SQL: source.SQL}}})
	if err != nil {
		return nil, fmt.Errorf("split %s through the owner of %s: %w", source.Name, opts.Dialect, err)
	}
	if !slices.Contains(split.Judged, lintext.FactStatementKind) {
		return nil, fmt.Errorf("the owner of %s does not report what each statement is, so ptah sql lint cannot lint its SQL", opts.Dialect)
	}
	reading := ownedReading{source: source, opts: opts, caps: caps, judged: split.Judged}
	for _, statement := range split.Files[0] {
		reading.read(statement)
	}
	findings := reading.findings
	if len(reading.unanalyzed) > 0 {
		findings = append(findings, statementsNotAnalyzedFinding(source, opts, reading.unanalyzed))
	}
	return findings, nil
}

// ownedReading collects what the rules report on one owner-split source.
type ownedReading struct {
	source     Source
	opts       Options
	caps       capability.Capabilities
	judged     []lintext.Fact
	findings   []Finding
	unanalyzed []string
}

func (r *ownedReading) read(statement lintext.Statement) {
	kind := statementKind(statement)
	line, column := lineColumn(r.source.SQL, statement.Start)
	if kind.Class == lintext.ClassOther {
		r.findings = append(r.findings, Finding{Rule: RuleUnsupportedStatement, Title: "Unsupported SQL statement", Severity: SeverityError,
			File: r.source.Name, Line: line, Column: column, Dialect: r.opts.Dialect,
			Message:   fmt.Sprintf("ptah sql lint does not lint %s statements yet", kind.Kind),
			Rationale: "Unsupported SQL is reported explicitly so the file cannot be mistaken for a clean lint result."})
		return
	}
	examined := false
	for _, effect := range statement.Effects {
		switch effect.Fact {
		case lintext.FactMissingPrimaryKey:
			r.findings = append(r.findings, Finding{Rule: RuleTableWithoutPrimaryKey, Title: "Table has no primary key", Severity: SeverityWarning,
				File: r.source.Name, Line: line, Column: column, Dialect: r.opts.Dialect,
				Message:      fmt.Sprintf("table %q has no primary key", effect.Table),
				Rationale:    "The target's owner reports that the statement creates this table without a primary key.",
				SuggestedFix: "Declare a primary key for the table."})
		case lintext.FactRequiredCapability:
			examined = true
			r.require(effect, line, column)
		}
	}
	if examined || r.examines(kind.Class) {
		return
	}
	r.unanalyzed = appendUnanalyzed(r.unanalyzed, []string{kind.Kind})
}

// examines reports whether a judged fact answers for every statement of class.
func (r *ownedReading) examines(class lintext.StatementClass) bool {
	switch class {
	case lintext.ClassCreateTable:
		return slices.Contains(r.judged, lintext.FactMissingPrimaryKey) || slices.Contains(r.judged, lintext.FactRequiredCapability)
	case lintext.ClassAlterTable:
		return slices.Contains(r.judged, lintext.FactRequiredCapability)
	default:
		return false
	}
}

// require reports a requirement the target's capabilities refuse, and lists
// one they do not answer among the statements no rule examined.
func (r *ownedReading) require(effect lintext.Effect, line, column int) {
	subject := requirementSubject(effect)
	switch {
	case r.caps.Has(effect.Capability):
	case r.caps.Established(effect.Capability):
		r.findings = append(r.findings, Finding{Rule: RuleUnsupportedCapability, Title: "Statement requires unsupported capability", Severity: SeverityError,
			File: r.source.Name, Line: line, Column: column, Dialect: r.opts.Dialect,
			Message:   fmt.Sprintf("%s requires target capability %s, unavailable on this target", subject, effect.Capability),
			Rationale: "Capability-aware lint rules catch SQL that one release line of an engine accepts and another refuses."})
	default:
		r.unanalyzed = appendUnanalyzed(r.unanalyzed, []string{fmt.Sprintf("the %s requirement of %s", effect.Capability, subject)})
	}
}

// statementKind is the statement's one kind, which a split that judges kinds
// gives every statement.
func statementKind(statement lintext.Statement) lintext.Effect {
	for _, effect := range statement.Effects {
		if effect.Fact == lintext.FactStatementKind {
			return effect
		}
	}
	return lintext.Effect{}
}

// requirementSubject names the part of a statement a requirement belongs to,
// from the words the owner gave, for a message.
func requirementSubject(effect lintext.Effect) string {
	subject := strings.TrimSpace(effect.Kind + " " + effect.Object)
	if effect.Table != "" && effect.Table != effect.Object {
		subject += " of table " + effect.Table
	}
	if effect.Clause != "" {
		subject += " (" + effect.Clause + ")"
	}
	if subject == "" {
		return "the statement"
	}
	return subject
}
