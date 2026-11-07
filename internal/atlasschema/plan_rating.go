package atlasschema

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"ptah.run/core/sqlutil"
	"ptah.run/migration/risk"
	"ptah.run/migration/safety"
)

// ratedStatementWidth is how much of a statement a rating shows. The rating
// follows the plan's full text, so it only has to say which statement it means.
const ratedStatementWidth = 100

// errRatingRuntime refuses a rating from a runtime that cannot judge statements.
var errRatingRuntime = errors.New("the engine runtime cannot judge a plan's statements")

// PlanRating is what a person about to approve a plan reads of its risk: the
// verdicts `schema plan` records for the same statements, for a run that never
// wrote a plan file.
type PlanRating struct {
	// Severity is the highest severity of any statement, [safety.Safe] for a plan
	// with none above it.
	Severity safety.Severity
	// Total is how many statements the plan holds. A statement that holds only
	// comments is not one, since nothing runs for it.
	Total int
	// Statements are the statements rated above safe, in plan order.
	Statements []RatedStatement
}

// RatedStatement is one statement of a [PlanRating].
type RatedStatement struct {
	// Position is the 1-based position of the statement among the plan's Total.
	Position int
	// Text names the statement on one line: its SQL, shortened, or the kind and
	// subject of a native operation.
	Text string
	// Severity and Reason are the statement's verdict.
	Severity safety.Severity
	Reason   string
}

// ratingOf reads a rating off a plan's statement list.
func ratingOf(statements []PlanStatement, dialect string) PlanRating {
	rating := PlanRating{Severity: safety.Safe}
	for _, statement := range statements {
		text := ratedStatementText(statement, dialect)
		if text == "" {
			continue
		}
		rating.Total++
		if risk.Rank(statement.Severity) <= risk.Rank(safety.Safe) {
			continue
		}
		if risk.Rank(statement.Severity) > risk.Rank(rating.Severity) {
			rating.Severity = statement.Severity
		}
		rating.Statements = append(rating.Statements, RatedStatement{
			Position: rating.Total,
			Text:     text,
			Severity: statement.Severity,
			Reason:   statement.Reason,
		})
	}
	return rating
}

// ratedStatementText names a statement on one line, and is empty for a statement
// that holds only comments.
func ratedStatementText(statement PlanStatement, dialect string) string {
	if statement.Native != nil {
		return strings.TrimSpace(string(statement.Native.Kind) + " " + statement.Native.Subject)
	}
	text := strings.TrimRight(strings.Join(strings.Fields(sqlutil.StripCommentsForDialect(statement.SQL, dialect)), " "), "; ")
	if runes := []rune(text); len(runes) > ratedStatementWidth {
		return string(runes[:ratedStatementWidth]) + "..."
	}
	return text
}

// Rating rates the prepared plan as `schema plan` rates it: the same judgment of
// the same statements, with the verdicts the declared rows carry; see
// [judgePlanUnits]. It reads nothing new from the database.
func (p ApplyRuntimePlan) Rating(ctx context.Context) (PlanRating, error) {
	runtime, ok := p.runtime.(safety.Runtime)
	if !ok {
		return PlanRating{}, errRatingRuntime
	}
	statements, err := p.plannedStatements(ctx, runtime)
	if err != nil {
		return PlanRating{}, err
	}
	return ratingOf(statements, p.conn.Info().Dialect), nil
}

// RatingOfSQL is [ApplyRuntimePlan.Rating] for the text a person is about to
// approve. An edit is judged as [PlanFile.WithStatementsFromSQL] judges one for
// `schema plan --edit`, so a statement the edit keeps keeps its verdict.
func (p ApplyRuntimePlan) RatingOfSQL(ctx context.Context, sqlText string) (PlanRating, error) {
	original, err := p.SQL()
	if err != nil {
		return PlanRating{}, err
	}
	if sqlText == original {
		return p.Rating(ctx)
	}
	runtime, ok := p.runtime.(safety.Runtime)
	if !ok {
		return PlanRating{}, errRatingRuntime
	}
	statements, err := p.plannedStatements(ctx, runtime)
	if err != nil {
		return PlanRating{}, err
	}
	dialect := p.conn.Info().Dialect
	edited, err := PlanFile{Dialect: dialect, Statements: statements}.WithStatementsFromSQL(ctx, runtime, sqlText)
	if err != nil {
		return PlanRating{}, err
	}
	return ratingOf(edited.Statements, dialect), nil
}

// plannedStatements is the statement list [PreparePlanFile] records for this
// plan, without the native operations' encoded bytes.
func (p ApplyRuntimePlan) plannedStatements(ctx context.Context, runtime safety.Runtime) ([]PlanStatement, error) {
	computation := applyComputation{ApplyPlan: p.rehearsal.schema, dataStatements: p.rehearsal.data}
	statements, err := judgePlanUnits(ctx, runtime, computation.units, p.conn.Info().Dialect, computation.owned, computation.columns)
	if err != nil {
		return nil, err
	}
	return insertDataStatements(statements, computation), nil
}

// Rating rates a saved plan as it stands. A statement keeps the verdict the plan
// recorded, since an owner's verdict or a column's cannot be read back from text,
// and is raised to what its SQL says, since nothing proves a recorded verdict was
// not edited down. dialect is the connected target's: a plan in the Atlas format
// names none.
func (p PlanFile) Rating(ctx context.Context, runtime safety.Runtime, dialect string) (PlanRating, error) {
	var text []string
	for _, statement := range p.Statements {
		if statement.Native == nil {
			text = append(text, statement.SQL)
		}
	}
	judged, _, err := classifyPlanStatements(ctx, runtime, text, dialect, nil, nil)
	if err != nil {
		return PlanRating{}, err
	}
	statements := make([]PlanStatement, len(p.Statements))
	next := 0
	for i, statement := range p.Statements {
		statements[i] = statement
		if statement.Native == nil {
			raisePlanStatement(&statements[i], judged[next])
			next++
		}
	}
	return ratingOf(statements, dialect), nil
}

// WritePlanRating writes what rate answers, for the prompt that follows it to
// stand beside. A plan rated safe writes nothing. A failed rating writes a
// warning and stops nothing, because the prompt is still the decision and the
// plan is on the screen above it.
func WritePlanRating(out io.Writer, rate func() (PlanRating, error)) {
	rating, err := rate()
	if err != nil {
		fmt.Fprintf(out, "warning: this plan has no rating: %v\n", err)
		return
	}
	if len(rating.Statements) == 0 {
		return
	}
	fmt.Fprintf(out, "Plan rating: %s\n", rating.Severity)
	for _, statement := range rating.Statements {
		fmt.Fprintf(out, "Statement %d of %d, %s: %s\n  %s\n",
			statement.Position, rating.Total, statement.Severity, statement.Reason, statement.Text)
	}
}
