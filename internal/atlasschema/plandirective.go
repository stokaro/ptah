package atlasschema

import (
	"fmt"
	"strconv"
	"strings"

	"go.5x5.cz/ptah/migration/migrator"
)

// PlanDirectiveComment is the comment marker a directive line carries. A
// directive is an ordinary SQL line comment: a database never sees it, and
// every tool that reads migration SQL already knows how to skip it.
const PlanDirectiveComment = "-- "

// planDirectiveNamespace prefixes every directive `--directive` accepts.
// Ptah's own `-- +ptah` family is deliberately out of reach: that flag exists
// on the Atlas surface and spells the Atlas family, and a plan file offering
// two spellings for one idea would be harder to review than the plan.
const planDirectiveNamespace = "atlas:"

// A plan directive is written into the migration text and stays there. It is
// NOT lifted into a field of [PlanFile], and that is the design rather than a
// shortcut.
//
// The migration header of a plan file is a shared namespace: `atlas:nolint`
// already lives there and is honored by `schema plan lint`, which reads it out
// of the SQL. A plan-file layer that claimed the whole header would have to
// either refuse the lines it does not own -- breaking a directive that works
// today -- or keep a field and the text in agreement, which is two sources of
// truth for one line. Leaving every directive in the text hands each one back
// to the subsystem that honors it, and gives all of them one spelling
// (stokaro/ptah#1700).
const (
	// planTxModeDirectiveKey selects how the plan is executed. It is honored
	// by `schema apply --plan`, which reads it with the same migration-file
	// reader that honors it in a versioned migration.
	planTxModeDirectiveKey = "atlas:txmode"

	// planNoLintDirectiveKey silences a lint rule over the plan's SQL. It is
	// honored by `schema plan lint`, which reads it from the statement text.
	planNoLintDirectiveKey = "atlas:nolint"
)

// SupportedPlanDirectives is the whole `--directive` vocabulary, in the order
// a refusal lists it.
//
// It is short, and the shortness is deliberate. A directive Ptah writes but
// does not act on is worse than one it refuses: the plan file would carry an
// instruction the reviewer reads as binding and the run ignores, which is the
// defect `-- atlas:checkpoint` still has in the migration reader
// (stokaro/ptah#954). A key joins this list when something honors it.
var SupportedPlanDirectives = []string{
	planTxModeDirectiveKey + " none",
	planTxModeDirectiveKey + " file",
	planNoLintDirectiveKey + " [<selector>...]",
}

// PlanDirective is one directive line's body -- `atlas:txmode none`, without
// the comment marker.
type PlanDirective string

// ParsePlanDirective normalizes one `--directive` value and refuses everything
// nothing would act on.
//
// Both spellings of the same line are accepted -- with the `-- ` marker, as it
// appears in a file, and without it, as it is typed on a command line --
// because an operator copying a directive out of a migration file copies the
// whole line.
func ParsePlanDirective(raw string) (PlanDirective, error) {
	body := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(raw), "--"))
	if body == "" {
		return "", fmt.Errorf("--directive: the directive is empty; %s", supportedPlanDirectiveList())
	}
	if !strings.HasPrefix(body, planDirectiveNamespace) {
		return "", fmt.Errorf(
			"--directive %q: a plan directive is written in the %s namespace; %s",
			raw, strings.TrimSuffix(planDirectiveNamespace, ":"), supportedPlanDirectiveList())
	}
	directive := PlanDirective(strings.Join(strings.Fields(body), " "))
	switch directive.key() {
	case planNoLintDirectiveKey:
		return directive, nil
	case planTxModeDirectiveKey:
		if _, err := directive.txMode(); err != nil {
			// The migration reader recognized the key and refused the value,
			// which is a more precise answer than this function could
			// assemble. Surfacing it tells an operator who wrote
			// `atlas:txmode all` which value to write instead of telling them
			// the directive is unknown, which it is not.
			return "", fmt.Errorf("--directive %q: %w", raw, err)
		}
		return directive, nil
	default:
		return "", fmt.Errorf(
			"--directive %q: Ptah writes only directives something acts on, and nothing acts on this one; %s",
			raw, supportedPlanDirectiveList())
	}
}

// Line returns the directive as it is written into a migration body.
func (d PlanDirective) Line() string {
	return PlanDirectiveComment + string(d)
}

// key is the directive's name, without its arguments.
func (d PlanDirective) key() string {
	key, _, _ := strings.Cut(string(d), " ")
	return key
}

// txMode reads the transaction mode this directive selects.
//
// The reading is delegated to the migration-file reader rather than repeated
// here, over a body assembled the way a migration file is written: the
// directive line, a blank line, one statement. That is not a workaround for a
// missing accessor -- it is the guarantee that `-- atlas:txmode none` cannot
// come to mean one thing in a migration file and another in a plan, and that
// the placement rule has one implementation.
func (d PlanDirective) txMode() (migrator.MigrationFileTxMode, error) {
	parsed, err := migrator.ParseMigrationUp(
		planDirectiveProbeName, d.Line()+"\n\n"+planDirectiveProbeStatement)
	if err != nil {
		return migrator.MigrationFileTxModeUnspecified, err
	}
	return parsed.TxMode, nil
}

const (
	// planDirectiveProbeStatement is the executable statement
	// [PlanDirective.txMode] puts below the directive, so the directive sits
	// in a leading comment block rather than at the end of an empty body. It
	// is never executed and never leaves this package.
	planDirectiveProbeStatement = "SELECT 1;\n"

	// planDirectiveProbeName is what the migration reader calls the body it
	// was handed. It reaches the operator inside a refused value's diagnostic,
	// so it names what the operator wrote rather than a file that does not
	// exist.
	planDirectiveProbeName = "--directive"
)

// PlanDirectives normalizes a `--directive` flag's values.
//
// A second transaction mode is refused rather than resolved: a plan file is a
// review artifact, two `atlas:txmode` lines saying different things have no
// defensible winner, and two saying the same thing mean the operator believes
// one of them is doing something the other is not. Repeated `atlas:nolint`
// lines are ordinary -- each names its own selectors.
func PlanDirectives(raw []string) ([]PlanDirective, error) {
	directives := make([]PlanDirective, 0, len(raw))
	seenTxMode := false
	for _, value := range raw {
		directive, err := ParsePlanDirective(value)
		if err != nil {
			return nil, err
		}
		if directive.key() == planTxModeDirectiveKey {
			if seenTxMode {
				return nil, fmt.Errorf(
					"--directive %q: the plan already sets a transaction mode, and it has one",
					value)
			}
			seenTxMode = true
		}
		directives = append(directives, directive)
	}
	return directives, nil
}

// PlanDirectiveHeader renders the directives as the header of a migration
// body: one line each, then the blank line that closes the header block.
//
// The blank line is what the Atlas family separates its header from the
// statements with, and the position is the one its reader honors -- a
// directive is read in the unbroken comment run that starts the body, and
// nowhere after it. Writing them anywhere else would produce a file whose
// directives are silently inert.
func PlanDirectiveHeader(directives []PlanDirective) string {
	if len(directives) == 0 {
		return ""
	}
	lines := make([]string, 0, len(directives))
	for _, directive := range directives {
		lines = append(lines, directive.Line())
	}
	return strings.Join(lines, "\n") + "\n\n"
}

// PlanTxMode reports the transaction mode a plan's migration text selects, as
// a FILE mode: the plan states how it wants to be executed, and how that
// combines with an operator's `--tx-mode` is
// [migrator.ResolveAtlasDirectiveTxMode]'s decision rather than this
// function's. [migrator.MigrationFileTxModeUnspecified] means it states
// nothing.
//
// source names the plan for a refused value's diagnostic.
func PlanTxMode(source, migration string) (migrator.MigrationFileTxMode, error) {
	parsed, err := migrator.ParseMigrationUp(source, migration)
	if err != nil {
		return migrator.MigrationFileTxModeUnspecified, err
	}
	return parsed.TxMode, nil
}

func supportedPlanDirectiveList() string {
	quoted := make([]string, 0, len(SupportedPlanDirectives))
	for _, directive := range SupportedPlanDirectives {
		quoted = append(quoted, strconv.Quote(directive))
	}
	return "supported directives are " + strings.Join(quoted, ", ")
}
