package atlasschema

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/concurrentindex"
	"ptah.run/internal/sqlitevirtual"
	"ptah.run/migration/diffpolicy"
)

// DiffPolicy holds Atlas-compatible schema diff policy that maps to Ptah's
// local planning capabilities.
type DiffPolicy struct {
	// SkipChangeKinds lists the destructive change kinds a plan leaves out:
	// `diff.skip` in ptah.yaml, and `diff { skip { ... } }` in an Atlas
	// project file. They are removed by [diffpolicy.ApplyForDialect], the
	// filter `ptah migrations generate` applies, so both commands omit the same
	// changes, and the plan carries one comment for each change it left out.
	SkipChangeKinds []planext.ChangeKind

	ConcurrentIndexCreate bool
	ConcurrentIndexDrop   bool
	// OnlineAlter asks the server to apply the plan without blocking the
	// writes already running against its tables, in whichever grammar the
	// target has. A target without one is planned as if this were off.
	OnlineAlter bool

	// ConcurrentIndexCreateDisabled is the operator saying no, which is a
	// different answer from not saying yes.
	//
	// ConcurrentIndexCreate builds EVERY addition concurrently. Its zero value
	// does not mean the opposite: it means nothing was requested, and a desired
	// description that asked for `CREATE INDEX CONCURRENTLY` is still honored
	// (stokaro/ptah#2019). Only this field turns that off, and it is set from an
	// explicit `false` in the project config rather than from an absent one --
	// which is why the config's presence bit is read and not just its value.
	ConcurrentIndexCreateDisabled bool

	// AllowTableRebuild plans a change the target cannot make in place as an
	// explicit table rebuild. The native commands set it from
	// --allow-table-rebuild, and the compatibility surface, which takes no new
	// flag, from PTAH_ALLOW_TABLE_REBUILD.
	AllowTableRebuild bool
	// TableRebuildDialects names the dialects whose plans create a table again
	// where the engine has no ALTER TABLE for the change: the
	// `diff "<dialect>" { allow_recreate = true }` blocks of an Atlas project
	// file, which the compatibility surface reads. Use [DiffPolicy.AllowsTableRebuild]
	// to ask about one plan.
	TableRebuildDialects []string
	// TableRebuildRequest is how the caller asks for AllowTableRebuild, and the
	// refusal of a change only a rebuild can make names it. Empty names the
	// native flag. See [ptah.run/core/planext.Options.TableRebuildRequest].
	TableRebuildRequest string
	// SkipCheckScan plans an added CHECK without the scans of the rows the
	// table holds; see [ptah.run/core/planext.Options.SkipCheckScan]. The
	// native commands set it from --skip-check-scan, and the compatibility
	// surface from PTAH_SKIP_CHECK_SCAN.
	SkipCheckScan bool
	// AllowValueRounding plans a column type change that keeps fewer digits
	// after the point without reading the values the table holds, so the
	// server rounds or cuts each one to the new digits. Without it such a
	// plan is refused while a row holds a value the change would alter; see
	// [ptah.run/internal/roundingscan]. The native commands set it from
	// --allow-value-rounding, and the compatibility surface from
	// PTAH_ALLOW_VALUE_ROUNDING.
	AllowValueRounding bool
	// ValueRoundingRequest is how the caller asks for AllowValueRounding, and
	// the refusal names it.
	ValueRoundingRequest string

	// Dialects holds, by dialect, the values the labeled `diff "<dialect>"`
	// blocks of an Atlas project file set. A plan for the dialect follows them
	// in place of SkipChangeKinds and the concurrent index fields above
	// ([DiffPolicy.ForDialect]), and a plan for another dialect is refused
	// where they would change it ([DiffPolicy.ReviewOtherDialects]).
	Dialects map[string]DialectDiffPolicy

	// RotateSecrets names the secrets, as the operator wrote them, the plan
	// gives the value their declared variable holds now. A comparison cannot
	// find a changed value, which the server never returns, so this is the
	// only way one is planned. The native commands set it from
	// --rotate-secret.
	RotateSecrets []string
}

// DialectDiffPolicy is what the labeled diff blocks for one dialect set, by
// where the project file writes them. A value the top-level block sets and the
// env's unlabeled diff block sets too is left out of TopLevel, since the env is
// nearer the run.
type DialectDiffPolicy struct {
	// TopLevel holds the values the top-level labeled block sets.
	TopLevel DiffSettings
	// Env holds the values the env's labeled block sets.
	Env DiffSettings
}

// DiffSetting names one value of a diff policy block as an Atlas project file
// spells it: the block, "skip" or "concurrent_index", and the attribute in it.
type DiffSetting struct {
	Block string
	Name  string
}

// The concurrent_index values a labeled diff block can set. A skip value is
// named by [SkipSetting].
var (
	ConcurrentIndexCreateSetting = DiffSetting{Block: "concurrent_index", Name: "create"}
	ConcurrentIndexDropSetting   = DiffSetting{Block: "concurrent_index", Name: "drop"}
)

// SkipSetting names the skip value of a change kind: `drop_table` in the skip
// block for [planext.DropTable].
func SkipSetting(kind planext.ChangeKind) DiffSetting {
	return DiffSetting{Block: "skip", Name: string(kind)}
}

// DiffSettings holds the values one diff block sets. A setting it does not
// hold is one the block leaves to another.
type DiffSettings map[DiffSetting]bool

// ErrDiffPolicyForAnotherDialect is the refusal of a plan that a labeled diff
// block for another dialect would change.
var ErrDiffPolicyForAnotherDialect = errors.New("a labeled diff block holds a policy for another dialect")

// ForDialect returns the policy a plan for the dialect follows: the values its
// labeled diff blocks set in place of the unlabeled block's, the env's over the
// top level's. The labeled blocks stay in the result, for
// [DiffPolicy.ReviewOtherDialects], and asking again changes nothing.
func (p DiffPolicy) ForDialect(dialect string) DiffPolicy {
	labeled, found := p.Dialects[dialect]
	if !found {
		return p
	}
	skipped := diffpolicy.NewSkipSet(p.SkipChangeKinds...)
	var kinds []planext.ChangeKind
	for _, kind := range diffpolicy.AllChangeKinds() {
		if labeled.decides(SkipSetting(kind), skipped.Has(kind)) {
			kinds = append(kinds, kind)
		}
	}
	p.SkipChangeKinds = kinds
	if create, set := labeled.value(ConcurrentIndexCreateSetting); set {
		p.ConcurrentIndexCreate = create
		p.ConcurrentIndexCreateDisabled = !create
	}
	p.ConcurrentIndexDrop = labeled.decides(ConcurrentIndexDropSetting, p.ConcurrentIndexDrop)
	return p
}

// value returns the value the labeled blocks set for setting, the env's over
// the top level's, and whether either sets it.
func (d DialectDiffPolicy) value(setting DiffSetting) (value, set bool) {
	if value, set = d.Env[setting]; set {
		return value, true
	}
	value, set = d.TopLevel[setting]
	return value, set
}

// decides returns the value the labeled blocks set for setting, or otherwise
// the value the plan follows without them.
func (d DialectDiffPolicy) decides(setting DiffSetting, otherwise bool) bool {
	if value, set := d.value(setting); set {
		return value
	}
	return otherwise
}

// ReviewOtherDialects decides what the labeled diff blocks for other dialects
// do to a plan for dialect, whose comparison is compared.
//
// Ptah applies a labeled block only to a plan for the dialect its label names.
// The community binary ignores the label and applies the block to a plan for
// any dialect, measured on v1.4.0. So a block for another dialect is refused,
// with [ErrDiffPolicyForAnotherDialect], where it would change this plan: it
// sets a value that no nearer block decides, the value differs from the one
// the plan follows, and compared holds a change of the kind the value governs.
// A skip value governs the removals of its kind. A concurrent_index value
// governs the index additions, or removals, of a dialect whose grammar builds
// them concurrently, whatever the desired description declares. Every other
// block for another dialect is reported on diagnostics, since it changes
// nothing in this plan.
//
// A value the env's labeled block for dialect sets is decided by that block
// when the other block is written at the top level; the env is nearer the run.
// capabilities is the target's set; an empty one reads the dialect's default.
func (p DiffPolicy) ReviewOtherDialects(
	diagnostics io.Writer,
	compared *difftypes.SchemaDiff,
	dialect string,
	capabilities capability.Capabilities,
) error {
	resolved := p.ForDialect(dialect)
	nearest := p.Dialects[dialect].Env
	if capabilities.Len() == 0 {
		capabilities = capability.ForDialect(dialect)
	}
	for _, other := range slices.Sorted(maps.Keys(p.Dialects)) {
		if other == dialect {
			continue
		}
		labeled := p.Dialects[other]
		for _, setting := range labeled.undecided(nearest) {
			value, _ := labeled.value(setting)
			if followed, set := resolved.followed(setting); set && followed == value {
				continue
			}
			change, found := governedChange(setting, compared, dialect, capabilities)
			if !found {
				continue
			}
			return fmt.Errorf("%w: the diff %q block of the project file sets %s = %t in its %s block, which Ptah applies only to a plan for %s. "+
				"This plan is for %q and holds a change that value decides: %s. "+
				"The community binary ignores the label and applies the block to every dialect. "+
				"Move the value into an unlabeled diff block to apply it to every dialect, or into the env of the %s database",
				ErrDiffPolicyForAnotherDialect, other, setting.Name, value, setting.Block, other, dialect, change, other)
		}
		if diagnostics != nil {
			_, _ = fmt.Fprintf(diagnostics, "the diff %q block of the project file is not applied to this plan for %q: "+
				"Ptah applies it only to a plan for %s, and it changes nothing in this one\n", other, dialect, other)
		}
	}
	return nil
}

// undecided returns, in a stable order, the settings the labeled blocks set
// that no nearer block decides for a plan whose own env block sets nearest.
func (d DialectDiffPolicy) undecided(nearest DiffSettings) []DiffSetting {
	var settings []DiffSetting
	for setting := range d.Env {
		settings = append(settings, setting)
	}
	for setting := range d.TopLevel {
		_, env := d.Env[setting]
		_, nearer := nearest[setting]
		if !env && !nearer {
			settings = append(settings, setting)
		}
	}
	slices.SortFunc(settings, func(a, b DiffSetting) int {
		return strings.Compare(a.Block+"."+a.Name, b.Block+"."+b.Name)
	})
	return settings
}

// followed returns the value a resolved policy follows for setting, so a block
// that sets the same value changes nothing. A concurrent_index create value is
// unset where the policy neither builds every index concurrently nor turns
// concurrent builds off: either value a block sets changes the plan then.
func (p DiffPolicy) followed(setting DiffSetting) (value, set bool) {
	switch setting {
	case ConcurrentIndexCreateSetting:
		return p.ConcurrentIndexCreate, p.ConcurrentIndexCreate || p.ConcurrentIndexCreateDisabled
	case ConcurrentIndexDropSetting:
		return p.ConcurrentIndexDrop, true
	}
	return slices.Contains(p.SkipChangeKinds, planext.ChangeKind(setting.Name)), true
}

// governedChange names the first change of compared that setting governs on
// a plan for dialect, and reports whether there is one.
func governedChange(setting DiffSetting, compared *difftypes.SchemaDiff, dialect string, capabilities capability.Capabilities) (string, bool) {
	described, _ := platform.Lookup(dialect)
	switch setting {
	case ConcurrentIndexCreateSetting:
		if !described.ConcurrentIndexes || !capabilities.Has(capability.CreateIndexConcurrently) || len(compared.IndexesAdded) == 0 {
			return "", false
		}
		return fmt.Sprintf("index %q is added", compared.IndexesAdded[0].Index.Name), true
	case ConcurrentIndexDropSetting:
		if !described.ConcurrentIndexes || !capabilities.Has(capability.DropIndexConcurrently) || len(compared.IndexesRemoved) == 0 {
			return "", false
		}
		return fmt.Sprintf("index %q is removed", compared.IndexesRemoved[0].Name), true
	}
	_, omitted := diffpolicy.ApplyForDialect(compared, diffpolicy.NewSkipSet(planext.ChangeKind(setting.Name)), dialect)
	if len(omitted) == 0 {
		return "", false
	}
	return fmt.Sprintf("%s of %q", setting.Name, omitted[0].Object), true
}

// AllowsTableRebuild reports whether a plan for the dialect creates a table
// again where the engine has no ALTER TABLE for the change: the policy asks for
// it on every dialect, or its project file asks for it on this one.
func (p DiffPolicy) AllowsTableRebuild(dialect string) bool {
	return p.AllowTableRebuild || slices.Contains(p.TableRebuildDialects, dialect)
}

// withSecretRotation adds to opts a rotation request for each secret policy
// names, as written: the secret's owner reads the spelling, and decides
// whether each is a rotation, a creation, or undecided, and the comparison
// refuses a spelling no owner reads and a name the declaration does not hold.
func withSecretRotation(opts *config.CompareOptions, policy DiffPolicy) {
	opts.FeatureRequests = append(opts.FeatureRequests, schemaext.WrittenRequests(schemaext.RotateAction, policy.RotateSecrets)...)
}

// declaredConcurrentIndexRefs is the index additions the desired description
// asked to build concurrently, subject to the operator's mode.
//
// The two early returns are the two cases where the declaration changes
// nothing: the operator turned concurrent builds off, and an instruction is not
// overruled by a description; or the operator turned them on for every
// addition, which already includes these.
//
// Capabilities are resolved to the dialect default when the caller pinned no
// server version, because that is what the planner does with the same nil and
// the two must not disagree. Reading the nil through [capability.Capabilities.Has]
// instead answers false for every key, which would leave the declaration
// silently unhonored on exactly the invocation that pins no version -- the
// common one.
func declaredConcurrentIndexRefs(
	policy DiffPolicy,
	diff *difftypes.SchemaDiff,
	desired *schemamodel.Database,
	current *catalog.Database,
	dialect string,
	capabilities capability.Capabilities,
) []difftypes.IndexRef {
	if policy.ConcurrentIndexCreateDisabled || policy.ConcurrentIndexCreate {
		return nil
	}
	if capabilities.Len() == 0 {
		capabilities = capability.ForDialect(dialect)
	}
	return concurrentindex.DeclaredRefs(diff, desired, current, catalog.ServerInfo{
		Dialect:      dialect,
		Capabilities: capabilities,
	})
}

// ApplyDiffPolicy returns diff without the changes of the kinds
// policy.SkipChangeKinds lists, and the changes it left out in the order a plan
// names them. A skipped table drop takes the removals that belong to the kept
// table with it; [diffpolicy.ApplyForDialect] is the filter and says which.
// dialect selects the index-name namespace a standalone index drop is told
// from a replacement in. diff is not changed, and a policy that skips nothing
// returns it as given.
func ApplyDiffPolicy(diff *difftypes.SchemaDiff, policy DiffPolicy, dialect string) (*difftypes.SchemaDiff, []diffpolicy.SkippedChange) {
	return diffpolicy.ApplyForDialect(diff, diffpolicy.NewSkipSet(policy.SkipChangeKinds...), dialect)
}

// CompareOptions returns the default comparison options, told which removals
// the caller deletes from the comparison's answer under policy.
//
// The comparison reads them only in the SQLite virtual-table guard, which runs
// inside it and refuses on the statements it predicts. A drop the policy
// deletes afterwards is a statement nothing runs, so without this the guard
// refuses a plan the policy has already emptied (stokaro/ptah#1028). Every
// kind the guard reads is forwarded; see [config.CompareOptions.SkipTableDrops].
func (p DiffPolicy) CompareOptions() *config.CompareOptions {
	opts := config.DefaultCompareOptions()
	opts.SkipTableDrops = slices.Contains(p.SkipChangeKinds, planext.DropTable)
	opts.SkipColumnDrops = slices.Contains(p.SkipChangeKinds, planext.DropColumn)
	opts.SkipIndexDrops = slices.Contains(p.SkipChangeKinds, planext.DropIndex)
	return opts
}

// virtualTablePolicy is the policy as the SQLite virtual-table guard reads it,
// for a comparison that runs the guard itself rather than through
// [DiffPolicy.CompareOptions].
func (p DiffPolicy) virtualTablePolicy() sqlitevirtual.Policy {
	opts := p.CompareOptions()
	return sqlitevirtual.Policy{
		SkipDropTable:  opts.SkipTableDrops,
		SkipDropColumn: opts.SkipColumnDrops,
		SkipDropIndex:  opts.SkipIndexDrops,
	}
}

// ReportOmitted writes one line for each change the diff policy left out, in
// the words the plan's comment carries. A command whose plan has no statement
// left to carry the comment writes it beside its result instead, so "synced"
// never stands alone over a change the policy withheld.
func ReportOmitted(diagnostics io.Writer, omitted []diffpolicy.SkippedChange) {
	if diagnostics == nil {
		return
	}
	for _, change := range omitted {
		_, _ = fmt.Fprintln(diagnostics, change.Comment())
	}
}
