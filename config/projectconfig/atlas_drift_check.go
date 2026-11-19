package projectconfig

import (
	"slices"
	"strconv"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"
)

// DriftOnError says what a pre-apply drift check that did not come back clean
// does to the apply.
type DriftOnError string

const (
	// DriftOnErrorFail refuses the apply: no migration file runs. It is what an
	// atlas.hcl drift block without `on_error` asks for.
	DriftOnErrorFail DriftOnError = "FAIL"
	// DriftOnErrorContinue reports what the check found and applies anyway.
	DriftOnErrorContinue DriftOnError = "CONTINUE"
)

// PreApplyDrift is the drift check an atlas.hcl environment declares for
// `migrate apply`:
//
//	check "migrate_apply" {
//	  drift {
//	    on_error = FAIL
//	    exclude  = ["*.*.comment"]
//	  }
//	}
//
// Before any pending migration runs, the apply replays the history the
// revision table records on the environment's dev database and compares the
// result with the target.
type PreApplyDrift struct {
	// OnError is FAIL or CONTINUE, written in atlas.hcl as the bare
	// identifiers. FAIL when the block does not set it.
	OnError DriftOnError
	// Exclude lists the selectors the comparison leaves out. When ExcludeSet
	// is true it replaces the environment's `exclude` for the check, rather
	// than adding to it; when false the check uses the environment's list.
	Exclude    []string
	ExcludeSet bool
	// Filename and Line locate the drift block, for a diagnostic about it.
	Filename string
	Line     int
}

// Origin names where the drift block was written, as `atlas.hcl check
// "migrate_apply" drift at <file>:<line>`.
func (d PreApplyDrift) Origin() string {
	return `atlas.hcl check "migrate_apply" drift at ` + d.Filename + ":" + strconv.Itoa(d.Line)
}

// migrateApplyCheckLabel is the label of the check block `migrate apply`
// reads. Another label names a check no Ptah command runs.
const migrateApplyCheckLabel = "migrate_apply"

// atlasDriftOnErrorValues are the identifiers `on_error` may name, as values
// of the evaluation context only that attribute sees.
var atlasDriftOnErrorValues = map[string]cty.Value{
	string(DriftOnErrorFail):     cty.StringVal(string(DriftOnErrorFail)),
	string(DriftOnErrorContinue): cty.StringVal(string(DriftOnErrorContinue)),
}

// parseCheck reads an env `check` block. Only `check "migrate_apply"` holding a
// `drift` block has an effect; every other check, and every other block inside
// it, such as the `allow` and `deny` rules of a pre-execution check, is
// tolerated and reported as ignored, as an unknown block is.
func (p atlasParser) parseCheck(block *hclsyntax.Block, cfg *Config) error {
	if len(block.Labels) != 1 || block.Labels[0] != migrateApplyCheckLabel ||
		!slices.ContainsFunc(block.Body.Blocks, func(nested *hclsyntax.Block) bool { return nested.Type == "drift" }) {
		return p.tolerateUnknownBlock("env", block)
	}
	const scope = "env.check"
	for _, name := range sortedAttributeNames(block.Body.Attributes) {
		if err := p.tolerateUnknownAttr(scope, name, block.Body.Attributes[name]); err != nil {
			return err
		}
	}
	for _, nested := range block.Body.Blocks {
		if nested.Type != "drift" {
			if err := p.tolerateUnknownBlock(scope, nested); err != nil {
				return err
			}
			continue
		}
		// Two drift blocks would leave the run to choose which settings
		// apply, so the second is refused rather than one dropped in
		// silence.
		if cfg.PreApplyDrift != nil {
			return unsupportedBlock(nested)
		}
		drift, err := p.parseDrift(nested)
		if err != nil {
			return err
		}
		cfg.PreApplyDrift = &drift
	}
	return nil
}

// parseDrift reads the drift block of `check "migrate_apply"`.
func (p atlasParser) parseDrift(block *hclsyntax.Block) (PreApplyDrift, error) {
	if len(block.Labels) > 0 {
		return PreApplyDrift{}, unsupportedBlock(block)
	}
	drift := PreApplyDrift{
		OnError:  DriftOnErrorFail,
		Filename: block.TypeRange.Filename,
		Line:     block.TypeRange.Start.Line,
	}
	const scope = "env.check.drift"
	for _, name := range sortedAttributeNames(block.Body.Attributes) {
		attr := block.Body.Attributes[name]
		switch name {
		case "on_error":
			onError, err := p.driftOnError(name, attr)
			if err != nil {
				return PreApplyDrift{}, err
			}
			drift.OnError = onError
		case "exclude":
			values, err := p.stringListAttr(name, attr)
			if err != nil {
				return PreApplyDrift{}, err
			}
			drift.Exclude, drift.ExcludeSet = values, true
		default:
			if err := p.tolerateUnknownAttr(scope, name, attr); err != nil {
				return PreApplyDrift{}, err
			}
		}
	}
	for _, nested := range block.Body.Blocks {
		if err := p.tolerateUnknownBlock(scope, nested); err != nil {
			return PreApplyDrift{}, err
		}
	}
	return drift, nil
}

// driftOnError evaluates `on_error` with FAIL and CONTINUE in scope, so the
// bare identifiers and an expression choosing between them, such as
// `atlas.env == "prod" ? FAIL : CONTINUE`, both read. Any other name is an
// unknown variable, as HCL reports one.
func (p atlasParser) driftOnError(name string, attr *hclsyntax.Attribute) (DriftOnError, error) {
	ctx := &hcl.EvalContext{Variables: atlasDriftOnErrorValues}
	if p.ctx != nil {
		ctx = p.ctx.NewChild()
		ctx.Variables = atlasDriftOnErrorValues
	}
	value, diags := attr.Expr.Value(ctx)
	if diags.HasErrors() {
		return "", p.evaluationFailed(name, attr, diags)
	}
	const want = "FAIL or CONTINUE"
	if value.IsNull() || !value.IsKnown() || value.Type() != cty.String {
		return "", wrongValueType(name, attr, want)
	}
	onError := DriftOnError(strings.TrimSpace(value.AsString()))
	if onError != DriftOnErrorFail && onError != DriftOnErrorContinue {
		return "", wrongValueType(name, attr, want)
	}
	return onError, nil
}
