package engine

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"ptah.run/core/lintext"
	"ptah.run/core/ptaherr"
)

// Lint publishes the migration lint rules one owner checks and the service
// that checks them. A rule names the targets it judges, each one a target
// the runtime registers, and a code belongs to one rule across every
// provider of the runtime.
type Lint struct {
	// Rules are the rules the service checks, as data.
	Rules []lintext.Rule
	// Service checks them, once per lint run.
	Service lintext.Service
}

// ownedLintRule is a published rule and the service that checks it.
type ownedLintRule struct {
	owner   string
	rule    lintext.Rule
	service int
}

func (r *Runtime) registerLint(owner string, declaration Lint) error {
	if declaration.Service == nil || nilService(declaration.Service) || len(declaration.Rules) == 0 {
		return fmt.Errorf("%w: provider %q publishes lint rules without a service, or a service without rules", ErrInvalidRegistration, owner)
	}
	service := len(r.lintServices)
	r.lintServices = append(r.lintServices, declaration.Service)
	for _, rule := range declaration.Rules {
		if err := rule.Validate(); err != nil {
			return fmt.Errorf("%w: provider %q: %w", ErrInvalidRegistration, owner, err)
		}
		if existing, found := r.lintRules[rule.Code]; found {
			return fmt.Errorf("%w: lint rule %s is published by %q and %q", ErrInvalidRegistration, rule.Code, existing.owner, owner)
		}
		published := rule.Clone()
		for i, name := range published.Targets {
			selected, found := r.lookup(name)
			if !found {
				return fmt.Errorf("%w: lint rule %s judges unregistered target %q", ErrInvalidRegistration, rule.Code, name)
			}
			published.Targets[i] = selected.name
		}
		r.lintRules[rule.Code] = ownedLintRule{owner: owner, rule: published, service: service}
	}
	return nil
}

// LintRules returns the published rules that judge target, every published
// rule for an empty target, sorted by code. The rules are copies, each
// naming canonical targets. An unregistered target wraps
// ptaherr.ErrUnsupportedDialect.
func (r *Runtime) LintRules(target string) ([]lintext.Rule, error) {
	name, err := r.lintTarget(target)
	if err != nil {
		return nil, err
	}
	var rules []lintext.Rule
	if r != nil {
		for _, owned := range r.lintRules {
			if owned.rule.Judges(name) {
				rules = append(rules, owned.rule.Clone())
			}
		}
	}
	slices.SortFunc(rules, func(a, b lintext.Rule) int { return cmp.Compare(a.Code, b.Code) })
	return rules, nil
}

// SplitStatements splits a lint run's sources with the splitter of the
// request's target, under its canonical name. A target that is not
// registered or registers no splitter wraps ptaherr.ErrUnsupportedDialect, a
// reply that does not answer the request wraps lintext.ErrInvalid, and an
// error returns no result.
func (r *Runtime) SplitStatements(ctx context.Context, request lintext.SplitRequest) (lintext.SplitResult, error) {
	if ctx == nil {
		return lintext.SplitResult{}, fmt.Errorf("%w: splitting requires a context", lintext.ErrInvalid)
	}
	selected, found := r.lookup(request.Target)
	if !found || selected.splitter == nil {
		return lintext.SplitResult{}, fmt.Errorf("%w: no statement splitter for target %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	request.Target = selected.name
	request.Sources = slices.Clone(request.Sources)
	result, err := selected.splitter.SplitStatements(ctx, request)
	if err != nil {
		return lintext.SplitResult{}, err
	}
	if err := lintext.ValidateSplit(request, result); err != nil {
		return lintext.SplitResult{}, fmt.Errorf("statement splitter of %q: %w", selected.owner, err)
	}
	return result, ctx.Err()
}

// CheckMigrations sends each of the request's codes to the owner that
// publishes it, one call per owner, and joins the findings, sorted by file,
// statement and code, and the baseline subjects, sorted by file and code. A
// code no owner publishes, a rule that does not judge the request's target and
// an unregistered target are refused before any owner is called; a reply that
// does not answer its request, or names baseline subjects for a rule whose
// input reads no baseline, wraps lintext.ErrInvalid. An error returns no
// result.
func (r *Runtime) CheckMigrations(ctx context.Context, request lintext.CheckRequest) (lintext.CheckResult, error) {
	if ctx == nil {
		return lintext.CheckResult{}, fmt.Errorf("%w: checking requires a context", lintext.ErrInvalid)
	}
	name, err := r.lintTarget(request.Target.Name)
	if err != nil {
		return lintext.CheckResult{}, err
	}
	request.Target.Name = name
	batches := make(map[int][]string)
	var order []int
	for i, code := range request.Codes {
		var owned ownedLintRule
		found := false
		if r != nil {
			owned, found = r.lintRules[code]
		}
		if !found || !owned.rule.Judges(name) || slices.Contains(request.Codes[:i], code) {
			return lintext.CheckResult{}, fmt.Errorf("%w: no published lint rule %s judges target %q, or it is asked for twice", lintext.ErrInvalid, code, name)
		}
		if _, seen := batches[owned.service]; !seen {
			order = append(order, owned.service)
		}
		batches[owned.service] = append(batches[owned.service], code)
	}
	result := lintext.CheckResult{Complete: true}
	for _, service := range order {
		if err := ctx.Err(); err != nil {
			return lintext.CheckResult{}, err
		}
		batch := request
		batch.Codes = batches[service]
		reply, err := r.lintServices[service].CheckMigrations(ctx, batch)
		if err != nil {
			return lintext.CheckResult{}, err
		}
		owner := r.lintRules[batch.Codes[0]].owner
		if err := lintext.ValidateCheck(batch, reply); err != nil {
			return lintext.CheckResult{}, fmt.Errorf("lint rules of %q: %w", owner, err)
		}
		for _, subject := range reply.BaselineSubjects {
			if !r.lintRules[subject.Code].rule.Input.ReadsBaseline() {
				return lintext.CheckResult{}, fmt.Errorf("lint rules of %q: %w: rule %s names baseline subjects but reads no baseline",
					owner, lintext.ErrInvalid, subject.Code)
			}
		}
		result.Findings = append(result.Findings, reply.Findings...)
		result.BaselineSubjects = append(result.BaselineSubjects, reply.BaselineSubjects...)
	}
	slices.SortStableFunc(result.Findings, func(a, b lintext.Finding) int {
		return cmp.Or(cmp.Compare(a.File, b.File), cmp.Compare(a.Statement, b.Statement), cmp.Compare(a.Code, b.Code))
	})
	slices.SortStableFunc(result.BaselineSubjects, func(a, b lintext.BaselineSubject) int {
		return cmp.Or(cmp.Compare(a.File, b.File), cmp.Compare(a.Code, b.Code))
	})
	return result, ctx.Err()
}

// lintTarget resolves the target a lint call names to its canonical name. An
// empty name stays empty: a run that names no target.
func (r *Runtime) lintTarget(name string) (string, error) {
	if name == "" {
		return "", nil
	}
	selected, found := r.lookup(name)
	if !found {
		return "", fmt.Errorf("%w: lint target %q is not registered", ptaherr.ErrUnsupportedDialect, name)
	}
	return selected.name, nil
}
