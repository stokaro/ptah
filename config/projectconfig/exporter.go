package projectconfig

import (
	"fmt"
	"sort"

	"github.com/hashicorp/hcl/v2/hclsyntax"
)

// Exporter is a named output template declared by an atlas.hcl `exporter`
// block and selected by `--export`.
//
// # What an exporter is here
//
// A Go text/template over the same document `--format` renders, stored in the
// project config under a name. That is the whole design, and it is deliberately
// small: `schema diff` and `schema inspect` already render their reports
// through templates, so an exporter needs no evaluator of its own -- it is a
// format the project names once and every invocation can ask for by that name.
//
// The alternative was a declarative description of output structure, which
// would have meant a second language to learn, document and version, doing what
// the template surface already does (stokaro/ptah#1620).
type Exporter struct {
	// Name is the block's label, the token `--export` selects.
	Name string
	// Template is the Go text/template body rendered against the report.
	Template string
}

// Exporters returns the declared exporters, ordered by name.
func (c Config) Exporters() []Exporter {
	names := make([]string, 0, len(c.exporters))
	for name := range c.exporters {
		names = append(names, name)
	}
	sort.Strings(names)
	exporters := make([]Exporter, 0, len(names))
	for _, name := range names {
		exporters = append(exporters, Exporter{Name: name, Template: c.exporters[name]})
	}
	return exporters
}

// Exporter returns the exporter a name selects.
//
// A name nothing declares is reported rather than resolved to the default
// output. A caller that could not tell them apart would print the ordinary
// report and let the operator believe their exporter ran.
func (c Config) Exporter(name string) (Exporter, error) {
	body, ok := c.exporters[name]
	if !ok {
		return Exporter{}, &UnknownExporterError{Name: name, Declared: exporterNames(c.exporters)}
	}
	return Exporter{Name: name, Template: body}, nil
}

// UnknownExporterError reports an --export naming no declared exporter.
type UnknownExporterError struct {
	Name string
	// Declared lists what the project does declare, so the message can show
	// the choice rather than only refusing the one that was made.
	Declared []string
}

func (e *UnknownExporterError) Error() string {
	if len(e.Declared) == 0 {
		return fmt.Sprintf(
			"no exporter %q: this project declares no exporter blocks", e.Name)
	}
	return fmt.Sprintf("no exporter %q: this project declares %v", e.Name, e.Declared)
}

func exporterNames(exporters map[string]string) []string {
	names := make([]string, 0, len(exporters))
	for name := range exporters {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// parseAtlasExporterBlock reads one `exporter "name" { template = "..." }`.
func (p atlasParser) parseAtlasExporterBlock(block *hclsyntax.Block, cfg *Config) error {
	if len(block.Labels) != 1 {
		return fmt.Errorf(
			"atlas.hcl exporter block needs exactly one name label, got %d", len(block.Labels))
	}
	name := block.Labels[0]
	if name == "" {
		return fmt.Errorf("atlas.hcl exporter block name is empty")
	}
	if _, exists := cfg.exporters[name]; exists {
		// Two blocks under one name would make --export depend on which the
		// parser reached last, which is not a choice anyone made.
		return fmt.Errorf("atlas.hcl declares exporter %q more than once", name)
	}
	var body string
	var found bool
	for _, attrName := range sortedAttributeNames(block.Body.Attributes) {
		attr := block.Body.Attributes[attrName]
		switch attrName {
		case "template":
			value, err := p.stringAttr(attrName, attr)
			if err != nil {
				return err
			}
			body = value
			found = true
		default:
			// Atlas tolerates unknown names inside a block it does not
			// evaluate; Ptah keeps that and says what it dropped.
			scope := atlasTopLevelScope + ".exporter." + name
			value, diags := attr.Expr.Value(p.ctx)
			if diags.HasErrors() {
				return p.evaluationFailed(attrName, attr, diags)
			}
			if err := checkAtlasToleratedValue(scope, attrName, attr, value); err != nil {
				return err
			}
			p.noteIgnored("attribute", scope+"."+attrName, attr.NameRange)
		}
	}
	if !found {
		return fmt.Errorf("atlas.hcl exporter %q declares no template", name)
	}
	// An empty template is the same user error as a missing one: it renders
	// nothing, so an export that selected it would print nothing at all and
	// exit 0. Refusing it here is what keeps a selected exporter from being
	// indistinguishable from no exporter downstream.
	if body == "" {
		return fmt.Errorf("atlas.hcl exporter %q declares an empty template", name)
	}
	// Child blocks go through the same tolerated-body evaluation an unknown
	// top-level block gets. Before this parser existed, `exporter` followed the
	// unknown-name path, so a nested `metadata { value = var.missing }` was
	// evaluated and refused exactly as Atlas CE refuses it. Walking only
	// Attributes made that configuration succeed while discarding the block --
	// which is this surface being LOOSER than the community binary on a
	// construct it already handled. Measured: the same bad reference inside an
	// unknown `frobnicate` block is still refused, and inside `exporter` it was
	// not (stokaro/ptah#1620).
	for _, child := range block.Body.Blocks {
		if err := p.evaluateIgnoredBody(
			atlasTopLevelScope+".exporter."+name+"."+child.Type, child.Body); err != nil {
			return err
		}
		p.noteIgnored("block", atlasTopLevelScope+".exporter."+name+"."+child.Type, child.TypeRange)
	}
	if cfg.exporters == nil {
		cfg.exporters = make(map[string]string)
	}
	cfg.exporters[name] = body
	return nil
}
