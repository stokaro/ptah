package yqlparse

import (
	"ptah.run/core/ast"
	"ptah.run/internal/ydbcolumn"
	"ptah.run/internal/ydbttl"
)

// tableOptionValue gives TTL ownership of the commas between retention tiers;
// other option values end at the next comma outside parentheses.
func (p *parser) tableOptionValue(name string) string {
	if name != "ttl" {
		return p.expression()
	}
	start := p.peek().Start
	p.ttl()
	end := start
	if p.pos > 0 {
		end = p.tokens[p.pos-1].End
	}
	return p.text[start:end]
}

func (p *parser) ttl() (*ast.YDBTieredTTLSpec, bool) {
	spec := &ast.YDBTieredTTLSpec{}
	explicit := false
	for !p.done() {
		p.wantWord("Interval")
		p.want("(")
		interval := p.expression()
		value, ok := stringLiteral(interval, "String")
		if !ok {
			p.failf("TTL interval must be a string literal")
		}
		p.want(")")
		tier := ast.YDBTTLTierSpec{Interval: value}
		switch {
		case p.word("DELETE"):
			p.pos++
			explicit = true
		case p.word("TO"):
			p.pos++
			p.wantWord("EXTERNAL")
			p.wantWord("DATA")
			p.wantWord("SOURCE")
			tier.ExternalSource = decodedName(p.identifier())
			explicit = true
		}
		spec.Tiers = append(spec.Tiers, tier)
		if !p.accept(",") {
			break
		}
	}
	p.wantWord("ON")
	spec.Column = decodedName(p.identifier())
	if p.word("AS") {
		p.pos++
		spec.Unit = decodedName(p.identifier())
	}
	return spec, explicit
}

func (p *parser) applyTTL(table *ast.CreateTableNode, text string) {
	value := newParser(text)
	spec, explicit := value.ttl()
	if !value.done() {
		value.failf("unexpected text after TTL")
	}
	if value.err != nil {
		p.failf("%v", value.err)
		return
	}
	deletionOnly := len(spec.Tiers) == 1 && spec.Tiers[0].ExternalSource == ""
	var err error
	if !deletionOnly {
		if table.YDBColumnTable == nil {
			p.failf("tiered TTL requires STORE = COLUMN")
			return
		}
		table.YDBColumnTable.TTL = spec
		err = ydbcolumn.Validate(table.YDBColumnTable)
	} else {
		if explicit && table.YDBColumnTable == nil {
			p.failf("TTL DELETE requires STORE = COLUMN")
			return
		}
		table.RowDeletionPolicy = &ast.RowDeletionPolicySpec{Column: spec.Column, Unit: spec.Unit, Interval: spec.Tiers[0].Interval}
		err = ydbttl.Validate(table.RowDeletionPolicy)
	}
	if err != nil {
		p.failf("%v", err)
	}
}
