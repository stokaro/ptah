package projectconfig_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
)

// parseLintRules parses a project file and returns its lint rule configs.
func parseLintRules(c *qt.C, raw string) map[string]projectconfig.LintRuleConfig {
	c.Helper()
	cfg, err := projectconfig.ParseAtlas([]byte(raw), "atlas.hcl", "local")
	c.Assert(err, qt.IsNil)
	return cfg.Lint.RuleConfigs
}

// TestParseAtlas_LintRuleBlockDeclaresARule is the project-file half of
// stokaro/ptah#1706.
//
// The `match` expression is captured as SOURCE rather than evaluated: it is a
// predicate over a statement that does not exist while the project file is
// being read. Evaluating it here would fail on `statement` being undefined --
// and succeeding would be worse, since it would freeze one statement's answer
// for every file.
func TestParseAtlas_LintRuleBlockDeclaresARule(t *testing.T) {
	c := qt.New(t)

	rules := parseLintRules(c, `env "local" {
  url = "sqlite://app.db"

  lint {
    rule "NOVARCHAR" {
      severity        = "error"
      title           = "varchar(n) instead of text"
      match           = strcontains(lower(statement.sql), "varchar(")
      message         = "use text, not varchar(n)"
      dialects        = ["postgres"]
      applies_to_down = true
    }
  }
}
`)

	c.Assert(rules, qt.HasLen, 1)
	rule := rules["NOVARCHAR"]
	c.Assert(rule.Match, qt.Equals, `strcontains(lower(statement.sql), "varchar(")`)
	c.Assert(rule.Message, qt.Equals, "use text, not varchar(n)")
	c.Assert(rule.Title, qt.Equals, "varchar(n) instead of text")
	c.Assert(rule.Severity, qt.Equals, "error")
	c.Assert(rule.Dialects, qt.DeepEquals, []string{"postgres"})
	c.Assert(rule.AppliesToDown, qt.IsTrue)
}

// TestParseAtlas_LintRuleBlocksMayRepeat covers the arity that made this block
// different from every other one under `lint`.
//
// A project has as many rules as it has conventions, so `rule` is a collection
// rather than a singleton. The structure walk had keyed its duplicate check on
// the block TYPE alone, which accepted the first rule and refused the second.
func TestParseAtlas_LintRuleBlocksMayRepeat(t *testing.T) {
	c := qt.New(t)

	rules := parseLintRules(c, `env "local" {
  url = "sqlite://app.db"

  lint {
    rule "NOVARCHAR" {
      match   = strcontains(statement.sql, "varchar(")
      message = "use text"
    }

    rule "NOTIMESTAMP" {
      match   = contains(statement.words, "TIMESTAMP")
      message = "use timestamptz"
    }
  }
}
`)

	c.Assert(rules, qt.HasLen, 2)
	c.Assert(rules["NOVARCHAR"].Message, qt.Equals, "use text")
	c.Assert(rules["NOTIMESTAMP"].Message, qt.Equals, "use timestamptz")
}

// TestParseAtlas_LintRuleKeepsTheAtlasSpellingIgnored is the compatibility half.
//
// One block name carries two meanings: Atlas spells a custom rule
// `rule "hcl" "name" { src = [...] }` and CE ignores it. The label count is
// what tells them apart, and honoring the Atlas form would run a rule its
// author wrote for another tool.
func TestParseAtlas_LintRuleKeepsTheAtlasSpellingIgnored(t *testing.T) {
	c := qt.New(t)

	cfg, err := projectconfig.ParseAtlas([]byte(`env "local" {
  url = "sqlite://app.db"

  lint {
    rule "hcl" "atlas_form" {
      src = ["schema.rule.hcl"]
    }

    rule "NOVARCHAR" {
      match   = strcontains(statement.sql, "varchar(")
      message = "use text"
    }
  }
}
`), "atlas.hcl", "local")

	c.Assert(err, qt.IsNil)
	// The two-label form is reported as having no effect...
	names := make([]string, 0, len(cfg.IgnoredConstructs))
	for _, ignored := range cfg.IgnoredConstructs {
		names = append(names, ignored.Name)
	}
	c.Assert(names, qt.Contains, "rule")
	// ...and the one-label form is a declaration, not an ignored construct.
	c.Assert(cfg.Lint.RuleConfigs, qt.HasLen, 1)
	c.Assert(cfg.Lint.RuleConfigs["NOVARCHAR"].Match, qt.Equals, `strcontains(statement.sql, "varchar(")`)
}

// TestParseAtlas_LintRuleRefusesADuplicateCode keeps two declarations of one
// code from resolving to whichever the parser reached last.
func TestParseAtlas_LintRuleRefusesADuplicateCode(t *testing.T) {
	c := qt.New(t)

	_, err := projectconfig.ParseAtlas([]byte(`env "local" {
  url = "sqlite://app.db"

  lint {
    rule "NOVARCHAR" {
      match   = strcontains(statement.sql, "a")
      message = "first"
    }

    rule "NOVARCHAR" {
      match   = strcontains(statement.sql, "b")
      message = "second"
    }
  }
}
`), "atlas.hcl", "local")

	c.Assert(err, qt.ErrorMatches, `duplicate rule "NOVARCHAR" at atlas.hcl:10`)
}

// TestParseAtlas_LintRuleMatchIsNotEvaluated is the property stated directly.
//
// The expression names `statement`, which no project-file scope defines. If the
// attribute were evaluated like every other one, this file would fail to parse
// -- which is exactly how the feature failed the first time it was wired up.
func TestParseAtlas_LintRuleMatchIsNotEvaluated(t *testing.T) {
	c := qt.New(t)

	rules := parseLintRules(c, `env "local" {
  url = "sqlite://app.db"

  lint {
    rule "NOVARCHAR" {
      match   = strcontains(lower(statement.sql), "varchar(") && !file.is_down
      message = "use text"
    }
  }
}
`)

	c.Assert(rules["NOVARCHAR"].Match, qt.Equals,
		`strcontains(lower(statement.sql), "varchar(") && !file.is_down`)
}

// TestParseAtlas_LintRuleSurvivesTheGlobalEnvMerge covers the arm that made a
// declared rule parse and then vanish.
//
// Severity and Exclude are OVERRIDES merged field by field; the declaration is
// one indivisible definition. Rebuilding the entry from severity and exclude
// alone dropped `match`, leaving an entry that configured a rule which was
// never defined -- and the run failed with "does not match any registered
// rule", pointing at the rule's own code.
func TestParseAtlas_LintRuleSurvivesTheGlobalEnvMerge(t *testing.T) {
	c := qt.New(t)

	rules := parseLintRules(c, `lint {
  rule "GLOBALRULE" {
    match   = contains(statement.words, "DROP")
    message = "global"
  }
}

env "local" {
  url = "sqlite://app.db"

  lint {
    rule "ENVRULE" {
      match   = contains(statement.words, "TRUNCATE")
      message = "env"
    }
  }
}
`)

	c.Assert(rules["ENVRULE"].Match, qt.Equals, `contains(statement.words, "TRUNCATE")`)
	c.Assert(rules["ENVRULE"].Message, qt.Equals, "env")
	c.Assert(rules["GLOBALRULE"].Match, qt.Equals, `contains(statement.words, "DROP")`)
	c.Assert(rules["GLOBALRULE"].Message, qt.Equals, "global")
}
