package capabilityprobe

import (
	"ptah.run/core/platform/capability"
)

// withFullTextKeys asks the YDB full-text grammar. The key names YDB's index,
// not MySQL FULLTEXT, which the YDB provider measures on its own lines, so
// every engine here is asked the statement and its refusal is the
// measurement.
func withFullTextKeys(p plan, dialect string) plan {
	const statement = "ALTER TABLE fti ADD INDEX fti_text GLOBAL USING fulltext_relevance ON (body) WITH (tokenizer=standard, use_filter_lowercase=true)"
	spelling, ok := typeKeySpellingFor(dialect)
	if !ok {
		return p
	}
	p.experiments = append(p.experiments, proven(capability.FullTextIndexes, schemaChange{
		setup: []string{spelling.table("fti", "body "+spelling.integer)}, change: []string{statement},
	}))
	return p
}
