package capabilityprobe

import (
	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
)

// withFullTextKeys measures the YDB full-text grammar. Other engines are asked
// the same statement because this key names YDB's index, not MySQL FULLTEXT.
func withFullTextKeys(p plan, dialect string) plan {
	const statement = "ALTER TABLE fti ADD INDEX fti_text GLOBAL USING fulltext_relevance ON (body) WITH (tokenizer=standard, use_filter_lowercase=true)"
	if dialect == platform.YDB {
		p.experiments = append(p.experiments, proven(capability.FullTextIndexes, schemaChange{
			setup:  []string{ydbSpelling.table("fti", "id Uint64 NOT NULL, body Utf8", "id")},
			change: []string{statement},
			after: []check{ydbDescribedIndex("fti", "fti_text", "a full-text relevance index with its analyzer settings", func(index catalog.Index) bool {
				return index.Method == "GLOBAL USING fulltext_relevance" && index.StorageParams["tokenizer"] == "standard" && index.StorageParams["use_filter_lowercase"] == "true"
			})},
		}))
		return p
	}
	spelling, ok := typeKeySpellingFor(dialect)
	if !ok {
		return p
	}
	p.experiments = append(p.experiments, proven(capability.FullTextIndexes, schemaChange{
		setup: []string{spelling.table("fti", "body "+spelling.integer)}, change: []string{statement},
	}))
	return p
}
