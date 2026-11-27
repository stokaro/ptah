package schemaprep

import (
	"strings"

	"ptah.run/catalog"
	"ptah.run/internal/mysqlname"
)

// NameRule is how a server compares the schema and relation names a document
// spells with the names a read found, which [RespellReadNamesBy] matches
// them by. The zero value compares names as bytes, and a respelling by it
// changes nothing.
//
// The MySQL family compares database and table names by the
// lower_case_table_names a server was started with ([FoldingRule]). SQL
// Server compares schema and object names under the database collation,
// which only the server answers for ([CatalogRule], [UnansweredCatalogRule]).
type NameRule struct {
	// same reports whether the server reads two spellings as one name; nil
	// compares names as bytes.
	same func(left, right string) bool
	// key is the name the spellings the server reads as one share, which
	// groups the names a document declares.
	key func(string) string
	// known reports that a connection answered how the server compares
	// names. Without the answer a name that would be respelled is refused.
	known bool
	// bodies reports that the server stores a view body with each schema it
	// names spelled as the server holds it, so the qualifiers of a declared
	// body are respelled as well.
	bodies bool
	// schema is the noun a refusal names a schema by.
	schema string
	// setting is what decides how the server compares names, as an
	// [UnknownFoldingError] names it; empty is lower_case_table_names.
	setting string
	// reading is how the server compares names, as an [AmbiguousNameError]
	// says it; empty is without regard to case.
	reading string
}

// FoldingRule is the rule of a MySQL or MariaDB server whose folding is
// folding, from [catalog.ServerInfo.NameFolding]: names compare as bytes on a
// server that keeps them apart by case, without regard to case on one that
// folds them, and a server whose folding no connection read is a rule that
// refuses what it would respell.
func FoldingRule(folding catalog.NameFolding) NameRule {
	if folding == catalog.NameFoldingExact {
		return NameRule{}
	}
	return NameRule{
		same:   mysqlname.NamesOf(true).Same,
		key:    strings.ToLower,
		known:  folding == catalog.NameFoldingFolds,
		bodies: true,
		schema: "database",
	}
}

// CatalogRule is the rule of a server that answered how its catalog compares
// names: two spellings are one name where key gives them one key. A view
// body keeps the spelling it was written with, since such a server stores
// the body as written.
func CatalogRule(key func(string) string) NameRule {
	return NameRule{
		same:    func(left, right string) bool { return key(left) == key(right) },
		key:     key,
		known:   true,
		schema:  "schema",
		reading: "under the database collation",
	}
}

// UnansweredCatalogRule is the rule of a server whose catalog compares names
// under the database collation when no connection asked it how: two names that
// differ only in case may be one name, so a name that matches another only
// without regard to case is refused.
func UnansweredCatalogRule() NameRule {
	return NameRule{
		same:    strings.EqualFold,
		key:     strings.ToLower,
		schema:  "schema",
		setting: "the database collation",
		reading: "under the database collation",
	}
}

// Exact reports whether the rule compares names as bytes, so that a
// respelling by it changes nothing.
func (r NameRule) Exact() bool {
	return r.same == nil
}
