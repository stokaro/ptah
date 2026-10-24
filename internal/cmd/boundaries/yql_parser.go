package main

import (
	"fmt"

	"golang.org/x/tools/go/packages"
)

// yqlParser is the YQL parser. ADR 0027 moves it, with the YDB owner's SQL
// reading, into the YDB provider: a provider that reads its SQL documents
// whole parses them itself, and the host keeps no grammar of the engine.
//
// Until that move the two packages of the in-process YDB reading import it:
// internal/ydbsql reads a load, and internal/syntaxext/yql serves the YQL
// lexer to syntax readers. The count may fall and may never rise, so a third
// importer is refused, and the move records zero.
const yqlParser = "internal/yqlparse"

// yqlParserScanFloor is the fewest packages the rule must read. The rule
// answers from the packages the loader returns, and a load that stopped
// matching most of the module would report no importer and read as success.
// The figure is a source constant, so a regenerated baseline cannot lower it;
// the module holds several hundred more.
const yqlParserScanFloor = 500

// yqlParserImporters reports each package of the module, other than the parser
// itself, that imports the YQL parser directly. Tests are not loaded: a test
// may parse YQL to state what the provider has to read.
func yqlParserImporters(pkg *packages.Package) []finding {
	return importEdges(pkg, func(rel string) bool { return rel != yqlParser }, func(rel string) bool { return rel == yqlParser })
}

// checkYQLParserScan refuses a load too small for the rule to mean anything.
func checkYQLParserScan(loaded []*packages.Package) error {
	if len(loaded) < yqlParserScanFloor {
		return fmt.Errorf("rule %q reads %d packages, below its floor of %d", yqlParserRule, len(loaded), yqlParserScanFloor)
	}
	return nil
}

const yqlParserRule = "yql-parser-importers"
