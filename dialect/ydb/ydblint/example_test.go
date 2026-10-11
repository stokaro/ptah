package ydblint_test

import (
	"context"
	"fmt"

	"ptah.run/core/lintext"
	"ptah.run/dialect/ydb/ydblint"
)

// ExampleSplitter shows the YDB owner splitting one lint run's files as YQL:
// each statement carries the facts the rules every dialect shares read, here
// a dropped topic for DS107 and a unique index for MF101 and MF102, and the
// owner's judgment a safety assessment reads, here of the dropped topic.
func ExampleSplitter() {
	request := lintext.SplitRequest{Target: "ydb", Sources: []lintext.Source{{
		Name: "0000000002_events.up.sql", Version: 2, Up: true,
		SQL: "DROP TOPIC `app/events`;\nALTER TABLE users ADD INDEX by_email GLOBAL UNIQUE SYNC ON (email);\n",
	}}}
	result, err := ydblint.Splitter{}.SplitStatements(context.Background(), request)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, statement := range result.Files[0] {
		for _, effect := range statement.Effects {
			if effect.Fact == lintext.FactSafetyEffect {
				fmt.Printf("%d %s %s: %s\n", statement.Line, effect.Fact, effect.Impact, effect.Reason)
				continue
			}
			fmt.Printf("%d %s %s %q\n", statement.Line, effect.Fact, effect.Object, effect.Failure)
		}
	}
	// Output:
	// 1 stateful-drop `app/events` ""
	// 1 safety-effect destructive: DROP TOPIC removes the topic, every message it holds and every consumer's position in it
	// 2 unique-index by_email "YDB: Duplicate key found"
}

// ExampleAnnotator shows the YDB owner judging the drops of a run that names
// no dialect, over the host's dialect-blind split. It declines the drop that
// YQL reads inside a string, where the scanner ended the string at the
// backslash, rather than guess which reading the author meant.
func ExampleAnnotator() {
	const sql = "DROP TOPIC t;\nSELECT 'a\\';\nDROP TOPIC u;\nSELECT '';\n"
	// The statements as a scanner that knows no dialect splits sql.
	statement := func(start, line int, text string, words ...string) lintext.Statement {
		return lintext.Statement{Start: start, End: start + len(text), Line: line, SQL: text, Words: words}
	}
	request := lintext.AnnotateRequest{
		Facts:   []lintext.Fact{lintext.FactStatefulDrop},
		Sources: []lintext.Source{{Name: "0000000001_drop.up.sql", SQL: sql}},
		Files: [][]lintext.Statement{{
			statement(0, 1, "DROP TOPIC t", "DROP", "TOPIC", "T"),
			statement(14, 2, "SELECT 'a\\'", "SELECT", "'a\\'"),
			statement(27, 3, "DROP TOPIC u", "DROP", "TOPIC", "U"),
			statement(41, 4, "SELECT ''", "SELECT", "''"),
		}},
	}
	result, err := ydblint.Annotator{}.AnnotateStatements(context.Background(), request)
	if err != nil {
		fmt.Println(err)
		return
	}
	for i, annotation := range result.Files[0] {
		fmt.Println(request.Files[0][i].SQL, len(annotation.Effects), annotation.Declined)
	}
	// Output:
	// DROP TOPIC t 1 false
	// SELECT 'a\' 0 false
	// DROP TOPIC u 0 true
	// SELECT '' 0 false
}
