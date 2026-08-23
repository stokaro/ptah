//go:build integration

package migratedirquery_test

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	qt "github.com/frankban/quicktest"
	_ "modernc.org/sqlite"
)

var (
	generatedQueryMigration = regexp.MustCompile(`\b\d{14}_query_contract\b`)
	queryRunDuration        = regexp.MustCompile(`\b\d+(?:\.\d+)?(?:ns|µs|ms|s)\b`)
)

type queryDirectoryState struct {
	Files     map[string]string
	Databases map[string]queryDatabaseState
}

type queryDatabaseState struct {
	Schema []string
	Rows   map[string][]string
}

func queryRunState(c *qt.C, root string) queryDirectoryState {
	c.Helper()
	state := queryDirectoryState{
		Files:     make(map[string]string),
		Databases: make(map[string]queryDatabaseState),
	}
	opened, err := os.OpenRoot(root)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(opened.Close(), qt.IsNil) })
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if filepath.Ext(path) == ".db" {
			state.Databases[relative] = readQueryDatabaseState(c, path)
			return nil
		}
		data, err := opened.ReadFile(relative)
		if err != nil {
			return err
		}
		state.Files[normalizeGeneratedMigration(relative)] = normalizeQueryFile(relative, string(data))
		return nil
	})
	c.Assert(err, qt.IsNil)
	return state
}

func readQueryDatabaseState(c *qt.C, path string) queryDatabaseState {
	c.Helper()
	db, err := sql.Open("sqlite", path)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })

	state := queryDatabaseState{Rows: make(map[string][]string)}
	rows, err := db.Query(`SELECT type, name, COALESCE(sql, '') FROM sqlite_master
WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name`)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var objectType, name, statement string
		c.Assert(rows.Scan(&objectType, &name, &statement), qt.IsNil)
		state.Schema = append(state.Schema, objectType+"|"+name+"|"+statement)
		if objectType == "table" {
			tables = append(tables, name)
		}
	}
	c.Assert(rows.Err(), qt.IsNil)
	for _, table := range tables {
		state.Rows[table] = readQueryTableRows(c, db, table)
	}
	return state
}

func readQueryTableRows(c *qt.C, db *sql.DB, table string) []string {
	c.Helper()
	identifier := `"` + strings.ReplaceAll(table, `"`, `""`) + `"`
	rows, err := db.Query("SELECT * FROM " + identifier) // #nosec G202 -- table comes from sqlite_master and is quoted above
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	columns, err := rows.Columns()
	c.Assert(err, qt.IsNil)
	result := make([]string, 0)
	for rows.Next() {
		values := make([]any, len(columns))
		destinations := make([]any, len(columns))
		for index := range values {
			destinations[index] = &values[index]
		}
		c.Assert(rows.Scan(destinations...), qt.IsNil)
		parts := make([]string, len(columns))
		for index, value := range values {
			parts[index] = columns[index] + "=" + normalizeQueryValue(columns[index], value)
		}
		result = append(result, strings.Join(parts, "|"))
	}
	c.Assert(rows.Err(), qt.IsNil)
	slices.Sort(result)
	return result
}

func normalizeQueryValue(column string, value any) string {
	if column == "executed_at" || column == "execution_time" {
		return "<volatile>"
	}
	switch typed := value.(type) {
	case nil:
		return "<null>"
	case []byte:
		return "bytes:" + hex.EncodeToString(typed)
	default:
		return fmt.Sprintf("%T:%v", value, value)
	}
}

func normalizeQueryRunOutput(output, root string) string {
	output = strings.ReplaceAll(output, root, "<root>")
	output = normalizeGeneratedMigration(output)
	return queryRunDuration.ReplaceAllString(output, "<duration>")
}

func normalizeGeneratedMigration(value string) string {
	return generatedQueryMigration.ReplaceAllString(value, "<version>_query_contract")
}

func normalizeQueryFile(relative, content string) string {
	content = normalizeGeneratedMigration(content)
	if filepath.Base(relative) != "atlas.sum" || !strings.Contains(content, "<version>_query_contract.sql") {
		return content
	}
	lines := strings.Split(content, "\n")
	if len(lines) > 0 {
		lines[0] = "h1:<root>"
	}
	for index, line := range lines {
		if strings.HasPrefix(line, "<version>_query_contract.sql ") {
			lines[index] = "<version>_query_contract.sql h1:<generated>"
		}
	}
	return strings.Join(lines, "\n")
}
