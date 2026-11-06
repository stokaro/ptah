//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// mysqlKeyRead reads the keys of a database, which the catalog reads of the statement order
// tests leave out for a plain index.
const mysqlKeyRead = `SELECT CONCAT(table_name, ':', index_name, ':', seq_in_index, ':', column_name)
FROM information_schema.statistics WHERE table_schema = DATABASE()`

// TestCompatMigrateDiffE2E_AnAutoIncrementColumnAndItsPlainKey writes each plan, runs it over a
// row both ways and reads the catalog back after each. MySQL and MariaDB answer 1075 to a
// statement that leaves an AUTO_INCREMENT column without a key, so a replay of the schema is
// the first thing that stops where the index comes after the column. The expected values come
// from the same schema applied directly and from what the database held. The next plan has to
// find nothing left to do in either direction.
func TestCompatMigrateDiffE2E_AnAutoIncrementColumnAndItsPlainKey(t *testing.T) {
	tests := []struct {
		name string
		from string
		seed string
		to   string
	}{
		{
			name: "a new table",
			from: "CREATE TABLE z (id int PRIMARY KEY);\n",
			seed: "INSERT INTO z VALUES (1);\n",
			to:   "CREATE TABLE z (id int PRIMARY KEY);\nCREATE TABLE t (id int NOT NULL AUTO_INCREMENT, k int, KEY t_id (id));\n",
		},
		{
			name: "a table gaining the column",
			from: "CREATE TABLE t (k int PRIMARY KEY);\n",
			seed: "INSERT INTO t VALUES (1);\n",
			to:   "CREATE TABLE t (k int PRIMARY KEY, id int NOT NULL AUTO_INCREMENT, KEY t_id (id));\n",
		},
		{
			name: "a table losing the column",
			from: "CREATE TABLE t (k int PRIMARY KEY, id int NOT NULL AUTO_INCREMENT, KEY t_id (id));\n",
			seed: "INSERT INTO t (k) VALUES (1);\n",
			to:   "CREATE TABLE t (k int PRIMARY KEY);\n",
		},
		{
			name: "a table gaining the column and a key over it and another new column",
			from: "CREATE TABLE t (k int PRIMARY KEY);\n",
			seed: "INSERT INTO t VALUES (1);\n",
			to:   "CREATE TABLE t (k int PRIMARY KEY, id int NOT NULL AUTO_INCREMENT, v int, KEY t_id_v (id, v));\n",
		},
		{
			name: "a table losing the column and a key over it and another column",
			from: "CREATE TABLE t (k int PRIMARY KEY, id int NOT NULL AUTO_INCREMENT, v int, KEY t_id_v (id, v));\n",
			seed: "INSERT INTO t (k) VALUES (1);\n",
			to:   "CREATE TABLE t (k int PRIMARY KEY, v int);\n",
		},
		{
			name: "a column becoming AUTO_INCREMENT",
			from: "CREATE TABLE t (k int PRIMARY KEY, id int NOT NULL);\n",
			seed: "INSERT INTO t VALUES (1, 5);\n",
			to:   "CREATE TABLE t (k int PRIMARY KEY, id int NOT NULL AUTO_INCREMENT, KEY t_id (id));\n",
		},
		{
			name: "a column giving AUTO_INCREMENT up",
			from: "CREATE TABLE t (k int PRIMARY KEY, id int NOT NULL AUTO_INCREMENT, KEY t_id (id));\n",
			seed: "INSERT INTO t (k) VALUES (1);\n",
			to:   "CREATE TABLE t (k int PRIMARY KEY, id int NOT NULL);\n",
		},
		{
			name: "a key that stays while another column changes",
			from: "CREATE TABLE t (k int PRIMARY KEY, id int NOT NULL AUTO_INCREMENT, v int, KEY t_id (id));\n",
			seed: "INSERT INTO t (k) VALUES (1);\n",
			to:   "CREATE TABLE t (k int PRIMARY KEY, id int NOT NULL AUTO_INCREMENT, v bigint, KEY t_id (id));\n",
		},
	}
	for _, engine := range catalogOrderEngines[2:] {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				dev := engine.target(c)
				pairs := readMigrationPairs(c, diffEngineSteps(c, "", dev, []string{test.from, test.to}))
				target, declared := engine.target(c), engine.target(c)
				reads := append([]string{mysqlKeyRead}, engine.reads...)
				c.Assert(pairs, qt.HasLen, 2)

				applyEngineFile(c, declared, engine.dialect, test.to)
				applyEngineFile(c, target, engine.dialect, pairs[0].Up)
				applyEngineFile(c, target, engine.dialect, test.seed)
				before := catalogReadBack(c, target, reads)
				applyEngineFile(c, target, engine.dialect, pairs[1].Up)

				c.Assert(catalogReadBack(c, target, reads), qt.DeepEquals, catalogReadBack(c, declared, reads))
				c.Assert(nextPlanOf(c, target, dev, test.to), qt.Contains, "synced")

				applyEngineFile(c, target, engine.dialect, pairs[1].Down)

				c.Assert(catalogReadBack(c, target, reads), qt.DeepEquals, before)
				c.Assert(nextPlanOf(c, target, dev, test.from), qt.Contains, "synced")
			})
		}
	}
}
