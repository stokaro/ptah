package mysqlindex_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/mysqlindex"
)

func TestBlockSizes_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name, ddl string
		want      map[string]uint64
	}{
		{name: "server output", ddl: "CREATE TABLE `t` (`a` int, PRIMARY KEY (`a`) KEY_BLOCK_SIZE=4, KEY `k` (`a`) KEY_BLOCK_SIZE=8) ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=16", want: map[string]uint64{"PRIMARY": 4, "k": 8}},
		{name: "quoted text and expressions", ddl: "CREATE TABLE `KEY_BLOCK_SIZE` (`a` varchar(30) DEFAULT 'KEY_BLOCK_SIZE=64', KEY `k``x` ((concat(`a`, ',) KEY_BLOCK_SIZE=32'))) KEY_BLOCK_SIZE = 8 COMMENT 'KEY_BLOCK_SIZE=16', UNIQUE KEY `u` (`a`(7)) KEY_BLOCK_SIZE=4) COMMENT='KEY_BLOCK_SIZE=128'", want: map[string]uint64{"k`x": 8, "u": 4}},
		{name: "absent", ddl: "CREATE TABLE t (`KEY_BLOCK_SIZE` int, KEY k (`KEY_BLOCK_SIZE`) COMMENT 'KEY_BLOCK_SIZE=8') KEY_BLOCK_SIZE=16", want: make(map[string]uint64)},
		{name: "zero", ddl: "CREATE TABLE t (a int, KEY k(a) KEY_BLOCK_SIZE=0)", want: map[string]uint64{"k": 0}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := mysqlindex.BlockSizes(test.ddl)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

func TestBlockSizes_FailurePath(t *testing.T) {
	for _, ddl := range []string{"CREATE TABLE t (a int, KEY k(a) KEY_BLOCK_SIZE=)", "CREATE TABLE t (a int, KEY k(a) KEY_BLOCK_SIZE=bad)", "CREATE TABLE t (a int"} {
		t.Run(ddl, func(t *testing.T) {
			c := qt.New(t)
			_, err := mysqlindex.BlockSizes(ddl)
			c.Assert(err, qt.IsNotNil)
		})
	}
}
