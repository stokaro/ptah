package liquibaserun_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/liquibaserun"
)

// Condition recognizes each name in any case, and binds only a value that
// selects something or repeats the changeset. An empty value and `false` are
// Liquibase's defaults.
func TestCondition_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
		kind  liquibaserun.Kind
		binds bool
	}{
		{name: "dbms", key: "dbms", value: "mysql", kind: liquibaserun.Selector, binds: true},
		{name: "empty dbms", key: "dbms", value: " ", kind: liquibaserun.Selector, binds: false},
		{name: "context", key: "context", value: "prod", kind: liquibaserun.Selector, binds: true},
		{name: "contextFilter", key: "contextFilter", value: "prod", kind: liquibaserun.Selector, binds: true},
		{name: "contexts", key: "contexts", value: "prod", kind: liquibaserun.Selector, binds: true},
		{name: "labels", key: "labels", value: "nightly", kind: liquibaserun.Selector, binds: true},
		{name: "preconditions whatever it holds", key: "preConditions", value: "", kind: liquibaserun.Selector, binds: true},
		{name: "upper-case dbms", key: "DBMS", value: "mysql", kind: liquibaserun.Selector, binds: true},
		{name: "runAlways true", key: "runAlways", value: "TRUE", kind: liquibaserun.Repeat, binds: true},
		{name: "alwaysRun true", key: "alwaysRun", value: "true", kind: liquibaserun.Repeat, binds: true},
		{name: "runOnChange true", key: "runOnChange", value: "true", kind: liquibaserun.Repeat, binds: true},
		{name: "runOnChange false", key: "runOnChange", value: "false", kind: liquibaserun.Repeat, binds: false},
		{name: "empty runAlways", key: "runAlways", value: "", kind: liquibaserun.Repeat, binds: false},
		{name: "not a run condition", key: "runInTransaction", value: "false", kind: liquibaserun.None, binds: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			kind, binds, err := liquibaserun.Condition(test.key, test.value)

			c.Assert(err, qt.IsNil)
			c.Assert(kind, qt.Equals, test.kind)
			c.Assert(binds, qt.Equals, test.binds)
		})
	}
}

// A repeat whose value is not a boolean is refused: what the author meant by it
// is unknown.
func TestCondition_FailurePath(t *testing.T) {
	c := qt.New(t)

	kind, binds, err := liquibaserun.Condition("runAlways", "yes")

	c.Assert(err, qt.ErrorMatches, `runAlways "yes" is not true or false`)
	c.Assert(kind, qt.Equals, liquibaserun.Repeat)
	c.Assert(binds, qt.IsFalse)
}

// The zero value holds no condition, and a name is recorded once however often
// it is noted.
func TestConditions_HappyPath(t *testing.T) {
	c := qt.New(t)
	var none liquibaserun.Conditions
	var repeated liquibaserun.Conditions
	repeated.Note("preconditions", "")
	repeated.Note("preconditions", "")
	repeated.AddSelector("<sql> dbms")
	repeated.AddSelector("<sql> dbms")

	c.Assert(none.Err("s:1", "c.sql"), qt.IsNil)
	c.Assert(repeated.Err("s:1", "c.sql"), qt.ErrorMatches,
		`liquibase changeset s:1 in "c.sql" is conditional on preconditions, <sql> dbms; .*`)
}

// Attributes skips the author:id, however it is spelled, and reads a quoted
// value and one written after a blank.
func TestAttributes_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		args string
		want [][2]string
	}{
		{name: "author:id only", args: " dbms:1", want: nil},
		{name: "author named like an attribute", args: " dbms:1 runInTransaction:false", want: [][2]string{{"runInTransaction", "false"}}},
		{name: "quoted value", args: ` s:1 context:"prod or staging"`, want: [][2]string{{"context", "prod or staging"}}},
		{name: "value after a blank", args: " s:1 dbms: mysql labels:x", want: [][2]string{{"dbms", "mysql"}, {"labels", "x"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(liquibaserun.Attributes(test.args), qt.DeepEquals, test.want)
		})
	}
}

// ScanFormattedSQL refuses a file holding a changeset Liquibase runs
// conditionally or more than once, naming the first one, whichever changeset
// in the file it is.
func TestScanFormattedSQL_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		content string
		message string
	}{
		{
			name:    "dbms",
			content: "--liquibase formatted sql\n--changeset s:1 dbms:mysql\nCREATE TABLE t (id int);\n",
			message: `liquibase changeset s:1 in "1_x.sql" is conditional on dbms; a migration directory has no ` +
				`equivalent, so importing it would turn a conditional history into an unconditional one -- ` +
				`split the changelog or import it by hand`,
		},
		{
			name:    "dbms on the first of two changesets",
			content: "--liquibase formatted sql\n--changeset s:1 dbms:mysql\nSELECT 1;\n--changeset s:2\nSELECT 2;\n",
			message: `liquibase changeset s:1 in "1_x.sql" is conditional on dbms; .*`,
		},
		{
			name:    "context on the second changeset",
			content: "--liquibase formatted sql\n--changeset s:1\nSELECT 1;\n--changeset s:2 context:prod\nSELECT 2;\n",
			message: `liquibase changeset s:2 in "1_x.sql" is conditional on context; .*`,
		},
		{
			name:    "labels on the last changeset",
			content: "--liquibase formatted sql\n--changeset s:1\nSELECT 1;\n--changeset s:2 labels:nightly\nSELECT 2;\n",
			message: `liquibase changeset s:2 in "1_x.sql" is conditional on labels; .*`,
		},
		{
			name: "preconditions",
			content: "--liquibase formatted sql\n--changeset s:1\n--preconditions onFail:MARK_RAN\n" +
				"--precondition-sql-check expectedResult:0 SELECT COUNT(*) FROM t\nSELECT 1;\n",
			message: `liquibase changeset s:1 in "1_x.sql" is conditional on preconditions; .*`,
		},
		{
			name:    "runAlways",
			content: "--liquibase formatted sql\n--changeset s:1 runAlways:true\nINSERT INTO t VALUES (1);\n",
			message: `liquibase changeset s:1 in "1_x.sql" sets runAlways, so Liquibase can run it again on a ` +
				`later update; a Ptah migration runs once, so importing it would turn a repeated changeset into a ` +
				`one-time one -- import it by hand`,
		},
		{
			name:    "runOnChange",
			content: "--liquibase formatted sql\n--changeset s:1 runOnChange:true\nCREATE VIEW v AS SELECT 1;\n",
			message: `liquibase changeset s:1 in "1_x.sql" sets runOnChange, .*`,
		},
		{
			name:    "a repeat that is not a boolean",
			content: "--liquibase formatted sql\n--changeset s:1 runOnChange:maybe\nSELECT 1;\n",
			message: `liquibase changeset s:1 in "1_x.sql": runOnChange "maybe" is not true or false`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := liquibaserun.ScanFormattedSQL("1_x.sql", test.content)

			c.Assert(err, qt.ErrorMatches, test.message)
		})
	}
}

// ScanFormattedSQL reads run conditions and nothing else. A layout the
// changeset parser refuses -- a header with no changeset, SQL before the first
// one -- is carried whole by a copy and passes here.
func TestScanFormattedSQL_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{name: "header only", content: "--liquibase formatted sql\n"},
		{name: "no header", content: "CREATE TABLE t (id int);\n"},
		{name: "sql before the first changeset", content: "--liquibase formatted sql\nSELECT 0;\n--changeset s:1\nSELECT 1;\n"},
		{name: "defaults spelled out", content: "--liquibase formatted sql\n--changeset s:1 runAlways:false runOnChange:false dbms:\nSELECT 1;\n"},
		{name: "an attribute that is not a run condition", content: "--liquibase formatted sql\n--changeset atlas:1-1 runInTransaction:false\nSELECT 1;\n"},
		{name: "a comment that mentions a precondition", content: "--liquibase formatted sql\n--changeset s:1\n-- precondition checked by the deploy job\nSELECT 1;\n"},
		{name: "a precondition line before any changeset", content: "--liquibase formatted sql\n--preconditions onFail:HALT\n--changeset s:1\nSELECT 1;\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(liquibaserun.ScanFormattedSQL("1_x.sql", test.content), qt.IsNil)
		})
	}
}
