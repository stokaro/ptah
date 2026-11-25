package mysql

import (
	"fmt"
	"strings"
)

// refuseStoredProgramMode keeps a body whose parsing rules the model cannot
// carry out of every plan, export and rollback derived from a catalog read.
// Routines and triggers use the same predicate: a mode can change the meaning
// of either body. Matching the current session is not enough, because a saved
// migration or exported schema can run in another session. The current session
// is checked too: a default-mode body recreated under PIPES_AS_CONCAT changes
// its meaning in the other direction.
func refuseStoredProgramMode(kind, database, name, stored, session string) error {
	for _, source := range []struct {
		name string
		mode string
	}{
		{name: "stored", mode: stored},
		{name: "session", mode: session},
	} {
		if mode := programParsingMode(source.mode); mode != "" {
			return fmt.Errorf("cannot read %s %q: %s sql_mode includes %s, which Ptah cannot preserve "+
				"when recreating the body; manage this object in migration SQL that restores its stored sql_mode",
				kind, database+"."+name, source.name, mode)
		}
	}
	return nil
}

// programParsingMode names the first mode that changes a body's operators,
// expression types, evaluation order or procedural grammar. Composite modes
// are included even when a catalog reports them without their expanded flags.
// Lexical modes, such as ANSI_QUOTES and NO_BACKSLASH_ESCAPES, are separate:
// they change token spelling rather than the operations a body performs.
func programParsingMode(modes string) string {
	for mode := range strings.SplitSeq(modes, ",") {
		mode = strings.ToUpper(strings.TrimSpace(mode))
		switch mode {
		case "PIPES_AS_CONCAT", "HIGH_NOT_PRECEDENCE", "IGNORE_SPACE", "REAL_AS_FLOAT",
			"EMPTY_STRING_IS_NULL", "SIMULTANEOUS_ASSIGNMENT", "NO_UNSIGNED_SUBTRACTION",
			"ANSI", "ORACLE", "DB2", "MAXDB", "MSSQL", "POSTGRESQL", "MYSQL323", "MYSQL40":
			return mode
		}
	}
	return ""
}
