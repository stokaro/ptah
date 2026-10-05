package lint

import (
	"fmt"

	"ptah.run/internal/ydbsecret"
)

// ydbSchemaSecretInClearRule reports a statement that writes a YDB secret's value
// into the migration file: a CREATE SECRET or ALTER SECRET whose value is a
// literal, or a named expression Ptah does not define from the environment,
// and any statement on the deprecated `OBJECT ... (TYPE SECRET)`.
//
// A migration file is committed, copied into artifacts and printed by plans,
// and the value in it is the credential an external data source connects
// with. Measured on 26.2.1.14 and 25.1.4.7, the deprecated form keeps the
// value in `.metadata/secrets/values`, and every value it ever held in
// `values_history`, both readable in clear by the database administrator.
// The finding names the statement and the secret, never the value, so the
// lint output does not repeat what it reports.
func ydbSchemaSecretInClearRule() Rule {
	return Rule{
		Code:          "YD140",
		Title:         "secret value written in the migration",
		Severity:      SeverityError,
		Dialects:      ydbOnly,
		AppliesToDown: true,
		CheckStatement: func(stmt *Statement) (bool, string) {
			if !ydbRun(stmt.Target) {
				return false, ""
			}
			form, path, writes := ydbsecret.ClearValue(stmt.SQL)
			if !writes {
				return false, ""
			}
			return true, fmt.Sprintf(
				"%s %s writes the secret's value into the migration file, where anyone who reads the file or a plan "+
					"reads it; write `CREATE SECRET %s WITH (value = $%sNAME)` and set %sNAME where the migration "+
					"runs, which Ptah defines when the statement runs and never writes down",
				form, path, path, ydbsecret.ValuePrefix, ydbsecret.ValuePrefix)
		},
	}
}
