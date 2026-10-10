package lint

import (
	"fmt"
	"strings"

	"ptah.run/dialect/ydb/ydbexternal"
)

// ydbDeprecatedSecretRule reports an external data source that names a
// credential by a deprecated secret object, in an option ending in
// _SECRET_NAME.
//
// Measured on 25.1.4.7 and 26.2.1.14, the deprecated object keeps its value
// in `.metadata/secrets/values`, and every value it ever held in
// `values_history`, both readable in clear by the database administrator, and
// the user who created it cannot list it. YDB 25.4 and later take the path of
// a YDB secret instead, in the matching _SECRET_PATH option. The rule warns
// rather than refuses: 25.1, the oldest line Ptah measured, has no other way
// to give a data source a credential.
func ydbDeprecatedSecretRule() Rule {
	return Rule{
		Code:          "YD141",
		Title:         "external data source credential in a deprecated secret object",
		Severity:      SeverityWarning,
		Dialects:      ydbOnly,
		AppliesToDown: true,
		CheckStatement: func(stmt *Statement) (bool, string) {
			if !ydbRun(stmt.Target) {
				return false, ""
			}
			path, options := ydbexternal.DeprecatedSecretNames(stmt.SQL)
			if len(options) == 0 {
				return false, ""
			}
			replacements := make([]string, 0, len(options))
			for _, option := range options {
				replacements = append(replacements, strings.TrimSuffix(option, "_NAME")+"_PATH")
			}
			return true, fmt.Sprintf(
				"external data source %s names its credential by %s, a deprecated secret object whose value the "+
					"database administrator reads in clear from .metadata/secrets; on YDB 25.4 and later, keep the "+
					"value in a YDB secret and name it by %s",
				path, strings.Join(options, " and "), strings.Join(replacements, " and "))
		},
	}
}
