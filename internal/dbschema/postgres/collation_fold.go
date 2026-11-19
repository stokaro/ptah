package postgres

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// missingCollation matches PostgreSQL's refusal of a collation it does not
// find, SQLSTATE 42704, and captures the name it looked for. Measured on
// PostgreSQL 18.6: `collation "c" for encoding "UTF8" does not exist`. A server
// that answers in another language matches nothing, and the refusal goes out
// without a hint.
var missingCollation = regexp.MustCompile(`^collation "(.+)" for encoding "[^"]*" does not exist$`)

// shippedCollations are the collations PostgreSQL ships under a name with an
// upper-case letter, keyed by the name an unquoted spelling folds to.
var shippedCollations = map[string]string{"c": "C", "posix": "POSIX"}

// collationFoldHint is the line Ptah adds to the server's refusal of a
// collation whose name holds no upper-case letter, and empty for any other
// error. A SQL schema file folds an unquoted collation name to lower case, as
// PostgreSQL does, so `COLLATE C` names "c", which PostgreSQL does not ship.
// The rendered statement quotes every name, `COLLATE "c"` for an unquoted C
// and for a quoted "c" alike, so the hint cannot know which the author wrote
// and says what follows if the name was unquoted.
func collationFoldHint(err error) string {
	serverErr, ok := errors.AsType[*pgconn.PgError](err)
	if !ok || serverErr.Code != "42704" {
		return ""
	}
	parts := missingCollation.FindStringSubmatch(serverErr.Message)
	if parts == nil || parts[1] != strings.ToLower(parts[1]) || parts[1] == strings.ToUpper(parts[1]) {
		return ""
	}
	if shipped, found := shippedCollations[parts[1]]; found {
		return fmt.Sprintf("\nhint: if the name was written unquoted, it folded to lower case: COLLATE %s names %q; "+
			"write COLLATE %q", shipped, parts[1], shipped)
	}
	return fmt.Sprintf("\nhint: if the name was written unquoted, it folded to lower case, and %q may stand for a "+
		"collation spelled with upper-case letters; write the name in double quotes to keep its case", parts[1])
}
