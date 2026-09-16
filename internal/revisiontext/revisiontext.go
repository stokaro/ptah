// Package revisiontext renders the text a migration revision row records about
// the statement a migration stopped on.
//
// It sits below migration/migrator because what the revision table can store is
// a property of the column rather than of the migrator: the revision table's
// text columns carry a character set, and a migration statement carries
// whatever bytes its author wrote. The two meet when the migrator records
// progress, and a statement the server would accept must not be refused by the
// bookkeeping that precedes it.
package revisiontext

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// ValidUTF8 returns value as text every UTF-8 column accepts, rendering each
// byte that is not part of a valid UTF-8 sequence as \xNN with upper-case hex
// digits. Valid UTF-8 is returned unchanged, byte for byte, so an ordinary
// statement is recorded exactly as it was written.
//
// Every value bound to a revision row's error and error_stmt columns goes
// through this function. A migration statement is bytes the author chose, and
// only the server can say whether it is acceptable SQL; MySQL and MariaDB
// answer Error 1366 for a utf8mb4 column handed a byte sequence that is not
// valid UTF-8, so a statement recorded raw fails in the progress write before
// the statement itself is ever sent (stokaro/ptah#3316).
//
// The rendering is for a reader, not for a decoder. Escaping is not reversible
// here: a statement that literally contains the four characters \xFF renders
// the same way as one carrying that byte. The columns are read to identify the
// statement a migration stopped on -- by repair, by `ptah migrations status`,
// and by the dirty-revision message -- and for that, naming the byte beats both
// losing it and refusing to record anything at all.
func ValidUTF8(value string) string {
	if utf8.ValidString(value) {
		return value
	}
	var out strings.Builder
	out.Grow(len(value))
	for index := 0; index < len(value); {
		decoded, size := utf8.DecodeRuneInString(value[index:])
		if decoded == utf8.RuneError && size <= 1 {
			fmt.Fprintf(&out, `\x%02X`, value[index])
			index++
			continue
		}
		out.WriteString(value[index : index+size])
		index += size
	}
	return out.String()
}
