package migrationfile

import "strings"

// byteOrderMark is U+FEFF, which an editor writes in front of a file to mark
// it as UTF-8. Its encoding is the bytes EF BB BF.
const byteOrderMark = "\uFEFF"

// SQLText returns the SQL text a migration file holds, given the file's
// content: the content with one leading UTF-8 byte order mark removed.
//
// The mark says how the file is encoded and is not part of the SQL. A server
// that receives it as the first character of a statement refuses the
// statement, and a directive on the first line, such as
// `-- +ptah no_transaction` or `-- atlas:txmode none`, is read only once the
// mark is gone. Only one mark at the very start is removed: U+FEFF anywhere
// else is content and stays. A UTF-16 byte order mark (FE FF or FF FE) is not
// removed, because a UTF-16 file is not read as text here at all.
//
// The migrator, the migration linter and the Atlas CE strict-compatibility
// gate read file content through SQLText, and so does [ParseUp]. Sum files
// read the bytes instead: atlas.sum, ptah.sum and the digest of a repeatable
// migration cover the file as it is on disk, mark included.
func SQLText(content string) string {
	return strings.TrimPrefix(content, byteOrderMark)
}
