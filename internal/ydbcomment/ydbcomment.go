// Package ydbcomment stores Ptah's comments on YDB objects.
//
// YQL has no COMMENT statement and no comment clause: measured on 25.1.4.7
// and 26.2.1.14, `COMMENT ON TABLE` and a column's `COMMENT` are parse errors,
// and `WITH (COMMENT = ...)` answers `Unknown table setting`. What YDB has is
// a user attribute, a key and a value on a scheme object, which the table
// service sets (AlterTable's alter_attributes) and DescribeTable reports. A
// row table and a view hold attributes; an index, a directory and the
// database root do not (`PathNotTable`), and a topic refuses any key it does
// not know. So a comment is an attribute of the table or view it belongs to,
// and a column's and an index's comment live on their table, under a key that
// names them; see [Key].
//
// The package names the keys, holds YDB's limits on them, reads a comment
// back out of an object's attributes, and writes and reads the statement
// Ptah's YDB connection runs through the table service: `COMMENT ON TABLE`,
// `COLUMN`, `INDEX ... ON` and `VIEW`, in the PostgreSQL family's grammar.
// The statement is Ptah's own; another client sending it to YDB gets a parse
// error.
package ydbcomment

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"unicode/utf8"
)

// Object is what a comment belongs to.
type Object int

// The objects a YDB comment can belong to.
const (
	// Table is a row table's own comment.
	Table Object = iota + 1
	// Column is a column's comment, kept on its table.
	Column
	// Index is an index's comment, kept on its table: an index path holds no
	// attribute.
	Index
	// View is a view's own comment.
	View
)

// Keyword is the word COMMENT ON names the object with, and empty for a value
// that names no object.
func (o Object) Keyword() string {
	switch o {
	case Table:
		return "TABLE"
	case Column:
		return "COLUMN"
	case Index:
		return "INDEX"
	case View:
		return "VIEW"
	default:
		return ""
	}
}

// noun names the object in a sentence, with its article.
func (o Object) noun() string {
	if o == Index {
		return "an index"
	}
	return "a " + strings.ToLower(o.Keyword())
}

// The attribute keys. A table's and a view's own comment is [OwnKey]; a
// column's is [ColumnKeyPrefix] followed by the column's name, and an index's
// [IndexKeyPrefix] followed by the index's name, both on the table. YDB keeps
// every key whole, whatever bytes it holds, so the name follows the prefix as
// it is: a column name holds no dot and an index name may, and the prefix
// alone says which a key names.
const (
	OwnKey          = "ptah.comment"
	ColumnKeyPrefix = "ptah.comment.column."
	IndexKeyPrefix  = "ptah.comment.index."
)

// YDB's limits on user attributes, measured alike on 25.1.4.7 and 26.2.1.14.
const (
	// MaxKeyBytes is the longest key: a longer one answers `alter_attributes's
	// key length is not in [1; 100]`.
	MaxKeyBytes = 100
	// MaxValueBytes is the longest value, counted in bytes: a longer one
	// answers `alter_attributes's value length is not <= 4096`, for 4097
	// ASCII bytes and for 2049 two-byte characters alike.
	MaxValueBytes = 4096
	// MaxObjectBytes is what an object's attributes may take together, each
	// key's bytes and its value's: one byte more answers
	// `UserAttributes::CheckLimits: user attributes too big: 10241`. The
	// attributes Ptah does not own count too.
	MaxObjectBytes = 10240
)

// Key is the attribute that holds the comment of object, where name is the
// column's or the index's name; a table's and a view's own key takes none.
func Key(object Object, name string) string {
	switch object {
	case Column:
		return ColumnKeyPrefix + name
	case Index:
		return IndexKeyPrefix + name
	default:
		return OwnKey
	}
}

// Comments are the comments an object's attributes hold under Ptah's keys.
type Comments struct {
	// Own is the table's or the view's own comment.
	Own string
	// Columns are the columns' comments, by column name.
	Columns map[string]string
	// Indexes are the indexes' comments, by index name.
	Indexes map[string]string
}

// Read returns the comments attributes hold. An attribute under no key of
// Ptah's is not a comment and is left out, and so is one with an empty value,
// which YDB never stores (an empty value removes the key). The maps are nil
// where no such comment exists.
func Read(attributes map[string]string) Comments {
	var comments Comments
	for _, key := range slices.Sorted(maps.Keys(attributes)) {
		value := attributes[key]
		if value == "" {
			continue
		}
		if key == OwnKey {
			comments.Own = value
			continue
		}
		if name, found := strings.CutPrefix(key, ColumnKeyPrefix); found && name != "" {
			if comments.Columns == nil {
				comments.Columns = make(map[string]string)
			}
			comments.Columns[name] = value
			continue
		}
		if name, found := strings.CutPrefix(key, IndexKeyPrefix); found && name != "" {
			if comments.Indexes == nil {
				comments.Indexes = make(map[string]string)
			}
			comments.Indexes[name] = value
		}
	}
	return comments
}

// Refusal says why YDB cannot hold comment on object, named name for a column
// or an index, or returns "" when it can. An empty comment is no comment and
// takes nothing but its key. The reasons are the server's limits: a key
// longer than [MaxKeyBytes], which is what a column name over 80 bytes or an
// index name over 81 makes; a value longer than [MaxValueBytes]; and a value
// that is not UTF-8, which the table service's request cannot carry.
func Refusal(object Object, name, comment string) string {
	key := Key(object, name)
	switch {
	case (object == Column || object == Index) && name == "":
		return fmt.Sprintf("%s comment names no %s", object.noun(), strings.ToLower(object.Keyword()))
	case len(key) > MaxKeyBytes:
		return fmt.Sprintf("YDB keeps the comment as the table attribute %q, %d bytes long, and an attribute key "+
			"takes at most %d bytes; %s name takes at most %d", key, len(key), MaxKeyBytes,
			object.noun(), MaxKeyBytes-len(Key(object, "")))
	case len(comment) > MaxValueBytes:
		return fmt.Sprintf("the comment is %d bytes long, and YDB keeps at most %d bytes in an attribute value",
			len(comment), MaxValueBytes)
	case !utf8.ValidString(comment):
		return "the comment is not UTF-8 text, which YDB keeps in an attribute value"
	}
	return ""
}

// Size is the share of an object's [MaxObjectBytes] the comments take: each
// one's key and text, as YDB counts them. An empty comment takes nothing.
func Size(comments map[string]string) int {
	size := 0
	for key, value := range comments {
		if value != "" {
			size += len(key) + len(value)
		}
	}
	return size
}

// Attributes are the attributes, by key, that hold comments: the object's own
// comment, and the comments of the columns and the indexes it names. Each
// empty comment is left out.
func (c Comments) Attributes() map[string]string {
	attributes := make(map[string]string)
	if c.Own != "" {
		attributes[OwnKey] = c.Own
	}
	for name, comment := range c.Columns {
		if comment != "" {
			attributes[Key(Column, name)] = comment
		}
	}
	for name, comment := range c.Indexes {
		if comment != "" {
			attributes[Key(Index, name)] = comment
		}
	}
	return attributes
}

// SizeRefusal says why an object cannot hold the comments c, or returns "" when
// it can: together they take more than [MaxObjectBytes]. subject names the
// object.
func (c Comments) SizeRefusal(subject string) string {
	size := Size(c.Attributes())
	if size <= MaxObjectBytes {
		return ""
	}
	return fmt.Sprintf("the comments of %s take %d bytes as YDB table attributes, keys and text together, "+
		"and YDB keeps at most %d bytes of attributes on one object", subject, size, MaxObjectBytes)
}
