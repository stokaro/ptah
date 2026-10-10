package ydbpartition

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"ptah.run/core/platform/capability"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbtype"
)

// ParseSplitPoints reads the split points of partition_at_keys: a comma
// separated list whose items are a value, for a split on the first key column,
// or a parenthesized list of values, for a split on the leading key columns.
// `10, 20, 30` is three split points of one value each, and `(10, 'a'), (20)`
// is two, the first on two key columns. A value is a run of characters with no
// space, comma, parenthesis or quote in it, or a string in single or double
// quotes, in which a backslash escapes the next character.
//
// The grammar is YQL's own list, so a declaration reads like the setting it
// becomes. It holds only values, never an expression: measured on 25.1.4.7 and
// 26.2.1.14, PARTITION_AT_KEYS takes literal numbers and strings, and refuses
// `-10` and `Timestamp("...")` as parse errors.
func ParseSplitPoints(text string) ([][]string, error) {
	p := splitPointParser{text: text}
	p.skipSpace()
	if p.done() {
		return nil, errors.New("name at least one split point, such as 10, 20 or (10, 'a'), (20)")
	}
	var points [][]string
	for {
		p.skipSpace()
		point, err := p.point()
		if err != nil {
			return nil, err
		}
		points = append(points, point)
		p.skipSpace()
		if p.done() {
			return points, nil
		}
		if !p.take(',') {
			return nil, fmt.Errorf("expected a comma after split point %d at offset %d", len(points), p.at)
		}
	}
}

// splitPointParser reads [ParseSplitPoints]'s grammar.
type splitPointParser struct {
	text string
	at   int
}

func (p *splitPointParser) done() bool { return p.at >= len(p.text) }

func (p *splitPointParser) skipSpace() {
	for !p.done() && strings.ContainsRune(" \t\r\n", rune(p.text[p.at])) {
		p.at++
	}
}

func (p *splitPointParser) take(c byte) bool {
	if !p.done() && p.text[p.at] == c {
		p.at++
		return true
	}
	return false
}

// point reads one split point: a value, or a parenthesized list of them.
func (p *splitPointParser) point() ([]string, error) {
	if !p.take('(') {
		value, err := p.value()
		if err != nil {
			return nil, err
		}
		return []string{value}, nil
	}
	var values []string
	for {
		p.skipSpace()
		value, err := p.value()
		if err != nil {
			return nil, err
		}
		values = append(values, value)
		p.skipSpace()
		if p.take(')') {
			return values, nil
		}
		if !p.take(',') {
			return nil, fmt.Errorf("expected a comma or a closing parenthesis at offset %d", p.at)
		}
	}
}

// value reads one value, quoted or bare.
func (p *splitPointParser) value() (string, error) {
	if p.done() {
		return "", errors.New("a split point ends without a value")
	}
	if quote := p.text[p.at]; quote == '\'' || quote == '"' {
		p.at++
		var b strings.Builder
		for !p.done() {
			c := p.text[p.at]
			p.at++
			switch {
			case c == quote:
				return b.String(), nil
			case c == '\\' && !p.done():
				b.WriteByte(p.text[p.at])
				p.at++
			default:
				b.WriteByte(c)
			}
		}
		return "", fmt.Errorf("a string value has no closing %c", quote)
	}
	start := p.at
	for !p.done() && !strings.ContainsRune(" \t\r\n,()'\"", rune(p.text[p.at])) {
		p.at++
	}
	if p.at == start {
		return "", fmt.Errorf("expected a value at offset %d", start)
	}
	return p.text[start:p.at], nil
}

// bareValue is a value [FormatSplitPoints] writes without quotes.
var bareValue = regexp.MustCompile(`^[0-9]+$`)

// FormatSplitPoints writes split points in the grammar [ParseSplitPoints]
// reads, so that reading the result gives them back: a split point of one
// value as the value, and one of several in parentheses; a value that is not a
// whole number in single quotes.
func FormatSplitPoints(points [][]string) string {
	written := make([]string, len(points))
	for i, point := range points {
		values := make([]string, len(point))
		for j, value := range point {
			values[j] = formatValue(value)
		}
		written[i] = strings.Join(values, ", ")
		if len(point) != 1 {
			written[i] = "(" + written[i] + ")"
		}
	}
	return strings.Join(written, ", ")
}

func formatValue(value string) string {
	if bareValue.MatchString(value) {
		return value
	}
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(value) + "'"
}

// serialIntegers are the integer types behind YDB's Serial types, which the
// key column's type check reads.
var serialIntegers = map[string]string{
	ydbtype.SmallSerial: ydbtype.Int16,
	ydbtype.Serial:      ydbtype.Int32,
	ydbtype.BigSerial:   ydbtype.Int64,
}

// integerBits are the integer types a split point value may be written for,
// with their width and whether they are unsigned.
var integerBits = map[string]struct {
	bits     int
	unsigned bool
}{
	ydbtype.Int8: {8, false}, ydbtype.Int16: {16, false}, ydbtype.Int32: {32, false}, ydbtype.Int64: {64, false},
	ydbtype.Uint8: {8, true}, ydbtype.Uint16: {16, true}, ydbtype.Uint32: {32, true}, ydbtype.Uint64: {64, true},
}

// LayoutClause writes the starting layout a declaration gives a table whose
// key columns have keyTypes, the YDB types in key order: `UNIFORM_PARTITIONS =
// n`, `PARTITION_AT_KEYS = ((...), ...)`, or "" for none. A layout YDB would
// refuse for this key is an error saying why. Measured on 25.1.4.7 and
// 26.2.1.14:
//
//   - UNIFORM_PARTITIONS needs a Uint32 or Uint64 first key column (`Unsupported
//     first key column type Int64, only Uint32 and Uint64 are supported`; a
//     Serial key is Int32, and is refused the same way);
//   - a split point holds at most one value per key column (`Partition at
//     keys has 3 key values while there are only 2 key columns`);
//   - a value is a literal number or string: a number for an integer column, in
//     the column's range and not negative (`-10` does not parse, and `300`
//     answers `Failed to convert value "300"` on a Uint8 column), and a string
//     for a Utf8 or String column. A column of another type has no literal the
//     setting takes: a Uuid column refuses a string (`Failed to convert type:
//     String to Uuid`), and a constructor such as `Timestamp("...")` does not
//     parse.
//
// Split points that are not in ascending order are left to YDB, which refuses
// them before it creates the table (`Partition ranges are not sorted at index
// 1`).
func LayoutClause(spec *ydbschema.TablePartitioning, keyTypes []string, caps capability.Capabilities) (string, error) {
	switch {
	case spec == nil:
		return "", nil
	case spec.UniformPartitions != 0:
		if len(keyTypes) == 0 {
			return "", errors.New("uniform_partitions needs the table's key, and the table declares none")
		}
		first := integerOf(keyTypes[0])
		if first != ydbtype.Uint32 && first != ydbtype.Uint64 {
			return "", fmt.Errorf("uniform_partitions splits the range of the first key column, which YDB does for a Uint32 "+
				"or Uint64 column only, and this one is %s (`Unsupported first key column type %s, only Uint32 and Uint64 "+
				"are supported`)", keyTypes[0], first)
		}
		return fmt.Sprintf("UNIFORM_PARTITIONS = %d", spec.UniformPartitions), nil
	case len(spec.PartitionAtKeys) != 0:
		points := make([]string, len(spec.PartitionAtKeys))
		for i, point := range spec.PartitionAtKeys {
			written, err := splitPoint(point, keyTypes, caps)
			if err != nil {
				return "", fmt.Errorf("split point %d of partition_at_keys: %w", i+1, err)
			}
			points[i] = written
		}
		return "PARTITION_AT_KEYS = (" + strings.Join(points, ", ") + ")", nil
	default:
		return "", nil
	}
}

// splitPoint writes one split point as a tuple of the key's types.
func splitPoint(point, keyTypes []string, caps capability.Capabilities) (string, error) {
	switch {
	case len(point) == 0:
		return "", errors.New("it holds no value")
	case len(point) > len(keyTypes):
		return "", fmt.Errorf("it holds %d values, and the key has %d columns (`Partition at keys has %d key values while "+
			"there are only %d key columns`)", len(point), len(keyTypes), len(point), len(keyTypes))
	}
	values := make([]string, len(point))
	for i, value := range point {
		literal, err := splitValue(value, keyTypes[i], caps)
		if err != nil {
			return "", err
		}
		values[i] = literal
	}
	return "(" + strings.Join(values, ", ") + ")", nil
}

// splitValue writes one value of a split point for a key column of ydbType.
func splitValue(value, ydbType string, caps capability.Capabilities) (string, error) {
	columnType := integerOf(ydbType)
	if integer, ok := integerBits[columnType]; ok {
		parsed, err := strconv.ParseUint(value, 10, integer.bits)
		if err != nil || (!integer.unsigned && parsed > 1<<(integer.bits-1)-1) {
			return "", fmt.Errorf("%q is not a whole number from 0 to the largest %s, which is what YDB takes for a "+
				"%s key column here", value, columnType, ydbType)
		}
		return strconv.FormatUint(parsed, 10), nil
	}
	if columnType == ydbtype.Utf8 || columnType == ydbtype.String {
		return ydbtype.Literal(columnType, value, caps)
	}
	return "", fmt.Errorf("its key column is %s, and YDB takes only literal numbers and strings here, for integer, "+
		"Utf8 and String columns", ydbType)
}

// integerOf is the integer type behind a Serial type, and the type itself
// otherwise.
func integerOf(ydbType string) string {
	if integer, ok := serialIntegers[ydbType]; ok {
		return integer
	}
	return ydbType
}
