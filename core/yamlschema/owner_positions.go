package yamlschema

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"unicode/utf16"
	"unicode/utf8"

	"ptah.run/core/yamlext"
)

// yamlPositions converts parser character columns to original byte positions.
// Keeping encoding widths matters for BOMs, supplementary Unicode, and UTF-16
// input: none may be measured against a re-encoded copy of the document.
type yamlPositions struct {
	lines  [][]int
	starts []int
}

func indexYAMLPositions(data []byte) (yamlPositions, error) {
	offset := 0
	var order binary.ByteOrder
	switch {
	case bytes.HasPrefix(data, []byte{0xff, 0xfe}):
		order, offset = binary.LittleEndian, 2
	case bytes.HasPrefix(data, []byte{0xfe, 0xff}):
		order, offset = binary.BigEndian, 2
	case bytes.HasPrefix(data, []byte{0xef, 0xbb, 0xbf}):
		offset = 3
	}
	positions := yamlPositions{lines: [][]int{{offset}}, starts: []int{0}}
	for offset < len(data) {
		character, width, err := yamlSourceRune(data[offset:], order)
		if err != nil {
			return yamlPositions{}, err
		}
		offset += width
		switch character {
		case '\r', '\n', '\u0085', '\u2028', '\u2029':
			if character == '\r' && offset < len(data) {
				next, size, nextErr := yamlSourceRune(data[offset:], order)
				if nextErr != nil {
					return yamlPositions{}, nextErr
				}
				if next == '\n' {
					offset += size
				}
			}
			positions.lines = append(positions.lines, []int{offset})
			positions.starts = append(positions.starts, offset)
		default:
			index := len(positions.lines) - 1
			positions.lines[index] = append(positions.lines[index], offset)
		}
	}
	return positions, nil
}

func yamlSourceRune(data []byte, order binary.ByteOrder) (rune, int, error) {
	if order == nil {
		character, width := utf8.DecodeRune(data)
		if character == utf8.RuneError && width == 1 {
			return 0, 0, fmt.Errorf("YAML source contains invalid UTF-8")
		}
		return character, width, nil
	}
	if len(data) < 2 {
		return 0, 0, fmt.Errorf("YAML source contains incomplete UTF-16")
	}
	first := rune(order.Uint16(data))
	if !utf16.IsSurrogate(first) {
		return first, 2, nil
	}
	if len(data) >= 4 {
		character := utf16.DecodeRune(first, rune(order.Uint16(data[2:])))
		if character != utf8.RuneError {
			return character, 4, nil
		}
	}
	return 0, 0, fmt.Errorf("YAML source contains invalid UTF-16")
}

func (p yamlPositions) position(line, column int) (yamlext.SourcePosition, error) {
	if line < 1 || line > len(p.lines) || column < 1 || column > len(p.lines[line-1]) {
		return yamlext.SourcePosition{}, fmt.Errorf("YAML source position %d:%d is outside the original document", line, column)
	}
	offset := p.lines[line-1][column-1]
	return yamlext.SourcePosition{ByteOffset: offset, Line: line, Column: offset - p.starts[line-1] + 1}, nil
}
