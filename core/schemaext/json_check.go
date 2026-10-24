package schemaext

import (
	"bytes"
	"encoding/json"
	"unicode/utf8"
)

// maxJSONDepth is the nesting limit [CanonicalJSON] enforces.
const maxJSONDepth = 256

// canonicalizable reports whether [CanonicalJSON] accepts data. It reads the
// bytes once and allocates only when an object key contains an escape, where
// CanonicalJSON builds a value tree and encodes it again. A caller that needs
// the canonical bytes, or the error text of a refusal, still calls
// CanonicalJSON; this is for the caller that only needs to know whether it
// would succeed.
func canonicalizable(data []byte) bool {
	if !utf8.Valid(data) || checkUnicodeEscapes(data) != nil || !json.Valid(data) {
		return false
	}
	walker := jsonWalker{data: data}
	walker.skipSpace()
	return walker.value(0)
}

// jsonWalker walks bytes that json.Valid already accepted, so it checks only
// what the grammar does not: nesting depth and duplicate object keys.
type jsonWalker struct {
	data []byte
	pos  int
}

func (w *jsonWalker) skipSpace() {
	for w.pos < len(w.data) {
		switch w.data[w.pos] {
		case ' ', '\t', '\r', '\n':
			w.pos++
		default:
			return
		}
	}
}

func (w *jsonWalker) value(depth int) bool {
	if depth > maxJSONDepth {
		return false
	}
	switch w.data[w.pos] {
	case '{':
		return w.object(depth)
	case '[':
		return w.array(depth)
	case '"':
		w.str()
		return true
	default:
		for w.pos < len(w.data) {
			switch w.data[w.pos] {
			case ',', ']', '}', ' ', '\t', '\r', '\n':
				return true
			}
			w.pos++
		}
		return true
	}
}

// str advances past a string and returns its raw bytes, quotes included.
func (w *jsonWalker) str() []byte {
	start := w.pos
	w.pos++
	for w.data[w.pos] != '"' {
		if w.data[w.pos] == '\\' {
			w.pos++
		}
		w.pos++
	}
	w.pos++
	return w.data[start:w.pos]
}

func (w *jsonWalker) array(depth int) bool {
	w.pos++
	w.skipSpace()
	if w.data[w.pos] == ']' {
		w.pos++
		return true
	}
	for {
		w.skipSpace()
		if !w.value(depth + 1) {
			return false
		}
		w.skipSpace()
		closed := w.data[w.pos] == ']'
		w.pos++
		if closed {
			return true
		}
	}
}

func (w *jsonWalker) object(depth int) bool {
	w.pos++
	w.skipSpace()
	if w.data[w.pos] == '}' {
		w.pos++
		return true
	}
	var inline [8][]byte
	keys := inline[:0]
	for {
		w.skipSpace()
		key := w.str()
		for _, seen := range keys {
			if sameJSONKey(seen, key) {
				return false
			}
		}
		keys = append(keys, key)
		w.skipSpace()
		w.pos++ // the colon
		w.skipSpace()
		if !w.value(depth + 1) {
			return false
		}
		w.skipSpace()
		closed := w.data[w.pos] == '}'
		w.pos++
		if closed {
			return true
		}
	}
}

// sameJSONKey compares two raw object keys by the strings they decode to, so
// "a" and "a" are one key.
func sameJSONKey(a, b []byte) bool {
	if bytes.Equal(a, b) {
		return true
	}
	if !bytes.Contains(a, []byte{'\\'}) && !bytes.Contains(b, []byte{'\\'}) {
		return false
	}
	var left, right string
	return json.Unmarshal(a, &left) == nil && json.Unmarshal(b, &right) == nil && left == right
}
