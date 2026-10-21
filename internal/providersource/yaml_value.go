package providersource

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"unicode/utf16"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/yamlext"
	"ptah.run/internal/yamlvalue"
)

type yamlWalk struct {
	ctx    context.Context
	source []byte
	active map[*yamlext.Node]bool
	nodes  int
}

func (w *yamlWalk) enter(node *yamlext.Node, depth int) error {
	if err := w.ctx.Err(); err != nil {
		return err
	}
	w.nodes++
	if node == nil || w.active[node] {
		return fmt.Errorf("missing or cyclic YAML value")
	}
	if depth > provider.MaxSourceDepth || w.nodes > provider.MaxSourceNodes {
		return fmt.Errorf("YAML expansion exceeds source limits")
	}
	start, end := node.Range.Start, node.Range.End
	if start.ByteOffset < 0 || end.ByteOffset < start.ByteOffset || end.ByteOffset > len(w.source) || start.Line <= 0 || end.Line <= 0 || start.Column <= 0 || end.Column <= 0 {
		return fmt.Errorf("YAML range is outside its source")
	}
	w.active[node] = true
	return nil
}

func yamlSpan(node *yamlext.Node) (*protocol.SourceSpan, error) {
	start, err := yamlPosition(node.Range.Start)
	if err != nil {
		return nil, err
	}
	end, err := yamlPosition(node.Range.End)
	if err != nil {
		return nil, err
	}
	return &protocol.SourceSpan{Start: start, End: end}, nil
}

func yamlPosition(position yamlext.SourcePosition) (*protocol.SourcePosition, error) {
	if position.ByteOffset < 0 || position.Line <= 0 || int64(position.Line) > math.MaxUint32 || position.Column <= 0 || int64(position.Column) > math.MaxUint32 {
		return nil, fmt.Errorf("YAML source coordinate exceeds protocol bounds")
	}
	return &protocol.SourcePosition{ByteOffset: uint64(position.ByteOffset), Line: uint32(position.Line), Column: uint32(position.Column)}, nil
}

func (w *yamlWalk) value(node *yamlext.Node, depth int) (*protocol.SourceValue, error) {
	if err := w.enter(node, depth); err != nil {
		return nil, err
	}
	defer delete(w.active, node)
	span, err := yamlSpan(node)
	if err != nil {
		return nil, err
	}
	result := &protocol.SourceValue{Span: span}
	switch node.Kind {
	case yamlext.AliasNode:
		value, err := w.value(node.Alias, depth+1)
		if err != nil {
			return nil, err
		}
		value.Span = result.Span
		return value, nil
	case yamlext.ScalarNode:
		return w.scalar(node, result)
	case yamlext.SequenceNode:
		if node.Tag != "!!seq" {
			return nil, fmt.Errorf("unsupported YAML sequence tag %q", node.Tag)
		}
		sequence := &protocol.SourceSequence{}
		for _, child := range node.Content {
			value, err := w.value(child, depth+1)
			if err != nil {
				return nil, err
			}
			sequence.Values = append(sequence.Values, value)
		}
		result.Value = &protocol.SourceValue_Sequence{Sequence: sequence}
	case yamlext.MappingNode:
		mapping, err := w.mapping(node, depth)
		if err != nil {
			return nil, err
		}
		result.Value = &protocol.SourceValue_Mapping{Mapping: mapping}
	default:
		return nil, fmt.Errorf("unsupported YAML node kind %q", node.Kind)
	}
	return result, nil
}

func (w *yamlWalk) scalar(node *yamlext.Node, result *protocol.SourceValue) (*protocol.SourceValue, error) {
	switch node.Tag {
	case "!!null":
		result.Value = &protocol.SourceValue_Null{Null: &protocol.Empty{}}
	case "!!bool":
		if !strings.EqualFold(node.Value, "true") && !strings.EqualFold(node.Value, "false") {
			return nil, fmt.Errorf("invalid YAML boolean")
		}
		result.Value = &protocol.SourceValue_Boolean{Boolean: strings.EqualFold(node.Value, "true")}
	case "!!str":
		// The parser resolves overflowing implicit decimal exponents as strings.
		// Original syntax distinguishes them from quoted strings and explicit ! tags.
		if w.implicitScalar(node) {
			if number, err := yamlvalue.ExactNumber("!!float", node.Value); err == nil {
				result.Value = &protocol.SourceValue_Number{Number: number}
				return result, nil
			}
		}
		result.Value = &protocol.SourceValue_Text{Text: node.Value}
	case "!!int", "!!float":
		number, err := yamlvalue.ExactNumber(node.Tag, node.Value)
		if err != nil {
			return nil, err
		}
		result.Value = &protocol.SourceValue_Number{Number: number}
	default:
		return nil, fmt.Errorf("unsupported YAML scalar tag %q", node.Tag)
	}
	return result, nil
}

func (w *yamlWalk) implicitScalar(node *yamlext.Node) bool {
	raw := w.source[node.Range.Start.ByteOffset:node.Range.End.ByteOffset]
	text := string(raw)
	if len(w.source) >= 2 && (w.source[0] == 0xff && w.source[1] == 0xfe || w.source[0] == 0xfe && w.source[1] == 0xff) {
		var order binary.ByteOrder = binary.BigEndian
		if w.source[0] == 0xff {
			order = binary.LittleEndian
		}
		units := make([]uint16, 0, len(raw)/2)
		for i := 0; i+1 < len(raw); i += 2 {
			units = append(units, order.Uint16(raw[i:]))
		}
		text = string(utf16.Decode(units))
	}
	text = strings.TrimSpace(text)
	if strings.HasPrefix(text, "&") {
		end := strings.IndexAny(text, " \t\r\n\u0085\u2028\u2029")
		if end < 0 {
			return false
		}
		text = strings.TrimSpace(text[end:])
		for strings.HasPrefix(text, "#") {
			end = strings.IndexAny(text, "\r\n\u0085\u2028\u2029")
			if end < 0 {
				return false
			}
			text = strings.TrimSpace(text[end:])
		}
	}
	return text != "" && !strings.ContainsRune("!\"'|>", rune(text[0]))
}

func (w *yamlWalk) mapping(node *yamlext.Node, depth int) (*protocol.SourceMapping, error) {
	if node.Tag != "!!map" || len(node.Content)%2 != 0 {
		return nil, fmt.Errorf("unsupported YAML mapping")
	}
	result := &protocol.SourceMapping{}
	seen := make(map[string]bool)
	var merges []*yamlext.Node
	for i := 0; i < len(node.Content); i += 2 {
		key := node.Content[i]
		if key == nil {
			return nil, fmt.Errorf("missing YAML mapping key")
		}
		if key.Tag == "!!merge" {
			merges = append(merges, node.Content[i+1])
			continue
		}
		if err := w.enter(key, depth+1); err != nil {
			return nil, err
		}
		delete(w.active, key)
		if key.Kind != yamlext.ScalarNode || key.Tag != "!!str" || seen[key.Value] {
			return nil, fmt.Errorf("YAML mapping requires unique string keys")
		}
		if w.implicitScalar(key) {
			if _, err := yamlvalue.ExactNumber("!!float", key.Value); err == nil {
				return nil, fmt.Errorf("YAML mapping requires string keys")
			}
		}
		seen[key.Value] = true
		value, err := w.value(node.Content[i+1], depth+1)
		if err != nil {
			return nil, err
		}
		span, err := yamlSpan(key)
		if err != nil {
			return nil, err
		}
		result.Entries = append(result.Entries, &protocol.SourceEntry{Key: key.Value, KeySpan: span, Value: value})
	}
	for _, merge := range merges {
		if err := w.merge(result, seen, merge, depth); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (w *yamlWalk) merge(result *protocol.SourceMapping, seen map[string]bool, merge *yamlext.Node, depth int) error {
	value, err := w.value(merge, depth+1)
	if err != nil {
		return err
	}
	values := []*protocol.SourceValue{value}
	if sequence := value.GetSequence(); sequence != nil {
		values = sequence.GetValues()
	}
	for _, inherited := range values {
		mapping := inherited.GetMapping()
		if mapping == nil {
			return fmt.Errorf("YAML merge requires mappings")
		}
		for _, entry := range mapping.GetEntries() {
			if !seen[entry.GetKey()] {
				seen[entry.GetKey()] = true
				result.Entries = append(result.Entries, entry)
			}
		}
	}
	return nil
}
