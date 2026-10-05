package ydb

import (
	"fmt"
	"strconv"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/protobuf/encoding/protowire"

	"ptah.run/internal/ydbindex"
)

// fullTextIndex reads fields 10 and 11 of TableIndexDescription, which the
// pinned generated protocol does not model. The wire contract is
// ydb/public/api/protos/ydb_table.proto on stable-26-2. Unknown settings fail
// the read rather than disappearing from an exported declaration.
func fullTextIndex(index *Ydb_Table.TableIndexDescription) (ydbindex.Kind, map[string]string, error) {
	fields, err := splitFields(index.ProtoReflect().GetUnknown())
	if err != nil {
		return 0, nil, fmt.Errorf("its description does not parse: %w", err)
	}
	for _, field := range fields {
		if field.number != 10 && field.number != 11 {
			continue
		}
		if len(fields) != 1 || field.kind != protowire.BytesType {
			return 0, nil, fmt.Errorf("its full-text description has extra fields or an invalid wire type")
		}
		kind := ydbindex.FullTextPlain
		if field.number == 11 {
			kind = ydbindex.FullTextRelevance
		}
		options, err := fullTextSettings(field.bytes, kind, index.GetIndexColumns())
		return kind, options, err
	}
	return 0, nil, nil
}

// fullTextSettings reads both the analyzers and the internal tables. A tuned
// internal table cannot be exported as an index with default partitioning.
func fullTextSettings(data []byte, kind ydbindex.Kind, columns []string) (map[string]string, error) {
	fields, err := splitFields(data)
	if err != nil {
		return nil, err
	}
	var options map[string]string
	for _, field := range fields {
		if field.kind != protowire.BytesType {
			return nil, fmt.Errorf("full-text field %d is not a message", field.number)
		}
		switch {
		case field.number == 2:
			if options != nil {
				return nil, fmt.Errorf("duplicate full-text analyzer settings")
			}
			options, err = fullTextColumns(field.bytes, columns)
		case field.number == 1 || (kind == ydbindex.FullTextRelevance && field.number >= 3 && field.number <= 5):
			err = implementationTable("full-text", field.bytes)
		default:
			err = fmt.Errorf("unknown full-text index field %d", field.number)
		}
		if err != nil {
			return nil, err
		}
	}
	return ydbindex.ResolveFullText(options)
}

// fullTextColumns requires the single analyzed column that YQL can declare
// on the measured 26.2 release.
func fullTextColumns(data []byte, columns []string) (map[string]string, error) {
	fields, err := splitFields(data)
	if err != nil {
		return nil, err
	}
	if len(fields) != 1 || fields[0].number != 2 || fields[0].kind != protowire.BytesType {
		return nil, fmt.Errorf("full-text settings do not describe exactly one analyzed column")
	}
	fields, err = splitFields(fields[0].bytes)
	if err != nil {
		return nil, err
	}
	var column string
	var options map[string]string
	for _, field := range fields {
		switch {
		case field.number == 1 && field.kind == protowire.BytesType:
			column = string(field.bytes)
		case field.number == 2 && field.kind == protowire.BytesType:
			options, err = fullTextAnalyzers(field.bytes)
		default:
			err = fmt.Errorf("unknown full-text column field %d or wire type %d", field.number, field.kind)
		}
		if err != nil {
			return nil, err
		}
	}
	if len(columns) != 1 || column != columns[0] {
		return nil, fmt.Errorf("full-text analyzed column %q does not match the single index column", column)
	}
	return options, nil
}

// fullTextAnalyzerFields maps the release's wire fields to the YQL options.
var fullTextAnalyzerFields = map[protowire.Number]string{
	1: "tokenizer", 2: "language", 100: "use_filter_lowercase", 110: "use_filter_stopwords",
	120: "use_filter_ngram", 121: "use_filter_edge_ngram", 122: "filter_ngram_min_length", 123: "filter_ngram_max_length",
	130: "use_filter_length", 131: "filter_length_min", 132: "filter_length_max", 140: "use_filter_snowball",
}

// fullTextAnalyzers preserves optional false values as well as true values.
func fullTextAnalyzers(data []byte) (map[string]string, error) {
	fields, err := splitFields(data)
	if err != nil {
		return nil, err
	}
	options := make(map[string]string, len(fields))
	for _, field := range fields {
		name, known := fullTextAnalyzerFields[field.number]
		if !known {
			return nil, fmt.Errorf("unknown full-text analyzer field %d", field.number)
		}
		if _, duplicate := options[name]; duplicate {
			return nil, fmt.Errorf("duplicate full-text analyzer option %s", name)
		}
		value, err := fullTextAnalyzerValue(field)
		if err != nil {
			return nil, err
		}
		options[name] = value
	}
	return ydbindex.ResolveFullText(options)
}

// fullTextAnalyzerValue checks scalar wire types before decoding their values.
func fullTextAnalyzerValue(field wireField) (string, error) {
	if field.number == 2 && field.kind == protowire.BytesType {
		return string(field.bytes), nil
	}
	if field.number == 2 || field.kind != protowire.VarintType {
		return "", fmt.Errorf("invalid full-text analyzer wire type %d for field %d", field.kind, field.number)
	}
	switch field.number {
	case 1:
		tokenizers := map[uint64]string{1: "whitespace", 2: "standard", 3: "keyword"}
		value, known := tokenizers[field.value]
		if !known {
			return "", fmt.Errorf("unknown full-text tokenizer %d", field.value)
		}
		return value, nil
	case 100, 110, 120, 121, 130, 140:
		if field.value > 1 {
			return "", fmt.Errorf("invalid full-text boolean %d", field.value)
		}
		return strconv.FormatBool(field.value == 1), nil
	default:
		return strconv.FormatUint(field.value, 10), nil
	}
}
