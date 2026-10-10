package goschematogo

import (
	"encoding/json"
	"strconv"
	"strings"

	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbcolumn"
)

// captureColumnStores validates the YDB column storage each table carries and
// keeps it for the table annotation. An invalid value fails the export rather
// than disappearing from it.
func (ctx *renderContext) captureColumnStores() error {
	ctx.storesByTable = make(map[string]*ydbschema.DesiredColumnStore)
	for _, table := range ctx.db.Tables {
		store, err := ydbschema.DeclaredColumnStore(table.Facets)
		if err != nil {
			return err
		}
		if store == nil {
			continue
		}
		if err := ydbschema.ValidateDesiredColumnStore(store); err != nil {
			return err
		}
		ctx.storesByTable[table.QualifiedName()] = store
	}
	return nil
}

// columnStoreAttrs writes column storage as the table annotation attributes
// [ydbcolumn.Parse] reads; a row table writes none.
func columnStoreAttrs(store *ydbschema.DesiredColumnStore) []attr {
	if store == nil {
		return nil
	}
	attrs := []attr{
		{name: ydbcolumn.AttributeStore, value: "column", set: true},
		{name: ydbcolumn.AttributeHash, value: strings.Join(store.HashColumns, ","), set: len(store.HashColumns) > 0},
		{name: ydbcolumn.AttributeShards, value: strconv.FormatUint(store.Partitions, 10), set: store.Partitions != 0},
	}
	if store.TTL != nil {
		// The policy holds only strings and a slice, so JSON encoding cannot fail.
		encoded, _ := json.Marshal(store.TTL)
		attrs = append(attrs, attr{name: ydbcolumn.AttributeTTL, value: string(encoded), set: true})
	}
	return attrs
}
