package provider

import (
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// ColumnCollationsFeature has a column comparison state a column's collation
// apart from its type. Under it a host asks ColumnAspect COLUMN_COLLATION
// for each column pair it sends, and the provider answers a ColumnDifference
// whose spellings are the declared and the held collation, empty where a
// side leaves the engine's default; its COLUMN_TYPE answer then leaves
// collation out, so a column whose collation alone changes has a collation
// change and no type change. A host that does not list the feature asks no
// collation, and an engine that compares collation compares it inside the
// type, as it does for a host of contract 25.31. It is a two-way feature that
// requires schema.columns: the provider lists it, and so does each engine
// whose column comparison states collation. Contract 25.32.
const ColumnCollationsFeature = "schema.column-collations"

// validateColumnCollationsFeature refuses an engine that lists the feature
// without the provider's, or without the column comparison it extends.
func validateColumnCollationsFeature(description *protocol.Description, engine *protocol.Engine) error {
	if !slices.Contains(engine.GetFeatures(), ColumnCollationsFeature) {
		return nil
	}
	if !slices.Contains(description.GetFeatures(), ColumnCollationsFeature) || !EngineOffers(description, engine.GetId(), ColumnsFeature) {
		return fmt.Errorf("%w: column collations require the provider's feature and the column comparison", ErrProtocol)
	}
	return nil
}

// validateCollationReceipt checks the answer to COLUMN_COLLATION: SAME or
// CHANGED, each spelling one line, and two different spellings for a change.
func validateCollationReceipt(difference *protocol.ColumnDifference) error {
	if difference.GetRelation() != protocol.ColumnDifference_SAME && difference.GetRelation() != protocol.ColumnDifference_CHANGED {
		return columnsError("a collation difference needs SAME or CHANGED")
	}
	for _, spelling := range []string{difference.GetCurrent(), difference.GetDesired()} {
		if !utf8.ValidString(spelling) || strings.ContainsAny(spelling, "\r\n") || strings.TrimSpace(spelling) != spelling {
			return columnsError("a collation spelling is one line without surrounding white space")
		}
	}
	if difference.GetRelation() == protocol.ColumnDifference_CHANGED && difference.GetCurrent() == difference.GetDesired() {
		return columnsError("a changed collation names two different collations")
	}
	return nil
}
