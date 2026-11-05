package provider

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// UnreadTablesFeature enables Schema.unread_tables: a read names each table
// it found and does not describe, with the provider's reason (ADR 0063). A
// table the read names there is one the database holds, so its absence from
// Schema.tables says nothing about it. The provider and each engine that
// reports such tables list it, and the host lists it in its host features.
// Contract 25.20.
//
// Which tables a read leaves out is the provider's decision. The reason
// follows the rule of ErrorReasonsFeature: one line the provider wrote, which
// names the table and why and quotes no value, address or credential. It is
// required, and a host shows it whether or not the session negotiated
// ErrorReasonsFeature, since an entry means nothing without it.
const UnreadTablesFeature = "schema.unread-tables"

// maxUnreadTables bounds the entries of one read, as maxReservedIdentities
// bounds its reservations.
const maxUnreadTables = 4096

func unreadTableError(reason string) error {
	return fmt.Errorf("%w: unread tables: %s", ErrProtocol, reason)
}

// validateUnreadTables checks a snapshot's unread tables: negotiated for its
// engine, each a common table of that engine with a reason a host may show,
// none named twice, and none that the snapshot's tables or reservations also
// name. That an entry lies within a filtered read's scope is checked with the
// rest of the scope.
func validateUnreadTables(description *protocol.Description, engine *protocol.Engine, snapshot *protocol.Schema) error {
	entries := snapshot.GetUnreadTables()
	if len(entries) == 0 {
		return nil
	}
	if !slices.Contains(snapshot.GetTargetFeatures(), UnreadTablesFeature) || !EngineOffers(description, engine.GetId(), UnreadTablesFeature) {
		return unreadTableError("the feature was not negotiated")
	}
	if len(entries) > maxUnreadTables {
		return unreadTableError("too many entries")
	}
	var named []*protocol.Reference
	for _, entry := range entries {
		table := entry.GetTable()
		if !validReservedReference(table, snapshot.GetEngine()) || table.GetKind() != "table" || table.GetCatalog() != "" || len(table.GetPath()) != 0 {
			return unreadTableError("an entry must name a common table of the read's engine")
		}
		if ValidateDiagnostic(entry.GetDiagnostic()) != nil {
			return unreadTableError("an entry's reason must be one line a host may show")
		}
		named = append(named, table)
	}
	slices.SortFunc(named, compareReferences)
	for i := 1; i < len(named); i++ {
		if compareReferences(named[i-1], named[i]) == 0 {
			return unreadTableError("an entry names a table twice")
		}
	}
	unread := func(ref *protocol.Reference) bool {
		_, found := slices.BinarySearchFunc(named, ref, compareReferences)
		return found
	}
	for _, table := range snapshot.GetTables() {
		if unread(table.GetIdentity()) {
			return unreadTableError("an entry names a table the read describes")
		}
	}
	for _, record := range snapshot.GetReservedObjects() {
		if slices.Contains(record.GetConflictKinds(), "table") && unread(reservedReference(record.GetObject(), "table")) {
			return unreadTableError("an entry names a table the read reserves")
		}
	}
	return nil
}
