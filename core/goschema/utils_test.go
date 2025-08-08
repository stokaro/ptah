package goschema

import (
	"testing"
)

// TestDeduplicatePreservesFieldOrder tests that the deduplicate function preserves the original order of fields
func TestDeduplicatePreservesFieldOrder(t *testing.T) {
	// Create a database with fields in a specific order
	db := &Database{
		Fields: []Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: "true"},
			{StructName: "User", Name: "email", Type: "VARCHAR(255)"},
			{StructName: "User", Name: "name", Type: "VARCHAR(255)"},
			{StructName: "User", Name: "created_at", Type: "TIMESTAMP"},
			{StructName: "Profile", Name: "id", Type: "SERIAL", Primary: "true"},
			{StructName: "Profile", Name: "bio", Type: "TEXT"},
		},
		Indexes:           []Index{},
		Enums:             []Enum{},
		EmbeddedFields:    []EmbeddedField{},
		Functions:         []Function{},
		RLSPolicies:       []RLSPolicy{},
		RLSEnabledTables:  []RLSEnabledTable{},
		Extensions:        []Extension{},
		Tables:            []Table{},
	}

	// Record original field order
	originalOrder := make([]string, len(db.Fields))
	for i, field := range db.Fields {
		originalOrder[i] = field.StructName + "." + field.Name
	}

	// Run deduplication
	deduplicate(db)

	// Verify field order is preserved
	if len(db.Fields) != len(originalOrder) {
		t.Fatalf("Expected %d fields after deduplication, got %d", len(originalOrder), len(db.Fields))
	}

	for i, field := range db.Fields {
		expectedKey := originalOrder[i]
		actualKey := field.StructName + "." + field.Name
		if actualKey != expectedKey {
			t.Errorf("Field order not preserved at position %d: expected %s, got %s", i, expectedKey, actualKey)
		}
	}
}

// TestDeduplicateFieldOrderConsistency tests that multiple runs produce identical field order
func TestDeduplicateFieldOrderConsistency(t *testing.T) {
	createDatabase := func() *Database {
		return &Database{
			Fields: []Field{
				{StructName: "User", Name: "id", Type: "SERIAL", Primary: "true"},
				{StructName: "User", Name: "email", Type: "VARCHAR(255)"},
				{StructName: "User", Name: "name", Type: "VARCHAR(255)"},
				{StructName: "User", Name: "created_at", Type: "TIMESTAMP"},
				{StructName: "Profile", Name: "id", Type: "SERIAL", Primary: "true"},
				{StructName: "Profile", Name: "bio", Type: "TEXT"},
			},
			Indexes:           []Index{},
			Enums:             []Enum{},
			EmbeddedFields:    []EmbeddedField{},
			Functions:         []Function{},
			RLSPolicies:       []RLSPolicy{},
			RLSEnabledTables:  []RLSEnabledTable{},
			Extensions:        []Extension{},
			Tables:            []Table{},
		}
	}

	// Run deduplication multiple times and compare results
	runs := 10
	var results [][]string

	for run := 0; run < runs; run++ {
		db := createDatabase()
		deduplicate(db)

		// Record field order for this run
		fieldOrder := make([]string, len(db.Fields))
		for i, field := range db.Fields {
			fieldOrder[i] = field.StructName + "." + field.Name
		}
		results = append(results, fieldOrder)
	}

	// Compare all runs to the first run
	firstRunOrder := results[0]
	for run := 1; run < runs; run++ {
		currentRunOrder := results[run]
		
		if len(currentRunOrder) != len(firstRunOrder) {
			t.Fatalf("Run %d produced different number of fields: expected %d, got %d", 
				run, len(firstRunOrder), len(currentRunOrder))
		}

		for i, field := range currentRunOrder {
			if field != firstRunOrder[i] {
				t.Errorf("Inconsistent field order between runs: run 0 position %d = %s, run %d position %d = %s", 
					i, firstRunOrder[i], run, i, field)
			}
		}
	}
}

// TestDeduplicateRemovesDuplicateFields tests that duplicate fields are properly removed
func TestDeduplicateRemovesDuplicateFields(t *testing.T) {
	db := &Database{
		Fields: []Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: "true"},
			{StructName: "User", Name: "email", Type: "VARCHAR(255)"},
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: "true"}, // Duplicate
			{StructName: "User", Name: "name", Type: "VARCHAR(255)"},
			{StructName: "User", Name: "email", Type: "VARCHAR(255)"}, // Duplicate
		},
		Indexes:           []Index{},
		Enums:             []Enum{},
		EmbeddedFields:    []EmbeddedField{},
		Functions:         []Function{},
		RLSPolicies:       []RLSPolicy{},
		RLSEnabledTables:  []RLSEnabledTable{},
		Extensions:        []Extension{},
		Tables:            []Table{},
	}

	deduplicate(db)

	// Should have 3 unique fields
	if len(db.Fields) != 3 {
		t.Fatalf("Expected 3 unique fields after deduplication, got %d", len(db.Fields))
	}

	// Check that we have the expected fields in order
	expectedFields := []string{
		"User.id",
		"User.email", 
		"User.name",
	}

	for i, field := range db.Fields {
		expectedKey := expectedFields[i]
		actualKey := field.StructName + "." + field.Name
		if actualKey != expectedKey {
			t.Errorf("Unexpected field at position %d: expected %s, got %s", i, expectedKey, actualKey)
		}
	}
}

// TestDeduplicatePreservesIndexOrder tests that index deduplication preserves order
func TestDeduplicatePreservesIndexOrder(t *testing.T) {
	db := &Database{
		Fields: []Field{},
		Indexes: []Index{
			{StructName: "User", Name: "idx_email", Type: "btree"},
			{StructName: "User", Name: "idx_name", Type: "btree"},
			{StructName: "Profile", Name: "idx_bio", Type: "gin"},
		},
		Enums:             []Enum{},
		EmbeddedFields:    []EmbeddedField{},
		Functions:         []Function{},
		RLSPolicies:       []RLSPolicy{},
		RLSEnabledTables:  []RLSEnabledTable{},
		Extensions:        []Extension{},
		Tables:            []Table{},
	}

	originalOrder := make([]string, len(db.Indexes))
	for i, index := range db.Indexes {
		originalOrder[i] = index.StructName + "." + index.Name
	}

	deduplicate(db)

	if len(db.Indexes) != len(originalOrder) {
		t.Fatalf("Expected %d indexes after deduplication, got %d", len(originalOrder), len(db.Indexes))
	}

	for i, index := range db.Indexes {
		expectedKey := originalOrder[i]
		actualKey := index.StructName + "." + index.Name
		if actualKey != expectedKey {
			t.Errorf("Index order not preserved at position %d: expected %s, got %s", i, expectedKey, actualKey)
		}
	}
}

// TestDeduplicatePreservesEnumOrder tests that enum deduplication preserves order
func TestDeduplicatePreservesEnumOrder(t *testing.T) {
	db := &Database{
		Fields:  []Field{},
		Indexes: []Index{},
		Enums: []Enum{
			{Name: "user_status", Values: []string{"active", "inactive"}},
			{Name: "priority_level", Values: []string{"high", "medium", "low"}},
			{Name: "permission_type", Values: []string{"read", "write", "admin"}},
		},
		EmbeddedFields:    []EmbeddedField{},
		Functions:         []Function{},
		RLSPolicies:       []RLSPolicy{},
		RLSEnabledTables:  []RLSEnabledTable{},
		Extensions:        []Extension{},
		Tables:            []Table{},
	}

	originalOrder := make([]string, len(db.Enums))
	for i, enum := range db.Enums {
		originalOrder[i] = enum.Name
	}

	deduplicate(db)

	if len(db.Enums) != len(originalOrder) {
		t.Fatalf("Expected %d enums after deduplication, got %d", len(originalOrder), len(db.Enums))
	}

	for i, enum := range db.Enums {
		if enum.Name != originalOrder[i] {
			t.Errorf("Enum order not preserved at position %d: expected %s, got %s", i, originalOrder[i], enum.Name)
		}
	}
}

// TestDeduplicatePreservesEmbeddedFieldOrder tests that embedded field deduplication preserves order
func TestDeduplicatePreservesEmbeddedFieldOrder(t *testing.T) {
	db := &Database{
		Fields:  []Field{},
		Indexes: []Index{},
		Enums:   []Enum{},
		EmbeddedFields: []EmbeddedField{
			{StructName: "User", EmbeddedTypeName: "BaseModel", Mode: "inline"},
			{StructName: "User", EmbeddedTypeName: "Timestamps", Mode: "inline"},
			{StructName: "Profile", EmbeddedTypeName: "BaseModel", Mode: "inline"},
		},
		Functions:         []Function{},
		RLSPolicies:       []RLSPolicy{},
		RLSEnabledTables:  []RLSEnabledTable{},
		Extensions:        []Extension{},
		Tables:            []Table{},
	}

	originalOrder := make([]string, len(db.EmbeddedFields))
	for i, embedded := range db.EmbeddedFields {
		originalOrder[i] = embedded.StructName + "." + embedded.EmbeddedTypeName
	}

	deduplicate(db)

	if len(db.EmbeddedFields) != len(originalOrder) {
		t.Fatalf("Expected %d embedded fields after deduplication, got %d", len(originalOrder), len(db.EmbeddedFields))
	}

	for i, embedded := range db.EmbeddedFields {
		expectedKey := originalOrder[i]
		actualKey := embedded.StructName + "." + embedded.EmbeddedTypeName
		if actualKey != expectedKey {
			t.Errorf("Embedded field order not preserved at position %d: expected %s, got %s", i, expectedKey, actualKey)
		}
	}
}

// TestDeduplicateAllCollections tests order preservation across all collections
func TestDeduplicateAllCollections(t *testing.T) {
	db := &Database{
		Fields: []Field{
			{StructName: "User", Name: "id", Type: "SERIAL"},
			{StructName: "User", Name: "email", Type: "VARCHAR(255)"},
		},
		Indexes: []Index{
			{StructName: "User", Name: "idx_email", Type: "btree"},
		},
		Enums: []Enum{
			{Name: "user_status", Values: []string{"active", "inactive"}},
		},
		EmbeddedFields: []EmbeddedField{
			{StructName: "User", EmbeddedTypeName: "BaseModel", Mode: "inline"},
		},
		Functions: []Function{
			{Name: "get_user_count", Body: "SELECT COUNT(*) FROM users;"},
		},
		RLSPolicies: []RLSPolicy{
			{Name: "user_policy", Table: "users"},
		},
		RLSEnabledTables: []RLSEnabledTable{
			{Table: "users"},
		},
		Extensions: []Extension{
			{Name: "uuid-ossp"},
		},
		Tables: []Table{
			{Name: "users", StructName: "User"},
		},
	}

	// Record original orders
	originalFieldOrder := make([]string, len(db.Fields))
	for i, field := range db.Fields {
		originalFieldOrder[i] = field.StructName + "." + field.Name
	}

	originalIndexOrder := make([]string, len(db.Indexes))
	for i, index := range db.Indexes {
		originalIndexOrder[i] = index.StructName + "." + index.Name
	}

	originalEnumOrder := make([]string, len(db.Enums))
	for i, enum := range db.Enums {
		originalEnumOrder[i] = enum.Name
	}

	deduplicate(db)

	// Verify all orders are preserved
	for i, field := range db.Fields {
		expectedKey := originalFieldOrder[i]
		actualKey := field.StructName + "." + field.Name
		if actualKey != expectedKey {
			t.Errorf("Field order not preserved at position %d: expected %s, got %s", i, expectedKey, actualKey)
		}
	}

	for i, index := range db.Indexes {
		expectedKey := originalIndexOrder[i]
		actualKey := index.StructName + "." + index.Name
		if actualKey != expectedKey {
			t.Errorf("Index order not preserved at position %d: expected %s, got %s", i, expectedKey, actualKey)
		}
	}

	for i, enum := range db.Enums {
		if enum.Name != originalEnumOrder[i] {
			t.Errorf("Enum order not preserved at position %d: expected %s, got %s", i, originalEnumOrder[i], enum.Name)
		}
	}
}