package goschema

import (
	"testing"
)

func TestDeduplicatePreservesFieldOrder(t *testing.T) {
	// Create a database with duplicate fields in different orders
	db := &Database{
		Fields: []Field{
			{StructName: "User", Name: "id"},
			{StructName: "User", Name: "email"}, 
			{StructName: "User", Name: "name"},
			{StructName: "User", Name: "created_at"},
			{StructName: "User", Name: "id"}, // duplicate - should be removed
			{StructName: "User", Name: "email"}, // duplicate - should be removed
		},
	}

	// Run deduplication
	deduplicate(db)

	// Check that order is preserved and duplicates are removed
	expected := []string{"id", "email", "name", "created_at"}
	if len(db.Fields) != len(expected) {
		t.Fatalf("Expected %d fields, got %d", len(expected), len(db.Fields))
	}

	for i, field := range db.Fields {
		if field.Name != expected[i] {
			t.Errorf("Expected field %d to be %s, got %s", i, expected[i], field.Name)
		}
	}
}

func TestDeduplicateFieldOrderConsistency(t *testing.T) {
	// Test that multiple runs produce the same order
	originalFields := []Field{
		{StructName: "User", Name: "id"},
		{StructName: "User", Name: "email"}, 
		{StructName: "User", Name: "name"},
		{StructName: "User", Name: "created_at"},
		{StructName: "Post", Name: "id"},
		{StructName: "Post", Name: "title"},
	}

	var results [][]string
	for run := 0; run < 10; run++ {
		// Create fresh database for each run
		db := &Database{Fields: make([]Field, len(originalFields))}
		copy(db.Fields, originalFields)
		
		deduplicate(db)
		
		// Extract field names in order
		fieldNames := make([]string, len(db.Fields))
		for i, field := range db.Fields {
			fieldNames[i] = field.StructName + "." + field.Name
		}
		results = append(results, fieldNames)
	}

	// All runs should produce identical order
	firstResult := results[0]
	for run := 1; run < len(results); run++ {
		result := results[run]
		if len(result) != len(firstResult) {
			t.Fatalf("Run %d produced different number of fields: expected %d, got %d", 
				run, len(firstResult), len(result))
		}
		
		for i := 0; i < len(result); i++ {
			if result[i] != firstResult[i] {
				t.Errorf("Run %d field order differs at position %d: expected %s, got %s",
					run, i, firstResult[i], result[i])
			}
		}
	}
}

func TestDeduplicateIndexesPreservesOrder(t *testing.T) {
	db := &Database{
		Indexes: []Index{
			{StructName: "User", Name: "idx_email"},
			{StructName: "User", Name: "idx_name"},
			{StructName: "User", Name: "idx_created_at"},
			{StructName: "User", Name: "idx_email"}, // duplicate
		},
	}

	deduplicate(db)

	expected := []string{"idx_email", "idx_name", "idx_created_at"}
	if len(db.Indexes) != len(expected) {
		t.Fatalf("Expected %d indexes, got %d", len(expected), len(db.Indexes))
	}

	for i, index := range db.Indexes {
		if index.Name != expected[i] {
			t.Errorf("Expected index %d to be %s, got %s", i, expected[i], index.Name)
		}
	}
}

func TestDeduplicateEnumsPreservesOrder(t *testing.T) {
	db := &Database{
		Enums: []Enum{
			{Name: "status"},
			{Name: "priority"},
			{Name: "category"},
			{Name: "status"}, // duplicate
		},
	}

	deduplicate(db)

	expected := []string{"status", "priority", "category"}
	if len(db.Enums) != len(expected) {
		t.Fatalf("Expected %d enums, got %d", len(expected), len(db.Enums))
	}

	for i, enum := range db.Enums {
		if enum.Name != expected[i] {
			t.Errorf("Expected enum %d to be %s, got %s", i, expected[i], enum.Name)
		}
	}
}

func TestDeduplicateEmbeddedFieldsPreservesOrder(t *testing.T) {
	db := &Database{
		EmbeddedFields: []EmbeddedField{
			{StructName: "User", EmbeddedTypeName: "Timestamps"},
			{StructName: "User", EmbeddedTypeName: "SoftDelete"},
			{StructName: "User", EmbeddedTypeName: "Timestamps"}, // duplicate
		},
	}

	deduplicate(db)

	expected := []string{"Timestamps", "SoftDelete"}
	if len(db.EmbeddedFields) != len(expected) {
		t.Fatalf("Expected %d embedded fields, got %d", len(expected), len(db.EmbeddedFields))
	}

	for i, embedded := range db.EmbeddedFields {
		if embedded.EmbeddedTypeName != expected[i] {
			t.Errorf("Expected embedded field %d to be %s, got %s", i, expected[i], embedded.EmbeddedTypeName)
		}
	}
}

func TestDeduplicateFunctionsPreservesOrder(t *testing.T) {
	db := &Database{
		Functions: []Function{
			{Name: "calculate_total"},
			{Name: "validate_email"},
			{Name: "generate_uuid"},
			{Name: "calculate_total"}, // duplicate
		},
	}

	deduplicate(db)

	expected := []string{"calculate_total", "validate_email", "generate_uuid"}
	if len(db.Functions) != len(expected) {
		t.Fatalf("Expected %d functions, got %d", len(expected), len(db.Functions))
	}

	for i, function := range db.Functions {
		if function.Name != expected[i] {
			t.Errorf("Expected function %d to be %s, got %s", i, expected[i], function.Name)
		}
	}
}

func TestDeduplicateRLSPoliciesPreservesOrder(t *testing.T) {
	db := &Database{
		RLSPolicies: []RLSPolicy{
			{Name: "user_policy"},
			{Name: "admin_policy"},
			{Name: "read_policy"},
			{Name: "user_policy"}, // duplicate
		},
	}

	deduplicate(db)

	expected := []string{"user_policy", "admin_policy", "read_policy"}
	if len(db.RLSPolicies) != len(expected) {
		t.Fatalf("Expected %d RLS policies, got %d", len(expected), len(db.RLSPolicies))
	}

	for i, policy := range db.RLSPolicies {
		if policy.Name != expected[i] {
			t.Errorf("Expected RLS policy %d to be %s, got %s", i, expected[i], policy.Name)
		}
	}
}

func TestDeduplicateRLSEnabledTablesPreservesOrder(t *testing.T) {
	db := &Database{
		RLSEnabledTables: []RLSEnabledTable{
			{Table: "users"},
			{Table: "posts"},
			{Table: "comments"},
			{Table: "users"}, // duplicate
		},
	}

	deduplicate(db)

	expected := []string{"users", "posts", "comments"}
	if len(db.RLSEnabledTables) != len(expected) {
		t.Fatalf("Expected %d RLS enabled tables, got %d", len(expected), len(db.RLSEnabledTables))
	}

	for i, table := range db.RLSEnabledTables {
		if table.Table != expected[i] {
			t.Errorf("Expected RLS enabled table %d to be %s, got %s", i, expected[i], table.Table)
		}
	}
}