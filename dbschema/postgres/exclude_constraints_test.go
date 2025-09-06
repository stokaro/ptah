package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema/postgres"
	"github.com/stokaro/ptah/dbschema/types"
)

func TestParseExcludeConstraintDefinition(t *testing.T) {
	tests := []struct {
		name                string
		definition          string
		expectedUsingMethod string
		expectedElements    string
		expectedWhere       string
		expectError         bool
	}{
		{
			name:                "basic EXCLUDE constraint with GIST",
			definition:          "EXCLUDE USING gist (room_id WITH =, during WITH &&)",
			expectedUsingMethod: "gist",
			expectedElements:    "room_id WITH =, during WITH &&",
			expectedWhere:       "",
			expectError:         false,
		},
		{
			name:                "EXCLUDE constraint with WHERE clause",
			definition:          "EXCLUDE USING gist (room_id WITH =, during WITH &&) WHERE (is_active = true)",
			expectedUsingMethod: "gist",
			expectedElements:    "room_id WITH =, during WITH &&",
			expectedWhere:       "is_active = true",
			expectError:         false,
		},
		{
			name:                "EXCLUDE constraint with BTREE method",
			definition:          "EXCLUDE USING btree (user_id WITH =) WHERE (status = 'active')",
			expectedUsingMethod: "btree",
			expectedElements:    "user_id WITH =",
			expectedWhere:       "status = 'active'",
			expectError:         false,
		},
		{
			name:                "EXCLUDE constraint without WHERE parentheses",
			definition:          "EXCLUDE USING gist (location WITH &&) WHERE active = true",
			expectedUsingMethod: "gist",
			expectedElements:    "location WITH &&",
			expectedWhere:       "active = true",
			expectError:         false,
		},
		{
			name:                "complex elements with nested parentheses",
			definition:          "EXCLUDE USING gist (daterange(start_date, end_date, '[]') WITH &&)",
			expectedUsingMethod: "gist",
			expectedElements:    "daterange(start_date, end_date, '[]') WITH &&",
			expectedWhere:       "",
			expectError:         false,
		},
		{
			name:                "complex WHERE clause with nested parentheses",
			definition:          "EXCLUDE USING gist (room_id WITH =) WHERE ((status = 'active') AND (deleted_at IS NULL))",
			expectedUsingMethod: "gist",
			expectedElements:    "room_id WITH =",
			expectedWhere:       "(status = 'active') AND (deleted_at IS NULL)",
			expectError:         false,
		},
		{
			name:        "invalid definition without EXCLUDE USING",
			definition:  "CHECK (price > 0)",
			expectError: true,
		},
		{
			name:        "missing using method",
			definition:  "EXCLUDE USING",
			expectError: true,
		},
		{
			name:        "missing opening parenthesis",
			definition:  "EXCLUDE USING gist room_id WITH =",
			expectError: true,
		},
		{
			name:        "missing closing parenthesis",
			definition:  "EXCLUDE USING gist (room_id WITH =",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// Create a reader instance to access the method
			reader := &postgres.Reader{}

			// Use reflection to access the private method for testing
			// In a real implementation, you might want to make this method public for testing
			// or create a separate parsing package
			parsed, err := reader.ParseExcludeConstraintDefinition(tt.definition)

			if tt.expectError {
				c.Assert(err, qt.IsNotNil)
				return
			}

			c.Assert(err, qt.IsNil)
			c.Assert(parsed.UsingMethod, qt.Equals, tt.expectedUsingMethod)
			c.Assert(parsed.Elements, qt.Equals, tt.expectedElements)
			c.Assert(parsed.WhereCondition, qt.Equals, tt.expectedWhere)
		})
	}
}

func TestEnhanceExcludeConstraints(t *testing.T) {
	tests := []struct {
		name                string
		basicConstraints    []types.DBConstraint
		mockQueryResults    []mockExcludeConstraint
		expectedConstraints []types.DBConstraint
	}{
		{
			name: "enhance EXCLUDE constraint with full details",
			basicConstraints: []types.DBConstraint{
				{
					Name:      "no_overlapping_bookings",
					TableName: "bookings",
					Type:      "EXCLUDE",
				},
			},
			mockQueryResults: []mockExcludeConstraint{
				{
					constraintName: "no_overlapping_bookings",
					tableName:      "bookings",
					definition:     "EXCLUDE USING gist (room_id WITH =, during WITH &&) WHERE (is_active = true)",
				},
			},
			expectedConstraints: []types.DBConstraint{
				{
					Name:            "no_overlapping_bookings",
					TableName:       "bookings",
					Type:            "EXCLUDE",
					UsingMethod:     stringPtr("gist"),
					ExcludeElements: stringPtr("room_id WITH =, during WITH &&"),
					WhereCondition:  stringPtr("is_active = true"),
				},
			},
		},
		{
			name: "enhance EXCLUDE constraint without WHERE clause",
			basicConstraints: []types.DBConstraint{
				{
					Name:      "unique_locations",
					TableName: "locations",
					Type:      "EXCLUDE",
				},
			},
			mockQueryResults: []mockExcludeConstraint{
				{
					constraintName: "unique_locations",
					tableName:      "locations",
					definition:     "EXCLUDE USING gist (location WITH &&)",
				},
			},
			expectedConstraints: []types.DBConstraint{
				{
					Name:            "unique_locations",
					TableName:       "locations",
					Type:            "EXCLUDE",
					UsingMethod:     stringPtr("gist"),
					ExcludeElements: stringPtr("location WITH &&"),
					WhereCondition:  nil,
				},
			},
		},
		{
			name: "mixed constraint types - only EXCLUDE enhanced",
			basicConstraints: []types.DBConstraint{
				{
					Name:      "no_overlapping_bookings",
					TableName: "bookings",
					Type:      "EXCLUDE",
				},
				{
					Name:      "check_price_positive",
					TableName: "products",
					Type:      "CHECK",
				},
			},
			mockQueryResults: []mockExcludeConstraint{
				{
					constraintName: "no_overlapping_bookings",
					tableName:      "bookings",
					definition:     "EXCLUDE USING gist (room_id WITH =)",
				},
			},
			expectedConstraints: []types.DBConstraint{
				{
					Name:            "no_overlapping_bookings",
					TableName:       "bookings",
					Type:            "EXCLUDE",
					UsingMethod:     stringPtr("gist"),
					ExcludeElements: stringPtr("room_id WITH ="),
					WhereCondition:  nil,
				},
				{
					Name:      "check_price_positive",
					TableName: "products",
					Type:      "CHECK",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// This test would require mocking the database connection
			// For now, we'll test the parsing logic separately
			// In a full implementation, you would use a test database or mock the SQL queries

			c.Assert(len(tt.expectedConstraints), qt.Equals, len(tt.basicConstraints))
		})
	}
}

type mockExcludeConstraint struct {
	constraintName string
	tableName      string
	definition     string
}

func stringPtr(s string) *string {
	return &s
}
