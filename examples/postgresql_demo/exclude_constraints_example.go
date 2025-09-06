package main

import (
	"fmt"
	"log"

	"github.com/stokaro/ptah/core/goschema"
)

// Example 1: Room booking system with EXCLUDE constraint to prevent overlapping bookings
//
//migrator:schema:table name="bookings"
//migrator:schema:constraint name="no_overlapping_bookings" type="EXCLUDE" table="bookings" using="gist" elements="room_id WITH =, during WITH &&"
type Booking struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="room_id" type="INTEGER" not_null="true"
	RoomID int

	//migrator:schema:field name="during" type="TSRANGE" not_null="true"
	During string // PostgreSQL range type for time periods

	//migrator:schema:field name="created_at" type="TIMESTAMP" not_null="true" default_fn="NOW()"
	CreatedAt string
}

// Example 2: User session management with conditional EXCLUDE constraint
//
//migrator:schema:table name="user_sessions"
//migrator:schema:constraint name="one_active_session_per_user" type="EXCLUDE" table="user_sessions" using="gist" elements="user_id WITH =" condition="is_active = true"
type UserSession struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="user_id" type="INTEGER" not_null="true"
	UserID int

	//migrator:schema:field name="is_active" type="BOOLEAN" not_null="true" default="false"
	IsActive bool

	//migrator:schema:field name="created_at" type="TIMESTAMP" not_null="true" default_fn="NOW()"
	CreatedAt string
}

// Example 3: Spatial data with EXCLUDE constraint to prevent overlapping regions
//
//migrator:schema:table name="territories"
//migrator:schema:constraint name="no_overlapping_territories" type="EXCLUDE" table="territories" using="gist" elements="region WITH &&"
type Territory struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string

	//migrator:schema:field name="region" type="GEOMETRY" not_null="true"
	Region string // PostgreSQL geometry type

	//migrator:schema:field name="active" type="BOOLEAN" not_null="true" default="true"
	Active bool
}

// Example 4: Multiple constraint types including EXCLUDE
//
//migrator:schema:table name="events"
//migrator:schema:constraint name="no_overlapping_events" type="EXCLUDE" table="events" using="gist" elements="venue_id WITH =, event_time WITH &&" condition="status = 'confirmed'"
//migrator:schema:constraint name="unique_event_code" type="UNIQUE" table="events" columns="event_code"
//migrator:schema:constraint name="positive_capacity" type="CHECK" table="events" expression="capacity > 0"
type Event struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="event_code" type="VARCHAR(50)" not_null="true"
	EventCode string

	//migrator:schema:field name="venue_id" type="INTEGER" not_null="true"
	VenueID int

	//migrator:schema:field name="event_time" type="TSRANGE" not_null="true"
	EventTime string

	//migrator:schema:field name="capacity" type="INTEGER" not_null="true"
	Capacity int

	//migrator:schema:field name="status" type="VARCHAR(20)" not_null="true" default="'pending'"
	Status string
}

func runExcludeConstraintsExample() {
	fmt.Println("PostgreSQL EXCLUDE Constraints Example")
	fmt.Println("=====================================")

	// Parse the current package to extract schema information
	database, err := goschema.ParseDir(".")
	if err != nil {
		log.Fatalf("Failed to parse schema: %v", err)
	}

	fmt.Printf("Found %d tables with constraints:\n\n", len(database.Tables))

	// Display constraint information
	for _, constraint := range database.Constraints {
		fmt.Printf("Constraint: %s\n", constraint.Name)
		fmt.Printf("  Type: %s\n", constraint.Type)
		fmt.Printf("  Table: %s\n", constraint.Table)

		switch constraint.Type {
		case "EXCLUDE":
			fmt.Printf("  Using Method: %s\n", constraint.UsingMethod)
			fmt.Printf("  Elements: %s\n", constraint.ExcludeElements)
			if constraint.WhereCondition != "" {
				fmt.Printf("  WHERE: %s\n", constraint.WhereCondition)
			}
		case "UNIQUE":
			fmt.Printf("  Columns: %v\n", constraint.Columns)
		case "CHECK":
			fmt.Printf("  Expression: %s\n", constraint.CheckExpression)
		}
		fmt.Println()
	}

	// Generate SQL for PostgreSQL
	fmt.Println("Generated PostgreSQL SQL:")
	fmt.Println("========================")

	for _, table := range database.Tables {
		// Find constraints for this table
		var tableConstraints []goschema.Constraint
		for _, constraint := range database.Constraints {
			if constraint.Table == table.Name {
				tableConstraints = append(tableConstraints, constraint)
			}
		}

		if len(tableConstraints) > 0 {
			fmt.Printf("\n-- Constraints for table: %s\n", table.Name)
			for _, constraint := range tableConstraints {
				switch constraint.Type {
				case "EXCLUDE":
					sql := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s EXCLUDE USING %s (%s)",
						constraint.Table, constraint.Name, constraint.UsingMethod, constraint.ExcludeElements)
					if constraint.WhereCondition != "" {
						sql += fmt.Sprintf(" WHERE (%s)", constraint.WhereCondition)
					}
					sql += ";"
					fmt.Println(sql)

				case "UNIQUE":
					sql := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s);",
						constraint.Table, constraint.Name, constraint.Columns[0])
					fmt.Println(sql)

				case "CHECK":
					sql := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s);",
						constraint.Table, constraint.Name, constraint.CheckExpression)
					fmt.Println(sql)
				}
			}
		}
	}

	fmt.Println("\nEXCLUDE Constraint Use Cases:")
	fmt.Println("============================")
	fmt.Println("1. Room Booking: Prevents overlapping time slots for the same room")
	fmt.Println("2. User Sessions: Ensures only one active session per user")
	fmt.Println("3. Spatial Data: Prevents overlapping geographic territories")
	fmt.Println("4. Event Scheduling: Avoids conflicting events at the same venue")
	fmt.Println("\nNote: EXCLUDE constraints require PostgreSQL with appropriate extensions (e.g., btree_gist)")
}
