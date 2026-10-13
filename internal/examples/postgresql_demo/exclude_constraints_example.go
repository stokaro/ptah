package main

import (
	_ "embed" // exampleSource embeds this file, so the demo parses its own declarations.
	"fmt"
	"io"

	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/engine/builtin"
)

// exampleSource is this file. The demo parses it rather than a directory, so
// what it reads does not depend on where it is run from: a walk of the working
// directory, run from the repository root, reads every fixture in the tree.
//
//go:embed exclude_constraints_example.go
var exampleSource []byte

// Example 1: Room booking system with EXCLUDE constraint to prevent overlapping bookings
//
//ptah:schema:table name="bookings"
//ptah:schema:exclude name="no_overlapping_bookings" table="bookings" using="gist" elements="room_id WITH =, during WITH &&"
type Booking struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="room_id" type="INTEGER" not_null="true"
	RoomID int

	//ptah:schema:field name="during" type="TSRANGE" not_null="true"
	During string // PostgreSQL range type for time periods

	//ptah:schema:field name="created_at" type="TIMESTAMP" not_null="true" default_expr="NOW()"
	CreatedAt string
}

// Example 2: User session management with conditional EXCLUDE constraint
//
//ptah:schema:table name="user_sessions"
//ptah:schema:exclude name="one_active_session_per_user" table="user_sessions" using="gist" elements="user_id WITH =" condition="is_active = true"
type UserSession struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="user_id" type="INTEGER" not_null="true"
	UserID int

	//ptah:schema:field name="is_active" type="BOOLEAN" not_null="true" default="false"
	IsActive bool

	//ptah:schema:field name="created_at" type="TIMESTAMP" not_null="true" default_expr="NOW()"
	CreatedAt string
}

// Example 3: Spatial data with EXCLUDE constraint to prevent overlapping regions
//
//ptah:schema:table name="territories"
//ptah:schema:exclude name="no_overlapping_territories" table="territories" using="gist" elements="region WITH &&"
type Territory struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string

	//ptah:schema:field name="region" type="GEOMETRY" not_null="true"
	Region string // PostgreSQL geometry type

	//ptah:schema:field name="active" type="BOOLEAN" not_null="true" default="true"
	Active bool
}

// Example 4: Multiple constraint types including EXCLUDE
//
//ptah:schema:table name="events"
//ptah:schema:exclude name="no_overlapping_events" table="events" using="gist" elements="venue_id WITH =, event_time WITH &&" condition="status = 'confirmed'"
//ptah:schema:constraint name="unique_event_code" type="UNIQUE" table="events" columns="event_code"
//ptah:schema:constraint name="positive_capacity" type="CHECK" table="events" check="capacity > 0"
type Event struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="event_code" type="VARCHAR(50)" not_null="true"
	EventCode string

	//ptah:schema:field name="venue_id" type="INTEGER" not_null="true"
	VenueID int

	//ptah:schema:field name="event_time" type="TSRANGE" not_null="true"
	EventTime string

	//ptah:schema:field name="capacity" type="INTEGER" not_null="true"
	Capacity int

	//ptah:schema:field name="status" type="VARCHAR(20)" not_null="true" default="'pending'"
	Status string
}

func runExcludeConstraintsExample(w io.Writer) error {
	fmt.Fprintln(w, "PostgreSQL EXCLUDE Constraints Example")
	fmt.Fprintln(w, "=====================================")

	// Parse this file's declarations
	annotations, err := builtin.Annotations()
	if err != nil {
		return fmt.Errorf("assemble the bundled owners: %w", err)
	}
	database, err := goschema.ParseSource(annotations, "exclude_constraints_example.go", exampleSource)
	if err != nil {
		return fmt.Errorf("parse the schema: %w", err)
	}
	schemamodel.Finalize(&database)

	fmt.Fprintf(w, "Found %d tables with constraints:\n\n", len(database.Tables))

	// Display constraint information
	for _, constraint := range database.Constraints {
		fmt.Fprintf(w, "Constraint: %s\n", constraint.Name)
		fmt.Fprintf(w, "  Type: %s\n", constraint.Type)
		fmt.Fprintf(w, "  Table: %s\n", constraint.Table)

		switch constraint.Type {
		case "UNIQUE":
			fmt.Fprintf(w, "  Columns: %v\n", constraint.Columns)
		case "CHECK":
			fmt.Fprintf(w, "  Expression: %s\n", constraint.CheckExpression)
		}
		fmt.Fprintln(w)
	}

	objects, err := database.FeatureObjects.All()
	if err != nil {
		return fmt.Errorf("read exclusion constraints: %w", err)
	}
	for _, object := range objects {
		constraint, ok := object.Value.(*pgtable.DesiredExclusion)
		if !ok {
			continue
		}
		fmt.Fprintf(w, "Constraint: %s\n  Type: EXCLUDE\n", object.Ref.Name.Source)
		fmt.Fprintf(w, "  Table: %s\n", pgtable.ExclusionTable(object.Ref).Name.Source)
		fmt.Fprintf(w, "  Using Method: %s\n  Elements: %s\n", constraint.UsingMethod, constraint.Elements)
		if constraint.WhereCondition != "" {
			fmt.Fprintf(w, "  WHERE: %s\n", constraint.WhereCondition)
		}
		fmt.Fprintln(w)
	}

	// Render through the same owner runtime the CLI uses.
	fmt.Fprintln(w, "Generated PostgreSQL SQL:")
	fmt.Fprintln(w, "========================")

	statements, err := builtin.GetOrderedCreateStatements(&database, "postgres")
	if err != nil {
		return fmt.Errorf("render PostgreSQL schema: %w", err)
	}
	for _, statement := range statements {
		fmt.Fprintln(w, statement)
	}

	fmt.Fprintln(w, "\nEXCLUDE Constraint Use Cases:")
	fmt.Fprintln(w, "============================")
	fmt.Fprintln(w, "1. Room Booking: Prevents overlapping time slots for the same room")
	fmt.Fprintln(w, "2. User Sessions: Ensures only one active session per user")
	fmt.Fprintln(w, "3. Spatial Data: Prevents overlapping geographic territories")
	fmt.Fprintln(w, "4. Event Scheduling: Avoids conflicting events at the same venue")
	fmt.Fprintln(w, "\nNote: EXCLUDE constraints require PostgreSQL with appropriate extensions (e.g., btree_gist)")
	return nil
}
