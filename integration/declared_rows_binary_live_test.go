//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlasschema"
)

// TestDeclaredRowsConvergeOnABinaryColumnLive plans declared rows on a bytea
// column a second time and expects nothing.
//
// A declaration writes a binary column's value as text, and the reader hands
// the same column back as bytes, so that its bytes survive a checkpoint
// (stokaro/ptah#3297). The comparison has to pair the two: with text and bytes
// kept apart, every reconciliation plans the same UPDATE again, and the row
// keyed on its payload is planned as a delete and an insert. Only a server
// decides what the driver returns, so this is not observable offline.
func TestDeclaredRowsConvergeOnABinaryColumnLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	conn, done := declaredRowsDatabase(c, ctx, "binary_converge")
	defer done()

	desired := declaredBinarySchema("payload-one")
	applyDeclaredRows(c, ctx, conn, desired)

	plan, err := atlasschema.PreparePlanFile(ctx, conn, atlasschema.PlanFileOptions{Desired: desired})
	c.Assert(err, qt.IsNil)
	c.Assert(declaredPlanSQL(plan), qt.DeepEquals, []string(nil))
}

// TestDeclaredRowsChangeABinaryValueLive is the control: a payload that did
// change is still planned and applied, so the convergence above is agreement
// rather than a comparison that pairs everything.
func TestDeclaredRowsChangeABinaryValueLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	conn, done := declaredRowsDatabase(c, ctx, "binary_change")
	defer done()

	applyDeclaredRows(c, ctx, conn, declaredBinarySchema("payload-one"))

	changed := declaredBinarySchema("payload-two")
	plan, err := atlasschema.PreparePlanFile(ctx, conn, atlasschema.PlanFileOptions{Desired: changed})
	c.Assert(err, qt.IsNil)
	c.Assert(declaredPlanSQL(plan), qt.HasLen, 1)

	applyDeclaredRows(c, ctx, conn, changed)
	again, err := atlasschema.PreparePlanFile(ctx, conn, atlasschema.PlanFileOptions{Desired: changed})
	c.Assert(err, qt.IsNil)
	c.Assert(declaredPlanSQL(again), qt.DeepEquals, []string(nil))
}

// declaredBinarySchema is two tables holding one declared row each: one keyed on
// text with a bytea payload, and one keyed on the bytea value itself.
func declaredBinarySchema(payload string) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Blob", Name: "blobs"},
			{StructName: "BlobKey", Name: "blob_keys"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Blob", FieldName: "Code", Name: "code", Type: "TEXT", Primary: true},
			{StructName: "Blob", FieldName: "Payload", Name: "payload", Type: "BYTEA"},
			{StructName: "BlobKey", FieldName: "Digest", Name: "digest", Type: "BYTEA", Primary: true},
			{StructName: "BlobKey", FieldName: "Label", Name: "label", Type: "TEXT"},
		},
		ManagedData: []schemamodel.ManagedData{
			{
				StructName: "Blob",
				Table:      "blobs",
				Keys:       []string{"code"},
				File:       "blobs.yaml",
				Rows: []schemamodel.ManagedRow{{
					"code":    {Tag: "str", Text: "one"},
					"payload": {Tag: "str", Text: payload},
				}},
			},
			{
				StructName: "BlobKey",
				Table:      "blob_keys",
				Keys:       []string{"digest"},
				File:       "blob_keys.yaml",
				Rows: []schemamodel.ManagedRow{{
					"digest": {Tag: "str", Text: "digest-one"},
					"label":  {Tag: "str", Text: "one"},
				}},
			},
		},
	}
	schemamodel.Finalize(db)
	return db
}
