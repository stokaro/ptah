package embedydb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"ptah.run/internal/embedbackend"
	"ptah.run/internal/embedgen"
	"ptah.run/internal/embedstore"
	"ptah.run/internal/ydbcomment"
)

// RetireGeneration destroys one generation and records that it did.
//
// YDB runs a schema statement outside any transaction, so this is two steps
// where PostgreSQL takes one. The record comes first: the generation is
// stamped retired and its runs completed in one serializable transaction,
// decided by the same rules the PostgreSQL store applies before its DDL,
// after the row count and index the retirement was approved against are
// measured again. From that moment no pointer can move to it. Its index,
// columns or table are dropped second. A retirement whose drops did not finish
// is completed by running retire again: a generation already recorded as
// retired skips the record and drops what is left.
func (b *Backend) RetireGeneration(
	ctx context.Context, identity string, expectedPointer embedstore.Pointer,
	expectedRows int, destruction embedbackend.RetirementDestruction,
) (embedbackend.Retirement, error) {
	if err := destruction.Validate(); err != nil {
		return embedbackend.Retirement{}, err
	}
	registered, err := b.store.Generation(ctx, identity)
	if err != nil {
		return embedbackend.Retirement{}, err
	}
	retirement := embedbackend.Retirement{RetiredAt: registered.RetiredAt}
	if registered.Retired() {
		retirement.Notes = append(retirement.Notes, fmt.Sprintf(
			"generation %s was recorded as retired at %s; this run dropped what was left of it",
			identity, registered.RetiredAt.UTC().Format(time.RFC3339)))
	} else {
		if err := b.revalidateArtifacts(ctx, registered, expectedRows, destruction); err != nil {
			return embedbackend.Retirement{}, err
		}
		if _, err := b.store.decide(ctx, everything, "retire generation "+identity,
			func(memory *embedstore.Memory) error {
				var err error
				retirement.RetiredAt, err = memory.RetireGeneration(ctx, identity, expectedPointer)
				return err
			}); err != nil {
			return embedbackend.Retirement{}, err
		}
		retirement.RetiredAt = storedInstant(retirement.RetiredAt)
	}
	if err := b.dropGeneration(ctx, registered, destruction); err != nil {
		return embedbackend.Retirement{}, fmt.Errorf(
			"generation %s is recorded as retired, and dropping its storage failed; run retire again to "+
				"finish: %w", identity, err)
	}
	return retirement, nil
}

// revalidateArtifacts refuses a retirement whose approved row count or index
// state is no longer what the table holds.
func (b *Backend) revalidateArtifacts(
	ctx context.Context, generation embedstore.Generation, rows int,
	approved embedbackend.RetirementDestruction,
) error {
	counted, err := b.CountGenerationRows(ctx, generation)
	if err != nil {
		return err
	}
	if counted != rows {
		return fmt.Errorf("%w: generation %s row count changed from approved %d to %d",
			embedstore.ErrConflict, generation.Identity, rows, counted)
	}
	present, err := b.GenerationIndexExists(ctx, generation)
	if err != nil {
		return err
	}
	if present != approved.IndexExists {
		return fmt.Errorf("%w: generation %s index state changed from approved %t to %t",
			embedstore.ErrConflict, generation.Identity, approved.IndexExists, present)
	}
	return nil
}

// dropGeneration drops what a retirement destroys, each step skipped when its
// object is already gone.
//
// The index goes first, because YDB refuses to drop a column an index covers.
// The five columns go in one statement, so a failure leaves all of them or
// none. A table goes only when its comment names this generation: it is the
// record that Ptah created it, and a table nobody admits to owning is not
// Ptah's to drop with every row in it.
func (b *Backend) dropGeneration(
	ctx context.Context, generation embedstore.Generation, destruction embedbackend.RetirementDestruction,
) error {
	described, exists, err := b.describe(ctx, generation.TargetSchema, generation.TargetTable)
	if err != nil || !exists {
		return err
	}
	table := quoteTable(generation.TargetSchema, generation.TargetTable)
	name := tablePath(generation.TargetSchema, generation.TargetTable)
	index := embedgen.IndexName(generation.TargetTable, generation.TargetColumn, generation.Identity)
	if _, found := described.Index(index); found {
		if _, err := b.db.ExecContext(ctx, "ALTER TABLE "+table+" DROP INDEX "+quote(index)); err != nil {
			return fmt.Errorf("drop index %s: %w", index, err)
		}
	}
	if destruction.DropTable {
		want := embedgen.TableComment(generation.Identity)
		found := described.Attributes[commentKey(ydbcomment.Table, "")]
		if found != want {
			return fmt.Errorf(
				"%w: table %s is not Ptah's to drop: retiring generation %s would remove it and every row "+
					"in it, and it carries the comment %q rather than %q. Drop it by hand if it really is this "+
					"generation's storage",
				embedstore.ErrConflict, name, generation.Identity, found, want)
		}
		if _, err := b.db.ExecContext(ctx, "DROP TABLE "+table); err != nil {
			return fmt.Errorf("drop the target table %s of generation %s: %w", name, generation.Identity, err)
		}
		return nil
	}
	if !destruction.DropColumns {
		return nil
	}
	var drops []string
	for _, column := range targetColumns(generation.TargetColumn) {
		if _, found := described.Column(column.Name); found {
			drops = append(drops, "DROP COLUMN "+quote(column.Name))
		}
	}
	if len(drops) == 0 {
		return nil
	}
	if _, err := b.db.ExecContext(ctx, "ALTER TABLE "+table+" "+strings.Join(drops, ", ")); err != nil {
		return fmt.Errorf("drop the columns of generation %s: %w", generation.Identity, err)
	}
	// The comment prepare wrote on the vector column is an attribute of the
	// table, so it outlives the column unless it is removed with it.
	if _, found := described.Attributes[commentKey(ydbcomment.Column, generation.TargetColumn)]; found {
		if err := b.comment(ctx, ydbcomment.Statement{
			Object: ydbcomment.Column, Path: name, Name: generation.TargetColumn,
		}); err != nil {
			return fmt.Errorf("remove the comment of column %s: %w", generation.TargetColumn, err)
		}
	}
	return nil
}
