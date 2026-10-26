package migrator

import (
	"context"
	"crypto/sha256"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/renderer"
	"ptah.run/dbschema"
	"ptah.run/internal/inputapproval"
)

// MigrationInput records the value-free inputs for one exact migration
// statement. StatementDigest hashes the statement bytes executed by the
// migrator, after its usual splitting and whitespace trimming.
type MigrationInput struct {
	Version         int64                     `json:"version"`
	VersionKey      string                    `json:"version_key"`
	StatementIndex  int                       `json:"statement_index"`
	StatementDigest string                    `json:"statement_digest"`
	Inputs          []renderer.InputReference `json:"inputs"`
}

func cloneMigrationInputs(inputs []MigrationInput) []MigrationInput {
	result := slices.Clone(inputs)
	for i := range result {
		result[i].Inputs = slices.Clone(result[i].Inputs)
	}
	return result
}

// prepareMigrationInputs freezes references before the observer, approval hook,
// and first migration write. A Go migration without declared SQL receives no
// input approvals and cannot acquire them by asking during execution.
func (m *Migrator) prepareMigrationInputs(ctx context.Context, migrations []*Migration, direction MigrationDirection) (context.Context, []MigrationInput, error) {
	preparer, ok := m.conn.Writer().(dbschema.UnitInputPreparer)
	if !ok {
		return ctx, nil, ctx.Err()
	}
	var units []renderer.Fragment
	var positions []MigrationInput
	for _, migration := range migrations {
		body := migration.UpSQL
		if direction == MigrationDirectionDown {
			body = migration.DownSQL
		}
		for i, sql := range splitSQLStatementsForConnection(m.conn, body) {
			sql = strings.TrimSpace(sql)
			if sql == "" {
				continue
			}
			units = append(units, renderer.Fragment{SQL: sql})
			positions = append(positions, MigrationInput{Version: migration.Version, VersionKey: migration.RevisionVersion(), StatementIndex: i + 1,
				StatementDigest: fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(sql)))})
		}
	}
	prepared, err := preparer.PrepareUnitInputs(ctx, units)
	if err != nil {
		return nil, nil, err
	}
	if len(prepared) != len(units) {
		return nil, nil, fmt.Errorf("%w: execution input preparation changed the migration batch", renderer.ErrInvalidResult)
	}
	var inputs []MigrationInput
	for i, unit := range prepared {
		if unit.Native != nil || unit.SQL != units[i].SQL {
			return nil, nil, fmt.Errorf("%w: execution input preparation changed a migration statement", renderer.ErrInvalidResult)
		}
		if err := unit.Validate(); err != nil {
			return nil, nil, err
		}
		if len(unit.Inputs) != 0 {
			position := positions[i]
			position.Inputs = slices.Clone(unit.Inputs)
			inputs = append(inputs, position)
		}
	}
	ctx, err = inputapproval.With(ctx, m.conn.Info().Dialect, prepared)
	return ctx, inputs, err
}
