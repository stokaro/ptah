package migrator_test

import (
	"errors"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/migrator"
)

type statementValidatorRecorder struct {
	statements []string
	reject     string
	err        error
}

func (r *statementValidatorRecorder) ValidateStatement(statement string) error {
	r.statements = append(r.statements, statement)
	if statement == r.reject {
		return r.err
	}
	return nil
}

func TestFSMigrationProvider_StatementValidatorRejectsBeforeExecution(t *testing.T) {
	c := qt.New(t)
	validationErr := errors.New("unsafe second statement")
	validator := &statementValidatorRecorder{
		reject: "ATTACH DATABASE 'other.db' AS aux",
		err:    validationErr,
	}
	interceptor := &recordingInterceptor{}
	fsys := fstest.MapFS{
		"1_validate.sql": {
			Data: []byte(
				"CREATE TABLE users (id INTEGER PRIMARY KEY);\n" +
					"ATTACH DATABASE 'other.db' AS aux;\n",
			),
		},
	}
	provider, err := migrator.NewFSMigrationProvider(
		fsys,
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
		migrator.WithStatementValidator(validator),
		migrator.WithStatementInterceptor(interceptor),
	)
	c.Assert(err, qt.IsNil)
	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 1)

	err = migrations[0].Up(t.Context(), nil)

	c.Assert(err, qt.ErrorIs, validationErr)
	c.Assert(validator.statements, qt.DeepEquals, []string{
		"CREATE TABLE users (id INTEGER PRIMARY KEY)",
		"ATTACH DATABASE 'other.db' AS aux",
	})
	c.Assert(interceptor.statements, qt.HasLen, 0)
}

func TestFSMigrationProvider_StatementValidatorComposesWithInterceptor(t *testing.T) {
	c := qt.New(t)
	validator := &statementValidatorRecorder{}
	interceptor := &recordingInterceptor{}
	fsys := fstest.MapFS{
		"1_validate.sql": {
			Data: []byte("SELECT 1;\nSELECT 2;\n"),
		},
	}
	provider, err := migrator.NewFSMigrationProvider(
		fsys,
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
		migrator.WithStatementValidator(validator),
		migrator.WithStatementInterceptor(interceptor),
	)
	c.Assert(err, qt.IsNil)
	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 1)

	err = migrations[0].Up(t.Context(), nil)

	c.Assert(err, qt.IsNil)
	c.Assert(validator.statements, qt.DeepEquals, []string{"SELECT 1", "SELECT 2"})
	c.Assert(interceptor.statements, qt.DeepEquals, []string{"SELECT 1", "SELECT 2"})
}
