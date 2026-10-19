package descriptor

// Savepoint holds the keywords a savepoint's name follows in each statement,
// such as "SAVEPOINT". An empty Create means the engine has no savepoint; an
// empty Release means the savepoint lives until the transaction ends.
//
// A savepoint brackets one statement or one group of statements inside a
// transaction, so a refused one is rolled back and the transaction is kept.
// migration/seeder brackets each seed file in one, and internal/schemaclean
// each drop of a scoped cleanup.
type Savepoint struct {
	// Create sets the savepoint, as "SAVEPOINT" or "SAVE TRANSACTION".
	Create string `json:"create,omitempty"`
	// Rollback returns to it, as "ROLLBACK TO SAVEPOINT".
	Rollback string `json:"rollback,omitempty"`
	// Release ends it, as "RELEASE SAVEPOINT".
	Release string `json:"release,omitempty"`
}
