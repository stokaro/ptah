package atlas

import (
	"strconv"

	"go.5x5.cz/ptah/internal/atlasmigrateimport"
)

// flywayVersionTokens is the two-way dictionary between the version an operator
// can see and the version this build executes, for a directory read through
// `?format=flyway` / `--dir-format flyway`.
//
// WHY IT EXISTS. Atlas CE identifies a Flyway migration by the opaque version
// STRING between the prefix letter and the `__` separator; Ptah's migrator
// identifies a migration by an int64, so conversion projects that token onto
// the band-and-slot ordering key documented above
// atlasmigrateimport.flywayComponentSlot. The projection is correct at ordering,
// which is its job. It is wrong as an ADDRESS: `V1.sql` is called `1` by Flyway,
// `1` by the pinned community binary v1.3.0, and `4611686018427469511` here — a
// number that appears in no file the operator wrote (stokaro/ptah#1206).
//
// Measured on the pinned community binary v1.3.0, the token is matched BYTE FOR
// BYTE, not numerically. On a directory holding `V01__a.sql` and `V2__b.sql`:
//
//	migrate set 01   -> exit 0, "Current version is 01 (1 set)", row 01|a
//	migrate set 1    -> exit 1, migration with version "1" not found
//	migrate set 002  -> exit 1, migration with version "002" not found
//	migrate set 2    -> exit 0, "Current version is 2 (2 set)", rows 01|a and 2|b
//
// So the lookup below is a plain string key rather than anything that
// normalizes, and `1` deliberately fails to reach `V01__a.sql`.
//
// It is empty for every layout other than Flyway. The single layout gate lives
// inside [atlasmigrateimport.FlywayCoveredSourceVersions], which reports nothing
// for the others, so the plain-numeric-prefix layouts (golang-migrate, goose,
// dbmate, liquibase) keep resolving and rendering exactly as before — their
// token IS the int64 they convert to, so routing them through here would be the
// identity function anyway, and two independent gates for one rule is a rule no
// test can hold.
type flywayVersionTokens struct {
	// byToken resolves an operator's version token to the version this build
	// executes and records it under.
	byToken map[string]int64
	// byVersion renders an executed version back as the operator's token.
	byVersion map[int64]string
}

// newFlywayVersionTokens indexes the covered migrations of a converted Flyway
// directory. It returns the zero value for every other layout, which behaves as
// "no translation" throughout.
//
// Duplicate tokens cannot reach the index. A directory carrying two files with
// the same Atlas version is refused by rejectDuplicateFlywayVersions while the
// directory is converted, which every caller of this does before it resolves an
// operand, so the last-write-wins in the map below is unreachable rather than a
// silent choice between two files.
//
// A repeatable's token is the empty string — Atlas CE gives `R__a.sql`,
// `R1__a.sql` and `Rfoo.sql` all the version "" — and an empty key is skipped in
// byToken so no operand can resolve to it. It is still carried in byVersion,
// because "" is genuinely what that migration is called.
func newFlywayVersionTokens(covered []atlasmigrateimport.FlywayCoveredSourceVersion) flywayVersionTokens {
	if len(covered) == 0 {
		return flywayVersionTokens{}
	}
	tokens := flywayVersionTokens{
		byToken:   make(map[string]int64, len(covered)),
		byVersion: make(map[int64]string, len(covered)),
	}
	for _, migration := range covered {
		tokens.byVersion[migration.Version] = migration.Token
		if migration.Token == "" {
			continue
		}
		tokens.byToken[migration.Token] = migration.Version
	}
	return tokens
}

// translates reports whether this directory has a version space of its own. A
// native Atlas directory and every plain-numeric-prefix layout do not, and every
// method below is then the identity.
func (t flywayVersionTokens) translates() bool {
	return len(t.byVersion) != 0
}

// resolve maps an operator's version token to the version this build executes
// it under. The second result is false when this directory holds no migration
// by that name, which is the operand-not-found case the caller reports.
func (t flywayVersionTokens) resolve(token string) (int64, bool) {
	version, ok := t.byToken[token]
	return version, ok
}

// render spells an executed version the way the operator's directory does.
//
// A version this directory does not produce falls back to its decimal form
// rather than to the empty string. That is reachable from a revision table
// holding rows a baseline squash has retired, and printing nothing there would
// turn a row an operator can still see in their database into a blank.
func (t flywayVersionTokens) render(version int64) string {
	if token, ok := t.byVersion[version]; ok {
		return token
	}
	return strconv.FormatInt(version, 10)
}
