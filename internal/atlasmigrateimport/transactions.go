package atlasmigrateimport

import "ptah.run/core/lintext"

// TransactionOwner reports who opens the transaction a migration of this
// layout runs in when the layout's own tool applies the directory. Lint reads
// it to judge a file's transaction control against that tool rather than
// against the migrator that runs the converted copy.
//
// golang-migrate opens none: by default it sends a file as one query, which
// PostgreSQL runs as one implicit transaction, and with x-multi-statement=true
// it sends each statement on its own. Its only BeginTx writes the version row
// (measured on v4.20.1, stokaro/ptah#4953). Every other layout answers the
// zero value, a migrator that wraps each file unless the file opts out. Atlas
// does that. For Goose, dbmate, Flyway and Liquibase it is what the rules
// assume, and each of those tools can mark a migration that must run outside
// a transaction.
func (f Format) TransactionOwner() lintext.TransactionOwner {
	if f == FormatGolangMigrate {
		return lintext.TransactionOwnerFile
	}
	return lintext.TransactionOwnerMigrator
}
