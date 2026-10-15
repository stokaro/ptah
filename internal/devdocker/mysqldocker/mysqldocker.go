// Package mysqldocker declares the MySQL and MariaDB images internal/devdocker
// starts from a docker URL.
package mysqldocker

import (
	"fmt"

	"ptah.run/internal/devdocker/dockerengine"
)

// blockAttributes are the attributes of an atlas.hcl docker block of either
// engine. Neither takes `database`: the schema is the database.
var blockAttributes = []string{"image", "schema", "baseline", "env", "timeout"}

// Engines returns the MySQL-family engines, each under the name a docker URL
// gives it. Each call returns new values.
//
// The names are the spellings the pinned community binary accepts, measured
// one at a time. MariaDB answers to two of them, which start one container
// and cannot drift apart; only `mariadb` has an atlas.hcl docker block.
func Engines() []dockerengine.Engine {
	maria := mariaDB()
	maria.Name = "maria"
	mariaBlock := mariaDB()
	mariaBlock.Name = "mariadb"
	mariaBlock.BlockAttributes = append([]string(nil), blockAttributes...)
	return []dockerengine.Engine{mySQL(), maria, mariaBlock}
}

func mySQL() dockerengine.Engine {
	return dockerengine.Engine{
		Name:  "mysql",
		Image: "mysql",
		Port:  "3306",
		Env: func(database, password string) []string {
			return []string{
				"MYSQL_ROOT_PASSWORD=" + password,
				"MYSQL_DATABASE=" + database,
			}
		},
		// The database is NOT escaped here, unlike the PostgreSQL URL. A MySQL
		// DSN is not a URL: the driver reads the name between the last `/` and
		// the `?` literally and never percent-decodes it, so escaping would
		// connect to a database called `foo%23bar` while the container created
		// `foo#bar`. The one delimiter that would break the DSN, `?`, is
		// refused by devdocker.Parse instead.
		URL: func(hostPort, database, password, query string) string {
			return fmt.Sprintf("mysql://root:%s@tcp(%s)/%s%s", password, hostPort, database, dockerengine.QuerySuffix(query))
		},
		BlockAttributes: append([]string(nil), blockAttributes...),
	}
}

// mariaDB is the MariaDB engine without a name.
func mariaDB() dockerengine.Engine {
	return dockerengine.Engine{
		Image: "mariadb",
		Port:  "3306",
		Env: func(database, password string) []string {
			return []string{
				"MARIADB_ROOT_PASSWORD=" + password,
				"MARIADB_DATABASE=" + database,
			}
		},
		URL: func(hostPort, database, password, query string) string {
			return fmt.Sprintf("mariadb://root:%s@tcp(%s)/%s%s", password, hostPort, database, dockerengine.QuerySuffix(query))
		},
	}
}
