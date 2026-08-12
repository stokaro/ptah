package capabilityline

const (
	// MySQL8 is the measured MySQL 8 LTS release line.
	MySQL8 = "8.4"
	// MySQL9 is the measured MySQL 9 LTS release line.
	MySQL9 = "9.7"
	// MySQL26 is the newest measured MySQL release line.
	MySQL26 = "26.7"
	// MariaDB10 is the measured MariaDB 10 LTS release line.
	MariaDB10 = "10.11"
	// MariaDB114 is the measured MariaDB 11.4 release line.
	MariaDB114 = "11.4"
	// MariaDB11LTS is the measured MariaDB 11 LTS release line.
	MariaDB11LTS = "11.8"
	// MariaDB12 is the newest measured MariaDB release line.
	MariaDB12 = "12.3"
	// CockroachDB25 is the measured CockroachDB 25 release line.
	CockroachDB25 = "25.4"
	// CockroachDB26 is the measured CockroachDB 26 release line.
	CockroachDB26 = "26.2"
)

// MySQLMeasured returns every MySQL release line with direct matrix evidence.
func MySQLMeasured() []string {
	return []string{MySQL8, MySQL9, MySQL26}
}

// MariaDBMeasured returns every MariaDB release line with direct matrix evidence.
func MariaDBMeasured() []string {
	return []string{MariaDB10, MariaDB114, MariaDB11LTS, MariaDB12}
}

// CockroachDBMeasured returns every CockroachDB release line with direct matrix evidence.
func CockroachDBMeasured() []string {
	return []string{CockroachDB25, CockroachDB26}
}
