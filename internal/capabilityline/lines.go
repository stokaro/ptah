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
	// ClickHouse24 is the measured ClickHouse 24 release line, and the only one
	// below the 24.11 CHECK GRANT step.
	ClickHouse24 = "24.10"
	// ClickHouse25 is the measured ClickHouse 25 LTS release line.
	ClickHouse25 = "25.8"
	// ClickHouse263 is the measured ClickHouse 26.3 LTS release line.
	ClickHouse263 = "26.3"
	// ClickHouse26 is the newest measured ClickHouse release line.
	ClickHouse26 = "26.7"
	// YugabyteDB2024 is the measured YugabyteDB 2024 LTS release line, and the
	// only one below the PostgreSQL 11 to 15 engine swap.
	YugabyteDB2024 = "2024.2"
	// YugabyteDB2025 is the measured YugabyteDB 2025 release line, and the
	// first one above that swap.
	YugabyteDB2025 = "2025.2"
	// YugabyteDB2026 is the newest measured YugabyteDB release line.
	YugabyteDB2026 = "2026.1"
)

// YugabyteDBMeasured returns every YugabyteDB release line with direct matrix
// evidence.
func YugabyteDBMeasured() []string {
	return []string{YugabyteDB2024, YugabyteDB2025, YugabyteDB2026}
}

// ClickHouseMeasured returns every ClickHouse release line with direct matrix
// evidence.
func ClickHouseMeasured() []string {
	return []string{ClickHouse24, ClickHouse25, ClickHouse263, ClickHouse26}
}

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
