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
	// CockroachDB26 is the measured CockroachDB 26.2 release line.
	CockroachDB26 = "26.2"
	// CockroachDB263 is the newest measured CockroachDB release line, and the
	// first one carrying CREATE DOMAIN.
	CockroachDB263 = "26.3"
	// ClickHouse24 is the measured ClickHouse 24 release line, and the only one
	// below the 24.11 CHECK GRANT step.
	ClickHouse24 = "24.10"
	// ClickHouse25 is the measured ClickHouse 25 LTS release line.
	ClickHouse25 = "25.8"
	// ClickHouse263 is the measured ClickHouse 26.3 LTS release line.
	ClickHouse263 = "26.3"
	// ClickHouse267 is the measured ClickHouse 26.7 release line, and the one
	// the dialect's statement-level findings are recorded against:
	// engine/builtin/internal/dialects/clickhouse pins them to a live 26.7.3.19
	// throughout.
	ClickHouse267 = "26.7"
	// ClickHouse268 is the newest measured ClickHouse release line.
	//
	// Measured on 26.8.2.7 against the preset the cell declares: 54 rows, 34
	// agreements, 0 disagreements, and the cell's floor of 34 met. Until it was
	// measured this constant named 26.7, so a live 26.8 was past the newest
	// measured line and received the dialect default instead of this line's
	// answer -- which failed the nightly for three consecutive nights on a state
	// the cell's own note had predicted (stokaro/ptah#2802).
	ClickHouse268 = "26.8"
	// ClickHouse269 is the newest measured ClickHouse release line.
	//
	// Measured on 26.9.1.1629 against the preset the cell declares: 58 rows,
	// 35 agreements, 0 disagreements, and the cell's floor of 35 met.
	// Declaring the line without this
	// constant is what the probe refuses: a server past the newest measured
	// line receives the dialect default rather than this line's answer, and
	// the probe exits non-zero rather than papering over the gap.
	ClickHouse269 = "26.9"
	// YugabyteDB2024 is the measured YugabyteDB 2024 LTS release line, and the
	// only one below the PostgreSQL 11 to 15 engine swap.
	YugabyteDB2024 = "2024.2"
	// YugabyteDB2025 is the measured YugabyteDB 2025 release line, and the
	// first one above that swap.
	YugabyteDB2025 = "2025.2"
	// YugabyteDB2026 is the newest measured YugabyteDB release line.
	YugabyteDB2026 = "2026.1"
	// SQLServer2019 is the measured SQL Server 2019 release line.
	SQLServer2019 = "15.0"
	// SQLServer2022 is the measured SQL Server 2022 release line.
	SQLServer2022 = "16.0"
	// SQLServer2025 is the newest measured SQL Server release line.
	SQLServer2025 = "17.0"
	// Oracle21 is the measured Oracle 21 release line, and the only one below
	// the step that added the IF [NOT] EXISTS guards.
	Oracle21 = "21.3"
	// Oracle23 is the newest measured Oracle release line, and the first one
	// carrying those guards.
	Oracle23 = "23.26"
	// YDB251 is the oldest measured YDB release line, and the only one without
	// the 64-bit date and time types or a Decimal of any precision.
	YDB251 = "25.1"
	// YDB252 is the measured YDB 25.2 release line.
	YDB252 = "25.2"
	// YDB253 is the measured YDB 25.3 release line.
	YDB253 = "25.3"
	// YDB254 is the measured YDB 25.4 release line, which answers like 25.3.
	YDB254 = "25.4"
	// YDB261 is the measured YDB 26.1 release line, the first one that adds a
	// column with a default to an existing table.
	YDB261 = "26.1"
	// YDB262 is the newest measured YDB release line, the first one that sets
	// and drops a column default in place.
	YDB262 = "26.2"
)

// YugabyteDBMeasured returns every YugabyteDB release line with direct matrix
// evidence.
func YugabyteDBMeasured() []string {
	return []string{YugabyteDB2024, YugabyteDB2025, YugabyteDB2026}
}

// ClickHouseMeasured returns every ClickHouse release line with direct matrix
// evidence.
func ClickHouseMeasured() []string {
	return []string{ClickHouse24, ClickHouse25, ClickHouse263, ClickHouse267, ClickHouse268, ClickHouse269}
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
	return []string{CockroachDB25, CockroachDB26, CockroachDB263}
}

// SQLServerMeasured returns every SQL Server release line with direct matrix
// evidence.
func SQLServerMeasured() []string {
	return []string{SQLServer2019, SQLServer2022, SQLServer2025}
}

// YDBMeasured returns every YDB release line with a measured preset.
func YDBMeasured() []string {
	return []string{YDB251, YDB252, YDB253, YDB254, YDB261, YDB262}
}

// OracleMeasured returns every Oracle release line with direct matrix evidence.
func OracleMeasured() []string {
	return []string{Oracle21, Oracle23}
}
