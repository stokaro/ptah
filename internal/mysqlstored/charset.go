package mysqlstored

import "strings"

// UTF8CharacterSet reports whether name, a character set as MySQL and MariaDB
// name it, is one of the UTF-8 family: utf8mb4, utf8mb3 or utf8, in any case.
// Every reading here takes a statement's bytes as UTF-8, so it is the
// server's reading only on a session whose client character set is one of
// them. Measured on MySQL 8.4.13 and MariaDB 11.8.9: under latin1 the byte
// 0xA0 is whitespace, so CREATE<0xA0>TRIGGER defines a trigger, and under gbk
// the last byte of 中 and a backtick after it are one character.
func UTF8CharacterSet(name string) bool {
	switch strings.ToLower(name) {
	case "utf8mb4", "utf8mb3", "utf8":
		return true
	default:
		return false
	}
}
