package assistconfig

// modeBitsAreMeaningful reports whether a file's permission bits say who can
// read it.
//
// They do not on Windows: os.Stat synthesizes 0o666 or 0o444 from the read-only
// attribute, so a check on the group and other bits would pass for every file
// and assert nothing. Access there is an ACL question, which is a different
// mechanism this package does not read.
func modeBitsAreMeaningful() bool { return false }
