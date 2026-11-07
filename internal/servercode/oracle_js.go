//go:build js

package servercode

// oracleErrorNumber is always zero here. The go-ora driver does not build for
// js/wasm -- its network layer needs syscall.Sendmsg and MSG_OOB -- so no
// connection to an Oracle server exists on this platform and no error can have
// come from one.
func oracleErrorNumber(error) int { return 0 }
