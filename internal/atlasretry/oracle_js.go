//go:build js

package atlasretry

// oracleSerializationFailure always reports false here. The go-ora driver does
// not build for js/wasm -- its network layer needs syscall.Sendmsg and MSG_OOB
// -- so no connection to an Oracle server exists on this platform and no error
// can have come from one.
func oracleSerializationFailure(error) bool { return false }
