//go:build sqlcipher
// +build sqlcipher

package core

/*
#cgo CFLAGS: -DSQLITE_HAS_CODEC
#cgo LDFLAGS: -L${SRCDIR} -lsqlcipher -lcrypto -Wl,-rpath,'$ORIGIN'
*/
import "C"
