//go:build sqlcipher
// +build sqlcipher

package core

/*
#cgo CFLAGS: -DSQLITE_HAS_CODEC -I${SRCDIR}
#cgo darwin LDFLAGS: -L${SRCDIR} -lsqlcipher -lcrypto -Wl,-rpath,${SRCDIR}
#cgo linux LDFLAGS: -L${SRCDIR} -lsqlcipher -lcrypto -Wl,-rpath,'$ORIGIN'
#cgo LDFLAGS: -L${SRCDIR} -lsqlcipher -lcrypto
*/
import "C"
