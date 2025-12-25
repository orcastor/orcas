//go:build sqlcipher
// +build sqlcipher

package core

/*
#cgo CFLAGS: -DSQLITE_HAS_CODEC -I${SRCDIR}
#cgo LDFLAGS: -L${SRCDIR} -lsqlcipher -lcrypto
#cgo darwin LDFLAGS: -L${SRCDIR} -lsqlcipher -lcrypto -Wl,-rpath,@loader_path
#cgo linux LDFLAGS: -L${SRCDIR} -lsqlcipher -lcrypto -Wl,-rpath,'$ORIGIN'
*/
import "C"
