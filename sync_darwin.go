//go:build darwin
// +build darwin

package main

import (
	"golang.org/x/sys/unix"
	"os"
)

func syncFile(wal *os.File) error {
	_, err := unix.FcntlInt(wal.Fd(), unix.F_FULLFSYNC, 0)
	return err
}
