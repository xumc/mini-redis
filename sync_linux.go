//go:build linux
// +build linux

package main

import (
	"os"
	"syscall"
)

func syncFile(wal *os.File) error {
	return syscall.Fdatasync(int(wal.Fd()))
}
