//go:build !linux && !darwin
// +build !linux,!darwin

package main

import (
	"os"
)

func syncFile(wal *os.File) error {
	return wal.Sync()
}