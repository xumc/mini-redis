// Copyright 2011 Evan Shaw. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// These tests are adapted from gommap: http://labix.org/gommap
// Copyright (c) 2010, Gustavo Niemeyer <gustavo@niemeyer.net>

package main

import (
	"bytes"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"golang.org/x/sys/unix"
	"io/ioutil"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

var testData = []byte("0123456789ABCDEF")
var testPath = filepath.Join(os.TempDir(), "testdata")

func init() {
	f := openFile(os.O_RDWR | os.O_CREATE | os.O_TRUNC)
	f.Write(testData)
	f.Close()
}

func openFile(flags int) *os.File {
	f, err := os.OpenFile(testPath, flags, 0644)
	if err != nil {
		panic(err.Error())
	}
	return f
}

func TestUnmap(t *testing.T) {
	f := openFile(os.O_RDONLY)
	defer f.Close()
	m, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		t.Errorf("error mapping: %s", err)
	}
	if err := m.Unmap(); err != nil {
		t.Errorf("error unmapping: %s", err)
	}
}

func TestMe(t *testing.T) {
	f := openFile(os.O_RDWR)
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		t.Errorf(err.Error())
	}
	length := int(fi.Size())

	m, err := unix.Mmap(int(f.Fd()), 0, length, unix.PROT_WRITE|unix.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		t.Errorf("error mapping: %s", err.Error())
	}

	db := struct{
		mm []byte
	}{
		mm: m,
	}

	defer unix.Munmap(db.mm)

	fmt.Println(string(db.mm))

	db.mm[9] = 'X'
	unix.Msync(db.mm, unix.MS_SYNC)

	fileData, err := ioutil.ReadAll(f)
	if err != nil {
		t.Errorf("error reading file: %s", err)
	}

	fmt.Println(string(fileData))

	// leave things how we found them
	db.mm[9] = '9'
	unix.Msync(db.mm, unix.MS_SYNC)
}

func TestReadWrite(t *testing.T) {
	f := openFile(os.O_RDWR)
	defer f.Close()
	m, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		t.Errorf("error mapping: %s", err)
	}
	defer m.Unmap()
	if !bytes.Equal(testData, m) {
		t.Errorf("data != testData: %q, %q", m, testData)
	}

	m[9] = 'X'
	m.Flush()

	fileData, err := ioutil.ReadAll(f)
	if err != nil {
		t.Errorf("error reading file: %s", err)
	}
	if !bytes.Equal(fileData, []byte("012345678XABCDEF")) {
		t.Errorf("file wasn't modified")
	}

	// leave things how we found them
	m[9] = '9'
	m.Flush()
}

func TestProtFlagsAndErr(t *testing.T) {
	f := openFile(os.O_RDONLY)
	defer f.Close()
	if _, err := mmap.Map(f, mmap.RDWR, 0); err == nil {
		t.Errorf("expected error")
	}
}

func TestFlags(t *testing.T) {
	f := openFile(os.O_RDWR)
	defer f.Close()
	m, err := mmap.Map(f, mmap.COPY, 0)
	if err != nil {
		t.Errorf("error mapping: %s", err)
	}
	defer m.Unmap()

	m[9] = 'X'
	m.Flush()

	fileData, err := ioutil.ReadAll(f)
	if err != nil {
		t.Errorf("error reading file: %s", err)
	}
	if !bytes.Equal(fileData, testData) {
		t.Errorf("file was modified")
	}
}

// Test that we can map files from non-0 offsets
// The page size on most Unixes is 4KB, but on Windows it's 64KB
func TestNonZeroOffset(t *testing.T) {
	const pageSize = 65536

	// Create a 2-page sized file
	bigFilePath := filepath.Join(os.TempDir(), "nonzero")
	fileobj, err := os.OpenFile(bigFilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		panic(err.Error())
	}

	bigData := make([]byte, 2*pageSize, 2*pageSize)
	fileobj.Write(bigData)
	fileobj.Close()

	// Map the first page by itself
	fileobj, err = os.OpenFile(bigFilePath, os.O_RDONLY, 0)
	if err != nil {
		panic(err.Error())
	}
	m, err := mmap.MapRegion(fileobj, pageSize, mmap.RDONLY, 0, 0)
	if err != nil {
		t.Errorf("error mapping file: %s", err)
	}
	m.Unmap()
	fileobj.Close()

	// Map the second page by itself
	fileobj, err = os.OpenFile(bigFilePath, os.O_RDONLY, 0)
	if err != nil {
		panic(err.Error())
	}
	m, err = mmap.MapRegion(fileobj, pageSize, mmap.RDONLY, 0, pageSize)
	if err != nil {
		t.Errorf("error mapping file: %s", err)
	}
	err = m.Unmap()
	if err != nil {
		t.Error(err)
	}

	m, err = mmap.MapRegion(fileobj, pageSize, mmap.RDONLY, 0, 1)
	if err == nil {
		t.Error("expect error because offset is not multiple of page size")
	}

	fileobj.Close()
}
