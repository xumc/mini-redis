// Copyright 2011 Evan Shaw. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// These tests are adapted from gommap: http://labix.org/gommap
// Copyright (c) 2010, Gustavo Niemeyer <gustavo@niemeyer.net>

package main

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type op struct {
	typ string // S => set, D => delete, G => get
	key interface{}
	val interface{}
}

func executeCases(t *testing.T, cases []op) {
	dir := t.TempDir()
	path := filepath.Join(dir, t.Name()+"_db")
	err := os.MkdirAll(filepath.Dir(path), 0777)
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	db, err := LoadOrCreateDbFromFile(path)
	assert.Nil(t, err)
	defer db.Close()

	for _, op := range cases {
		switch op.typ {
		case "S":
			err := db.SetString(op.key.(string), op.val.(string))
			assert.Nil(t, err)
		case "D":
			re, err := db.DeleteString(op.key.([]string)...)
			for i, deleted := range re {
				assert.Equal(t, op.val.([]bool)[i], deleted)
			}
			assert.Nil(t, err)
		case "G":
			v, err := db.GetString(op.key.(string))
			if err == NotFoundError {
				assert.Equal(t, op.val, "not found")
			} else {
				assert.Equal(t, op.val, v)
			}
		}
	}
}

func Test_rune(t *testing.T) {
	fmt.Println([]byte(separator))
	cs := []op{
		{"S", "我是", "中国人"},
		{"G", "我是", "中国人"},
		{"D", []string{"我是"}, []bool{true}},
	}

	executeCases(t, cs)
}

func Test_multi_op(t *testing.T) {
	cs := []op{
		{"S", "hello0", "world0"},
		{"S", "hello1", "world1"},
		{"S", "hello2", "world2"},
		{"S", "hello3", "world3"},
		{"S", "hello4", "world4"},
		{"S", "hello5", "world5"},
		{"S", "hello6", "world6"},
		{"S", "hello7", "world7"},
		{"S", "hello8", "world8"},
		{"S", "hello9", "world9"},
		{"S", "hello5", "1234567890"},
		{"S", "hello6", "ABC"},
		{"D", []string{"hello0"}, []bool{true}},
		{"D", []string{"hello3"}, []bool{true}},
		{"D", []string{"hello5"}, []bool{true}},
		{"D", []string{"hello9"}, []bool{true}},
		{"G", "hello0", "not found"},
		{"G", "hello1", "world1"},
		{"G", "hello2", "world2"},
		{"G", "hello3", "not found"},
		{"G", "hello4", "world4"},
		{"G", "hello5", "not found"},
		{"G", "hello6", "ABC"},
		{"G", "hello7", "world7"},
		{"G", "hello8", "world8"},
		{"G", "hello9", "not found"},
	}

	executeCases(t, cs)
}

func Test_large_kv(t *testing.T) {
	largeKey := strings.Repeat("abc", 10000)
	largeValue := strings.Repeat("abc", 10000)

	t.Run("large value", func(t *testing.T) {

		cases := []op{
			{"S", "largeValue", largeValue},
			{"G", "largeValue", largeValue},
		}

		executeCases(t, cases)
	})

	t.Run("large key", func(t *testing.T) {
		cases := []op{
			{"S", largeKey, "large_key"},
			{"G", largeKey, "large_key"},
		}
		executeCases(t, cases)
	})

	t.Run("large value and large key", func(t *testing.T) {
		cases := []op{
			{"S", "largeValue", largeValue},
			{"G", "largeValue", largeValue},
			{"S", largeKey, "large_key"},
			{"G", largeKey, "large_key"},
		}

		executeCases(t, cases)
	})
}

func Test_update(t *testing.T) {
	t.Run("insufficient free space in page", func(t *testing.T) {
		largeValue := strings.Repeat("abc", 1000000)
		cases := []op{
			{"S", "key", "small value"},
			{"S", "key", largeValue},
			{"G", "key", largeValue},
		}

		executeCases(t, cases)
	})

	t.Run("sufficient free space in page", func(t *testing.T) {
		cases := []op{
			{"S", "key", "small value"},
			{"S", "key", "small value 2"},
			{"G", "key", "small value 2"},
		}

		executeCases(t, cases)
	})
}

func Test_delete(t *testing.T) {
	t.Run("insufficient free space in page", func(t *testing.T) {
		cases := []op{
			{"S", "key", "value"},
			{"D", []string{"key"}, []bool{true}},
			{"G", "key", "not found"},
			{"D", []string{"key"}, []bool{false}},
			{"G", "key", "not found"},
		}

		executeCases(t, cases)
	})

	t.Run("two keys", func(t *testing.T) {
		cases := []op{
			{"S", "key1", "value1"},
			{"D", []string{"key1", "key2"}, []bool{true, false}},
			{"G", "key1", "not found"},
			{"G", "key2", "not found"},
		}

		executeCases(t, cases)
	})
}