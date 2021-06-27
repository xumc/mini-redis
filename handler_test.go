package main

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_commandReader(t *testing.T) {
	cases := []struct {
		str string
		expect []string
	}{
		{
			"*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
			[]string{"set","key","value"},
		},
		{
			"*2\r\n$3\r\nget\r\n$3\r\nkey\r\n",
			[]string{"get","key"},
		},
		{
			"*3\r\n$3\r\ndel\r\n$2\r\nk1\r\n$2\r\nk2\r\n",
			[]string{"del","k1","k2"},
		},
	}

	for _, c := range cases {
		buf := bytes.NewReader([]byte(c.str))
		cmdhdr := &commandHandler{Reader: buf}

		cmd, err := cmdhdr.Next()
		assert.Nil(t, err)
		assert.EqualValues(t, c.expect, cmd)
	}
}

