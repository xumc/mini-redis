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
		{
			"*3\r\n$3\r\nset\r\n$4\r\n*key\r\n$6\r\n$value\r\n",
			[]string{"set","*key","$value"},
		},
	}

	for _, c := range cases {
		stream := bytes.NewReader([]byte(c.str))
		cmdhdr := &commandHandler{Reader: stream}

		cmd, err := cmdhdr.Next()
		assert.Nil(t, err)
		assert.EqualValues(t, c.expect, cmd)
	}

	// complex case
	joinedCaseStr := cases[0].str + cases[1].str + cases[2].str + cases[3].str
	buf := bytes.NewReader([]byte(joinedCaseStr))
	cmdhdr := &commandHandler{Reader: buf}

	for _, c := range cases {
		cmd, err := cmdhdr.Next()
		assert.Nil(t, err)
		assert.EqualValues(t, c.expect, cmd)
	}
}

