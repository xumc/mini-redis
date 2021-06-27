package main

import (
	"bytes"
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
	"net"
	"strconv"
)

const (
	respOK = "+OK\r\n"
	respError = "-Error \r\n"
)

var (
	separator = []byte{13,10}
	connectionBufSize = 7

	invalidFormat = fmt.Errorf("invalid int when parse int.\n")
)
type commandHandler struct {
	io.Reader
	io.Writer
}

func (crd *commandHandler) Next() ([]string, error) {
	buf := make([]byte, connectionBufSize)

	var strLinesCount string
	var totalArgsCount int64
	var argLen int64
	var result = []string{}

	var c int


	stream := make([]byte, 0)
	for {
		n, err := crd.Read(buf)
		if err != nil {
			return nil, err
		}

		stream = append(stream, buf[:n]...)

		for  {
			p := bytes.Index(stream[c:], separator)
			if p == -1 {
				break
			}

			switch stream[c] {
			case '*':
				strLinesCount = string(stream[c+1 : (c+p)])
				var err error
				totalArgsCount, err = strconv.ParseInt(strLinesCount, 10, 64)
				if err != nil {
					logrus.Errorf("invalid lines count %s. %s\n", strLinesCount, err)
					return nil, invalidFormat
				}
			case '$':
				strArgCount := string(stream[c+1 : (c+p)])
				var err error
				argLen, err = strconv.ParseInt(strArgCount, 10, 64)
				if err != nil {
					logrus.Errorf("invalid lines count %s. %s\n", strLinesCount, err)
					return nil, invalidFormat
				}
			default:
				arg := string(stream[c:(c + int(argLen))])
				result = append(result, arg)
				if int64(len(result)) == totalArgsCount {
					return  result, nil
				}
			}

			c = c + p + len(separator)
		}
	}

	return  result, nil
}

func (cmd *commandHandler) WriteString(str string) error {
	_, err := cmd.Writer.Write([]byte(str))
	if err != nil {
		logrus.Fatalf("write str resp to conn error. %s", err.Error())
	}
	return nil
}

func (cmd *commandHandler) Write(bs []byte) error {
	_, err := cmd.Writer.Write(bs)
	if err != nil {
		logrus.Fatalf("write byte resp to conn error. %s", err.Error())
	}
	return nil
}

func handleConn(conn net.Conn, db *DB) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Errorf("connection to %s failed due to reason: %s", conn.RemoteAddr().String(), r)
		}
	}()

	cmdhdr := &commandHandler{Reader: conn, Writer: conn}

	for {
		cmd, err := cmdhdr.Next()
		if err != nil {
			if err == io.EOF {
				err := conn.Close()
				if err != nil {
					logrus.Fatalf("close error %s\n", err.Error())
				}
				return
			} else {
				logrus.Fatalf("error happened when parse command.")
			}

		}

		var switchError error
		switch cmd[0] {
		case "set":
			switchError = db.SetString(cmd[1], cmd[2])
			if switchError == nil {
				cmdhdr.WriteString(respOK)
			}
		case "get":
			val, err := db.GetString(cmd[1])
			if err != nil {
				if err == NotFoundError {
					cmdhdr.WriteString(fmt.Sprintf("+(nil)\r\n"))
				} else {
					switchError = err
				}
			} else {
				cmdhdr.WriteString(fmt.Sprintf("+%s\r\n",val))
			}
		case "del":
			var result []bool
			result, switchError := db.DeleteString(cmd[1:]...)
			if switchError == nil {
				var deleteCount int
				for _, realDel := range result {
					if realDel {
						deleteCount++
					}
				}
				cmdhdr.Write([]byte(fmt.Sprintf(":%d\r\n", deleteCount)))
			}

		default:
			logrus.Errorf("unsupport cmd %s", cmd[0])
		}
		if switchError != nil {
			_, err := conn.Write([]byte(respError))
			if err != nil {
				logrus.Fatalf("db error. %s\n", err.Error())
			}
			continue
		}
	}
}