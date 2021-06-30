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
	connectionBufSize = 1024

	invalidFormat = fmt.Errorf("invalid int when parse int.\n")
)
type commandHandler struct {
	io.Reader
	io.Writer
	stream []byte
}

func newCommandHandler(conn net.Conn) *commandHandler {
	return &commandHandler{
		Reader: conn,
		Writer: conn,
		stream: make([]byte, 0),
	}
}

func (crd *commandHandler) Next() ([]string, error) {
	var strLinesCount string
	var totalArgsCount int64 = -1
	var argLen int64 = -1
	var result = make([]string, 0)

	var c int

	for {
		buf := make([]byte, connectionBufSize)
		n, err := crd.Read(buf)
		if err != nil {
			if err == io.EOF {
				if c >= len(crd.stream) {
					return nil, err
				}
			} else {
				return nil, err
			}
		}

		crd.stream = append(crd.stream, buf[:n]...)

		for {
			p := bytes.Index(crd.stream[c:], separator)
			if p == -1 {
				break
			}

			if crd.stream[c] == '*' && totalArgsCount == -1 {
				strLinesCount = string(crd.stream[c+1 : (c + p)])
				var err error
				totalArgsCount, err = strconv.ParseInt(strLinesCount, 10, 64)
				if err != nil {
					logrus.Errorf("invalid lines count %s. %s\n", strLinesCount, err)
					return nil, invalidFormat
				}
			} else if crd.stream[c] == '$' && totalArgsCount != -1 && argLen == -1 {
					strArgCount := string(crd.stream[c+1 : (c + p)])
					var err error
					argLen, err = strconv.ParseInt(strArgCount, 10, 64)
					if err != nil {
						logrus.Errorf("invalid lines count %s. %s\n", strLinesCount, err)
						return nil, invalidFormat
					}
			} else {
				arg := string(crd.stream[c:(c + int(argLen))])
				result = append(result, arg)
				argLen = -1
			}

			c = c + p + len(separator)

			if int64(len(result)) == totalArgsCount {
				crd.stream = crd.stream[c:]
				return result, nil
			}
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
		//if r := recover(); r != nil {
		//	logrus.Errorf("connection to %s failed due to reason: %s", conn.RemoteAddr().String(), r)
		//}
		logrus.Infof("connection closed. remote addr: %s", conn.RemoteAddr().String())

		connCounterGauge.Dec()
	}()

	cmdhdr := newCommandHandler(conn)

	for {
		//s := time.Now()
		cmd, err := cmdhdr.Next()
		//logrus.Infof("%s cost %f", conn.RemoteAddr(), time.Now().Sub(s).Seconds())

		if err != nil {
			if err == io.EOF {
				err := conn.Close()
				if err != nil {
					logrus.Errorf("close error %s\n", err.Error())
				}
				return
			} else {
				logrus.Fatalf("error happened when parse command.")
			}
		}

		logrus.Debugf("recv cmd: %s", cmd)
		recvCmdCount.Inc()

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
					cmdhdr.WriteString(fmt.Sprintf("$-1\r\n"))
				} else {
					switchError = err
				}
			} else {
				cmdhdr.WriteString(fmt.Sprintf("$%d\r\n%s\r\n",len(val), val))
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