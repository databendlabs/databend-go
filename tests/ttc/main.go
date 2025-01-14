package main

import (
	"bufio"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"

	_ "github.com/datafuselabs/databend-go"
	"github.com/pkg/errors"
)

type Server struct {
	listener net.Listener
	db       *sql.DB
}

func (s *Server) Serve() error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return errors.WithStack(err)
		}
		go func() {
			err := s.handleConn(conn)
			if err != nil {
				slog.Error("handle conn error", "error", err)
			}
		}()
	}
}

func (s *Server) handleConn(conn net.Conn) error {
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	defer conn.Close()
	for {
		var n uint32
		err := binary.Read(rw, binary.BigEndian, &n)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return errors.WithStack(err)
		}

		sql := make([]byte, n)
		_, err = io.ReadFull(rw, sql)
		if err != nil {
			return errors.WithStack(err)
		}

		res := NewResponse(s.db.Query(string(sql)))
		resData, err := json.Marshal(res)
		if err != nil {
			return errors.WithStack(err)
		}

		err = binary.Write(rw, binary.BigEndian, uint32(len(resData)))
		if err != nil {
			return errors.WithStack(err)
		}

		_, err = rw.Write(resData)
		if err != nil {
			return errors.WithStack(err)
		}

		err = rw.Flush()
		if err != nil {
			return errors.WithStack(err)
		}
	}
}

type Response struct {
	Values [][]*string `json:"values"`
	Error  *string     `json:"error"`
}

func NewResponse(rows *sql.Rows, err error) (res *Response) {
	res = &Response{}
	var errMsg string
	defer func() {
		if errMsg != "" {
			res.Error = &errMsg
			res.Values = nil
		}
	}()

	if err != nil {
		errMsg = err.Error()
		return
	}

	for rows.Next() {
		types, err := rows.ColumnTypes()
		if err != nil {
			errMsg = err.Error()
			return
		}

		row := make([]any, len(types))
		for i := range row {
			var v any
			row[i] = &v
		}
		err = rows.Scan(row...)
		if err != nil {
			errMsg = err.Error()
			return
		}

		values := make([]*string, 0, len(row))
		for _, v := range row {
			if v == nil {
				values = append(values, nil)
			} else {
				s := fmt.Sprintf("%v", v)
				values = append(values, &s)
			}
		}
		res.Values = append(res.Values, values)
	}

	err = rows.Close()
	if err != nil {
		errMsg = err.Error()
	}
	return
}

func main() {
	dataSource := os.Getenv("TTC_DSN")
	if dataSource == "" {
		slog.Error("TTC_DSN is not set")
		return
	}

	addr := os.Getenv("TTC_LISTEN_ADDR")
	if addr == "" {
		slog.Error("TTC_LISTEN_ADDR is not set")
		return
	}

	db, err := sql.Open("databend", dataSource)
	if err != nil {
		slog.Error("failed to open db", "error", err)
		return
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("failed to listen", "error", err)
		return
	}

	s := &Server{
		listener: listener,
		db:       db,
	}

	err = s.Serve()
	if err != nil {
		slog.Error("failed to serve", "error", err)
	}
}
