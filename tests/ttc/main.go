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

	godatabend "github.com/datafuselabs/databend-go"
	"github.com/pkg/errors"
)

type Server struct {
	listener net.Listener
	cfg      *godatabend.Config
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
	db := sql.OpenDB(s.cfg)
	db.SetMaxOpenConns(1)
	defer db.Close()
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

		res := NewResponse(db.Query(string(sql)))
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
			res.Values = [][]*string{}
		}
		if res.Values == nil {
			res.Values = [][]*string{}
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

		res.Values = append(res.Values, godatabend.LastRawRow(rows))
	}

	err = rows.Close()
	if err != nil {
		errMsg = err.Error()
	}
	return
}

func main() {
	dataSource := os.Getenv("DATABEND_DSN")
	if dataSource == "" {
		slog.Error("DATABEND_DSN is not set")
		return
	}

	port := os.Getenv("TTC_PORT")
	if port == "" {
		slog.Error("TTC_PORT is not set")
		return
	}

	cfg, err := godatabend.ParseDSN(dataSource)
	if err != nil {
		slog.Error("failed to parse dsn", "error", err)
		return
	}

	db := sql.OpenDB(cfg)
	err = db.Ping()
	if err != nil {
		slog.Error("failed to ping db", "error", err)
		return
	}
	_ = db.Close()

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		slog.Error("failed to listen", "error", err)
		return
	}

	s := &Server{
		listener: listener,
		cfg:      cfg,
	}

	fmt.Println("Ready to accept connections")
	err = s.Serve()
	if err != nil {
		slog.Error("failed to serve", "error", err)
	}
}
