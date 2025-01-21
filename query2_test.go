//go:build rows_hack

package godatabend

import (
	"database/sql"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestXXX(t *testing.T) {
	db, err := sql.Open("databend", "http://root@localhost:8000?presign=on")
	if err != nil {
		t.Fatal(err)
	}
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)

	rows, err := db.Query("select xxxx;")
	if err != nil {
		t.Fatal(err)
	}

	for rows.Next() {
		fmt.Println("--------------------------------")
		raw := LastRawRow(rows)
		for _, x := range raw {
			if x == nil {
				fmt.Println("nil")
			} else {
				fmt.Println(*x)
			}
		}

		types, err := rows.ColumnTypes()
		if err != nil {
			t.Fatal(err)
		}

		for _, t := range types {
			fmt.Printf("%+v\n", t)
		}

		row := make([]any, len(types))
		for i := range row {
			var v any
			row[i] = &v
		}

		err = rows.Scan(row...)
		require.NoError(t, err)

		for _, x := range row {
			fmt.Printf("%#v\n", reflect.ValueOf(x).Elem().Interface())
		}
	}
	// err = rows.Err()
	// require.NoError(t, err)

	err = rows.Close()
	require.NoError(t, err)
}
