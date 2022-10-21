package main

import (
	"database/sql"
	"fmt"
	"os"
	"reflect"

	log "github.com/sirupsen/logrus"

	dc "github.com/databendcloud/databend-go"
)

// getDSN constructs a DSN based on the test connection parameters
func getDSN() (string, *dc.Config, error) {
	env := func(k string, failOnMissing bool) string {
		if value := os.Getenv(k); value != "" {
			return value
		}
		if failOnMissing {
			log.Fatalf("%v environment variable is not set.", k)
		}
		return ""
	}

	org := env("DATABEND_TEST_ORG", true)
	user := env("DATABEND_TEST_USER", true)
	password := env("DATABEND_TEST_PASSWORD", true)
	warehouse := env("DATABEND_TEST_WAREHOUSE", true)
	host := env("DATABEND_TEST_HOST", false)
	accessToken := env("DATABEND_TEST_ACCESSTOKEN", false)
	refreshToken := env("DATABEND_TEST_REFRESHTOKEN", false)
	var err error
	cfg := dc.NewConfig()
	cfg.Org = org
	cfg.Warehouse = warehouse
	cfg.User = user
	cfg.Password = password
	cfg.Host = host
	cfg.Database = "books"
	cfg.AccessToken = accessToken
	cfg.RefreshToken = refreshToken

	dsn := cfg.FormatDSN()
	return dsn, cfg, err
}

func main() {
	dsn, cfg, err := getDSN()
	if err != nil {
		log.Fatalf("failed to create DSN from Config: %v, err: %v", cfg, err)
	}

	db, err := sql.Open("databend", dsn)
	if err != nil {
		log.Fatalf("failed to connect. %v, err: %v", dsn, err)
	}
	err = db.Ping()
	if err != nil {
		panic(err)
	}
	defer db.Close()
	query := "SELECT * from books limit ?"
	rows, err := db.Query(query, []interface{}{int64(10)}...) // no cancel is allowed
	if err != nil {
		log.Fatalf("failed to run a query. %v, err: %v", query, err)
	}
	res, err := scanValues(rows)
	if err != nil {
		fmt.Printf("scan err %v", err)
	}
	fmt.Println(res)

	fmt.Printf("Congrats! You have successfully run %v with databend DB!\n", query)

	err = selectExec(dsn)
	if err != nil {
		fmt.Printf("exec failed, err:%v", err)
	}
}
func selectExec(dsn string) error {
	db, err := sql.Open("databend", dsn)
	if err != nil {
		return fmt.Errorf("failed to connect. %v, err: %v", dsn, err)
	}
	defer db.Close()
	query := "SELECT ?"

	rows, err := db.Exec(query, []interface{}{1}...) // no cancel is allowed
	if err != nil {
		return fmt.Errorf("failed to run a query. %v, err: %v", query, err)
	}
	fmt.Println(rows.RowsAffected())
	fmt.Printf("Congrats! You have successfully run %v with databend DB!\n", query)
	return nil
}

func scanValues(rows *sql.Rows) (interface{}, error) {
	var err error
	var result [][]interface{}
	ct, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	types := make([]reflect.Type, len(ct))
	for i, v := range ct {
		types[i] = v.ScanType()
	}
	ptrs := make([]interface{}, len(types))
	for rows.Next() {
		if err = rows.Err(); err != nil {
			return nil, err
		}
		for i, t := range types {
			ptrs[i] = reflect.New(t).Interface()
		}
		err = rows.Scan(ptrs...)
		if err != nil {
			return nil, err
		}
		values := make([]interface{}, len(types))
		for i, p := range ptrs {
			values[i] = reflect.ValueOf(p).Elem().Interface()
		}
		result = append(result, values)
	}
	return result, nil
}
