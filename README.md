# databend-go
Golang driver for [databend cloud](https://www.databend.com/)

## Key features

- Supports native Databend HTTP client-server protocol
- Compatibility with [`database/sql`](#std-databasesql-interface)

# Examples

## Connecting
Connection can be achieved either via a DSN string with the format http://user:password@<host>/database?<query_option>=<value> and sql/Open method such as `https://username:password@app.databend.com:443/test?&org=databend&warehouse=bl`.

```go
func ConnectDSN() error {
    dsn, cfg, err := getDSN()
    if err != nil {
    log.Fatalf("failed to create DSN from Config: %v, err: %v", cfg, err)
    }
    conn, err := sql.Open("databend", dsn)
    if err != nil {
        return err
    }
    return conn.Ping()
}
```

## Connection Settings
- host - the server host
- username/password - auth credentials
- database - select the current default database
- org - the org of your databend cloud account
- warehouse - the warehouse you want to use

## Execution
Once a connection has been obtained, users can issue sql statements for execution via the Exec method.

```go
    dsn, cfg, err := getDSN()
    if err != nil {
    log.Fatalf("failed to create DSN from Config: %v, err: %v", cfg, err)
        }
		conn, err := sql.Open("databend", dsn)
    if err != nil {
        return err
	    }
    conn.Exec(`DROP TABLE IF EXISTS data`)
    _, err = conn.Exec(`
    CREATE TABLE IF NOT EXISTS  data(
        Col1 UInt8,
        Col2 String
    ) 
    `)
    if err != nil {
        return err
    }
    _, err = conn.Exec("INSERT INTO data VALUES (1, 'test-1')")
```

## Querying Row/s
Querying a single row can be achieved using the QueryRow method. This returns a *sql.Row, on which Scan can be invoked with pointers to variables into which the columns should be marshaled. 

```go
row := conn.QueryRow("SELECT * FROM data")
var (
    col1             uint8
    col2, col3, col4 string
    col5            []string
    col6             time.Time
)
if err := row.Scan(&col1, &col2, &col3, &col4, &col5, &col6); err != nil {
    return err
}
```

Iterating multiple rows requires the Query method. This returns a *sql.Rows struct on which Next can be invoked to iterate through the rows. QueryContext equivalent allows passing of a context.

```go
row := conn.QueryRow("SELECT * FROM data")
var (
    col1             uint8
    col2, col3, col4 string
    col5            []string
    col6             time.Time
)
for rows.Next() {
    if err := row.Scan(&col1, &col2, &col3, &col4, &col5, &col6); err != nil {
    return err
    }
    fmt.Printf("row: col1=%d, col2=%s, col3=%s, col4=%s, col5=%v, col6=%v\n", col1, col2, col3, col4, col5, col6)
}
```
