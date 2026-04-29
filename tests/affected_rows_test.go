package tests

import (
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	_ "github.com/datafuselabs/databend-go" // Import the Databend driver
)

func TestAffectedRows(t *testing.T) {
	tableName := fmt.Sprintf("books_%d", time.Now().UnixNano())
	defer cleanupTable(dsn, tableName)

	err := selectExec(dsn)
	require.NoError(t, err, "select exec failed")

	err = createAffectedTable(dsn, tableName)
	require.NoError(t, err, "create affected table failed")
	affectedRows, err := updateTable(dsn, tableName)
	require.NoError(t, err, "update table failed")
	assert.Equal(t, int64(2), affectedRows)

	affectedRowsDelete, err := deleteTable(dsn, tableName)
	require.NoError(t, err, "delete table failed")
	assert.Equal(t, int64(2), affectedRowsDelete)
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

func createAffectedTable(dsn, tableName string) error {
	db, err := sql.Open("databend", dsn)
	if err != nil {
		return fmt.Errorf("failed to connect. %v, err: %v", dsn, err)
	}
	defer db.Close()

	query := fmt.Sprintf("CREATE TABLE %s (id INT, title STRING, author STRING)", tableName)
	_, err = db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create table. %v, err: %v", query, err)
	}

	fmt.Println("Table created successfully.")

	// Insert sample data
	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (id, title, author) VALUES (1, '1984', 'George Orwell')", tableName))
	if err != nil {
		return fmt.Errorf("failed to insert data. %v, err: %v", query, err)
	}

	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (id, title, author) VALUES (1, 'To Kill a Mockingbird', 'Harper Lee')", tableName))
	if err != nil {
		return fmt.Errorf("failed to insert data. %v, err: %v", query, err)
	}

	return nil
}

func cleanupTable(dsn, tableName string) error {
	db, err := sql.Open("databend", dsn)
	if err != nil {
		return fmt.Errorf("failed to connect. %v, err: %v", dsn, err)
	}
	defer db.Close()

	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	_, err = db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to drop table. %v, err: %v", query, err)
	}
	fmt.Println("Table dropped successfully.")
	return nil
}

func updateTable(dsn, tableName string) (int64, error) {
	db, err := sql.Open("databend", dsn)
	if err != nil {
		return 0, fmt.Errorf("failed to connect. %v, err: %v", dsn, err)
	}
	defer db.Close()

	query := fmt.Sprintf("UPDATE %s SET title = 'Nineteen Eighty-Four' WHERE id = 1", tableName)
	result, err := db.Exec(query)
	if err != nil {
		return 0, fmt.Errorf("failed to update table. %v, err: %v", query, err)
	}

	// get affect rows
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get affected rows. %v, err: %v", query, err)
	}

	fmt.Println("Table updated successfully.")
	return rowsAffected, nil
}

func deleteTable(dsn, tableName string) (int64, error) {
	db, err := sql.Open("databend", dsn)
	if err != nil {
		return 0, fmt.Errorf("failed to connect. %v, err: %v", dsn, err)
	}
	defer db.Close()

	query := fmt.Sprintf("DELETE FROM %s WHERE id = 1", tableName)
	result, err := db.Exec(query)
	if err != nil {
		return 0, fmt.Errorf("failed to delete table. %v, err: %v", query, err)
	}

	// get affect rows
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get affected rows. %v, err: %v", query, err)
	}

	fmt.Println("Table deleted successfully.")
	return rowsAffected, nil
}
