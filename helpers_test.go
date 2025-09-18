package godatabend

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetTableFromInsertQuery(t *testing.T) {
	var args = []string{
		"insert into example",
		"INSERT INTO example",
	}
	for i := range args {
		table, err := getTableFromInsertQuery(args[i])
		assert.NoError(t, err)
		assert.Equal(t, "example", table)
	}
	var wrongArgs = []string{
		"create table example",
		"inssert int example",
	}
	for i := range wrongArgs {
		table, err := getTableFromInsertQuery(wrongArgs[i])
		assert.Error(t, err)
		assert.Empty(t, table)
	}
}

func TestGenerateDescTable(t *testing.T) {
	var args = []string{
		"insert into example",
		"INSERT INTO example",
	}
	for i := range args {
		desc, err := generateDescTable(args[i])
		assert.NoError(t, err)
		assert.Equal(t, "DESC example", desc)
	}
}
