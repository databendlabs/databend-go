package godatabend

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegrationResumeQueryWithStatePreservesRows(t *testing.T) {
	cfg := integrationTestConfig(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	firstClient := NewAPIClientFromConfig(cfg)
	firstClient.MaxRowsPerPage = 1

	startResp, err := firstClient.StartQuery(ctx, "SELECT number FROM numbers(5) ORDER BY number")
	require.NoError(t, err)
	require.NotNil(t, startResp)
	require.False(t, startResp.ReadFinished())

	state := firstClient.GetState()
	require.NotNil(t, state)
	require.NotEmpty(t, state.SessionState)

	secondClient := NewAPIClientFromConfig(cfg).WithState(state)
	secondClient.MaxRowsPerPage = 1

	finalResp, err := secondClient.PollUntilQueryEnd(ctx, startResp)
	require.NoError(t, err)
	require.NotNil(t, finalResp)
	require.True(t, finalResp.ReadFinished())
	defer func() {
		require.NoError(t, secondClient.CloseQuery(context.Background(), finalResp))
	}()

	for i := 0; i < 5; i++ {
		value, ok := finalResp.cellString(i, 0)
		require.True(t, ok)
		assert.Equal(t, strconv.Itoa(i), value)
	}

	require.NotNil(t, finalResp.Stats)
	assert.Equal(t, uint64(5), finalResp.Stats.ResultProgress.Rows)
}

func TestIntegrationStateRestoresSessionSettings(t *testing.T) {
	cfg := integrationTestConfig(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := NewAPIClientFromConfig(cfg)
	_, err := client.QuerySync(ctx, "SET max_result_rows = 5")
	require.NoError(t, err)

	state := client.GetState()
	require.NotNil(t, state)
	require.NotEmpty(t, state.SessionState)

	restored := NewAPIClientFromConfig(cfg).WithState(state)
	resp, err := restored.QuerySync(ctx, "SELECT value FROM system.settings WHERE name = 'max_result_rows'")
	require.NoError(t, err)

	value, ok := resp.cellString(0, 0)
	require.True(t, ok)
	assert.Equal(t, "5", value)

	roundedState := restored.GetState()
	require.NotNil(t, roundedState)
	require.NotEmpty(t, roundedState.SessionState)

	var sessionState SessionState
	require.NoError(t, json.Unmarshal([]byte(roundedState.SessionState), &sessionState))
	assert.Equal(t, "5", sessionState.Settings["max_result_rows"])
}

func integrationTestConfig(t *testing.T) *Config {
	t.Helper()

	dsn := os.Getenv("TEST_DATABEND_DSN")
	if dsn == "" {
		t.Skip("TEST_DATABEND_DSN is not set")
	}

	cfg, err := ParseDSN(dsn)
	require.NoError(t, err)
	return cfg
}
