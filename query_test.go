package godatabend

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/test-go/testify/require"
)

func Test_SessionState(t *testing.T) {
	ss := &SessionState{
		Database:       "db1",
		Role:           "",
		SecondaryRoles: nil,
		Settings:       map[string]string{},
	}
	buf, err := json.Marshal(ss)
	require.NoError(t, err)
	assert.Equal(t, "{\"database\":\"db1\",\"last_server_info\":{\"id\":\"\",\"start_time\":\"\"}}", string(buf))

	buf = []byte(`{"database":"db1", "secondary_roles": []}`)
	err = json.Unmarshal(buf, ss)
	require.NoError(t, err)
	assert.Equal(t, []string{}, *ss.SecondaryRoles)

	buf = []byte(`{"database":"db1", "secondary_roles": null}`)
	err = json.Unmarshal(buf, ss)
	require.NoError(t, err)
	assert.Nil(t, ss.SecondaryRoles)

	buf = []byte(`{"database":"db1"}`)
	err = json.Unmarshal(buf, ss)
	require.NoError(t, err)
	assert.Nil(t, ss.SecondaryRoles)
}
