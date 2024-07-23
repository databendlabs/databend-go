package godatabend

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVersion(t *testing.T) {
	r := require.New(t)
	content, err := os.ReadFile("VERSION")
	r.NoError(err)

	r.Equal(string(content), version)
}
