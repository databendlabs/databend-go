package godatabend

import (
	"context"
	"fmt"
	"os"
)

// AccessToken is used on Bearer authentication. The token may have a limited
// lifetime, you can rotate your token by this interface.
type AccessTokenLoader interface {
	// LoadAccessToken is called whenever a new request is made to the server.
	LoadAccessToken(ctx context.Context, forceRotate bool) (string, error)
}

type StaticTokenLoader struct {
	AccessToken string
}

func NewStaticTokenLoader(accessToken string) *StaticTokenLoader {
	return &StaticTokenLoader{
		AccessToken: accessToken,
	}
}

func (l *StaticTokenLoader) LoadAccessToken(ctx context.Context) (string, error) {
	return l.AccessToken, nil
}

type AccessTokenFileLoader struct {
	path string
}

func NewAccessTokenFileLoader(path string) *AccessTokenFileLoader {
	return &AccessTokenFileLoader{
		path: path,
	}
}

func (l *AccessTokenFileLoader) LoadAccessToken(ctx context.Context, forceRotate bool) (string, error) {
	buf, err := os.ReadFile(l.path)
	if err != nil {
		return "", fmt.Errorf("failed to read access token file: %w", err)
	}
	return string(buf), nil
}
