package godatabend

import (
	"context"

	"github.com/BurntSushi/toml"
)

// AccessTokenLoader is used on Bearer authentication. The token may have a limited
// lifetime, you can rotate your token by this interface.
type AccessTokenLoader interface {
	// LoadAccessToken is called whenever a new request is made to the server.
	LoadAccessToken(ctx context.Context, forceRotate bool) (string, error)
}

type StaticAccessTokenLoader struct {
	AccessToken string
}

func NewStaticAccessTokenLoader(accessToken string) *StaticAccessTokenLoader {
	return &StaticAccessTokenLoader{
		AccessToken: accessToken,
	}
}

func (l *StaticAccessTokenLoader) LoadAccessToken(ctx context.Context, forceRotate bool) (string, error) {
	return l.AccessToken, nil
}

type FileAccessTokenLoader struct {
	path string
}

type FileAccessTokenData struct {
	AccessToken string `toml:"access_token"`
}

func NewFileAccessTokenLoader(path string) *FileAccessTokenLoader {
	return &FileAccessTokenLoader{
		path: path,
	}
}

func (l *FileAccessTokenLoader) LoadAccessToken(ctx context.Context, forceRotate bool) (string, error) {
	data := &FileAccessTokenData{}
	_, err := toml.DecodeFile(l.path, &data)
	if err != nil {
		return "", err
	}
	return data.AccessToken, nil
}
