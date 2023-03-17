package godatabend

import (
	"context"
	"io/ioutil"

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

// try decode as toml, if not toml, return the plain key content
func (l *FileAccessTokenLoader) LoadAccessToken(ctx context.Context, forceRotate bool) (string, error) {
	buf, err := ioutil.ReadFile(l.path)
	if err != nil {
		return "", err
	}
	content := string(buf)

	data := &FileAccessTokenData{}
	if _, err = toml.Decode(content, &data); err == nil {
		return data.AccessToken, nil
	}

	return content, nil
}
