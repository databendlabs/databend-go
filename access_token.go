package godatabend

import "context"

// AccessToken is used on Bearer authentication. The token may have a limited
// lifetime, you can rotate your token by this interface.
type AccessTokenLoader interface {
	LoadAccessToken(ctx context.Context) (string, error)
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
