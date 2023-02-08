package godatabend

import "context"

type TokenLoader interface {
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
