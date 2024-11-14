package godatabend

import (
	"net/http"
	"net/url"
	"sync"
)

type IgnoreDomainCookieJar struct {
	mu      sync.Mutex
	cookies map[string]*http.Cookie
}

func NewIgnoreDomainCookieJar() *IgnoreDomainCookieJar {
	return &IgnoreDomainCookieJar{
		cookies: make(map[string]*http.Cookie),
	}
}

func (jar *IgnoreDomainCookieJar) SetCookies(_u *url.URL, cookies []*http.Cookie) {
	jar.mu.Lock()
	defer jar.mu.Unlock()
	for _, cookie := range cookies {
		jar.cookies[cookie.Name] = cookie
	}
}

func (jar *IgnoreDomainCookieJar) Cookies(u *url.URL) []*http.Cookie {
	jar.mu.Lock()
	defer jar.mu.Unlock()
	result := make([]*http.Cookie, 0, len(jar.cookies))
	for _, cookie := range jar.cookies {
		result = append(result, cookie)
	}
	return result
}
