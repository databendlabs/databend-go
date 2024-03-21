package godatabend

import (
	"bufio"
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/avast/retry-go"
	"github.com/pkg/errors"
)

type AuthMethod string

const (
	AuthMethodUserPassword AuthMethod = "userPassword"
	AuthMethodAccessToken  AuthMethod = "accessToken"
)

type ContextKey string

const (
	EMPTY_FIELD_AS string = "empty_field_as"
	PURGE          string = "purge"
)

type PresignedResponse struct {
	Method  string
	Headers map[string]string
	URL     string
}

type StageLocation struct {
	Name string
	Path string
}

func (sl *StageLocation) String() string {
	return fmt.Sprintf("@%s/%s", sl.Name, sl.Path)
}

func (c *APIClient) NewDefaultCSVFormatOptions() map[string]string {
	return map[string]string{
		"type":             "CSV",
		"field_delimiter":  ",",
		"record_delimiter": "\n",
		"skip_header":      "0",
		EMPTY_FIELD_AS:     c.EmptyFieldAs,
	}
}

func (c *APIClient) NewDefaultCopyOptions() map[string]string {
	return map[string]string{
		PURGE: "true",
	}
}

type APIClient struct {
	SessionID string
	QuerySeq  int64
	cli       *http.Client
	rows      *nextRows

	apiEndpoint  string
	host         string
	tenant       string
	warehouse    string
	database     string
	user         string
	password     string
	sessionState *SessionState

	statsTracker      QueryStatsTracker
	accessTokenLoader AccessTokenLoader

	WaitTimeSeconds      int64
	MaxRowsInBuffer      int64
	MaxRowsPerPage       int64
	PresignedURLDisabled bool
	EmptyFieldAs         string

	// only used for testing mocks
	doRequestFunc func(method, path string, req interface{}, resp interface{}) error
}

func (c *APIClient) NextQuery() {
	if c.rows != nil {
		_ = c.rows.Close()
	}
	c.QuerySeq += 1
}

func (c *APIClient) GetQueryID() string {
	return fmt.Sprintf("%s.%d", c.SessionID, c.QuerySeq)
}

func NewAPIClientFromConfig(cfg *Config) *APIClient {
	var apiScheme string
	switch cfg.SSLMode {
	case SSL_MODE_DISABLE:
		apiScheme = "http"
	default:
		apiScheme = "https"
	}

	// if role is set in config, we'd prefer to limit it as the only effective role,
	// so you could limit the privileges by setting a role with limited privileges.
	// however this can be overridden by executing `SET SECONDARY ROLES ALL` in the
	// query.
	// secondaryRoles now have two viable values:
	// - nil: means enabling ALL the granted roles of the user
	// - []string{}: means enabling NONE of the granted roles
	var secondaryRoles *[]string
	if len(cfg.Role) > 0 {
		secondaryRoles = &[]string{}
	}

	return &APIClient{
		SessionID: uuid.NewString(),
		cli: &http.Client{
			Timeout: cfg.Timeout,
		},
		apiEndpoint: fmt.Sprintf("%s://%s", apiScheme, cfg.Host),
		host:        cfg.Host,
		tenant:      cfg.Tenant,
		warehouse:   cfg.Warehouse,
		user:        cfg.User,
		password:    cfg.Password,
		sessionState: &SessionState{
			Database:       cfg.Database,
			Role:           cfg.Role,
			SecondaryRoles: secondaryRoles,
			Settings:       cfg.Params,
		},

		accessTokenLoader: initAccessTokenLoader(cfg),
		statsTracker:      cfg.StatsTracker,

		WaitTimeSeconds:      cfg.WaitTimeSecs,
		MaxRowsInBuffer:      cfg.MaxRowsInBuffer,
		MaxRowsPerPage:       cfg.MaxRowsPerPage,
		PresignedURLDisabled: cfg.PresignedURLDisabled,
		EmptyFieldAs:         cfg.EmptyFieldAs,
	}
}

func initAccessTokenLoader(cfg *Config) AccessTokenLoader {
	if cfg.AccessTokenLoader != nil {
		return cfg.AccessTokenLoader
	} else if cfg.AccessTokenFile != "" {
		return NewFileAccessTokenLoader(cfg.AccessTokenFile)
	} else if cfg.AccessToken != "" {
		return NewStaticAccessTokenLoader(cfg.AccessToken)
	}
	return nil
}

func (c *APIClient) doRequest(ctx context.Context, method, path string, req interface{}, resp interface{}) error {
	if c.doRequestFunc != nil {
		return c.doRequestFunc(method, path, req, resp)
	}

	var err error
	reqBody := []byte{}
	if req != nil {
		reqBody, err = json.Marshal(req)
		if err != nil {
			return errors.Wrap(err, "failed to marshal request body")
		}
	}

	url := c.makeURL(path)
	httpReq, err := http.NewRequest(method, url, bytes.NewBuffer(reqBody))
	if err != nil {
		return errors.Wrap(err, "failed to create http request")
	}
	httpReq = httpReq.WithContext(ctx)

	maxRetries := 2
	for i := 1; i <= maxRetries; i++ {
		headers, err := c.makeHeaders()
		if err != nil {
			return errors.Wrap(err, "failed to make request headers")
		}
		headers.Set(contentType, jsonContentType)
		headers.Set(accept, jsonContentType)
		httpReq.Header = headers

		if len(c.host) > 0 {
			httpReq.Host = c.host
		}

		httpResp, err := c.cli.Do(httpReq)
		if err != nil {
			return errors.Wrap(ErrDoRequest, err.Error())
		}
		defer func() {
			_ = httpResp.Body.Close()
		}()

		httpRespBody, err := io.ReadAll(httpResp.Body)
		if err != nil {
			return errors.Wrap(ErrReadResponse, err.Error())
		}

		if httpResp.StatusCode == http.StatusUnauthorized {
			if c.authMethod() == AuthMethodAccessToken && i < maxRetries {
				// retry with a rotated access token
				_, _ = c.accessTokenLoader.LoadAccessToken(context.Background(), true)
				continue
			}
			return NewAPIError("authorization failed", httpResp.StatusCode, httpRespBody)
		} else if httpResp.StatusCode >= 500 {
			return NewAPIError("please retry again later.", httpResp.StatusCode, httpRespBody)
		} else if httpResp.StatusCode >= 400 {
			return NewAPIError("please check your arguments.", httpResp.StatusCode, httpRespBody)
		}

		if resp != nil {
			if err := json.Unmarshal(httpRespBody, &resp); err != nil {
				return errors.Wrap(err, "failed to unmarshal response body")
			}
		}
		return nil
	}
	return errors.Errorf("failed to do request after %d retries", maxRetries)
}

func (c *APIClient) trackStats(resp *QueryResponse) {
	if c.statsTracker == nil || resp == nil {
		return
	}
	c.statsTracker(resp.ID, &resp.Stats)
}

func (c *APIClient) makeURL(path string, args ...interface{}) string {
	format := c.apiEndpoint + path
	return fmt.Sprintf(format, args...)
}

func (c *APIClient) authMethod() AuthMethod {
	if c.user != "" {
		return AuthMethodUserPassword
	}
	if c.accessTokenLoader != nil {
		return AuthMethodAccessToken
	}
	return ""
}

func (c *APIClient) makeHeaders() (http.Header, error) {
	headers := http.Header{}
	headers.Set(WarehouseRoute, "warehouse")
	headers.Set(UserAgent, fmt.Sprintf("databend-go/%s", version))
	if c.tenant != "" {
		headers.Set(DatabendTenantHeader, c.tenant)
	}
	if c.warehouse != "" {
		headers.Set(DatabendWarehouseHeader, c.warehouse)
	}

	headers.Set(DatabendQueryIDHeader, c.GetQueryID())

	switch c.authMethod() {
	case AuthMethodUserPassword:
		headers.Set(Authorization, fmt.Sprintf("Basic %s", encode(c.user, c.password)))
	case AuthMethodAccessToken:
		accessToken, err := c.accessTokenLoader.LoadAccessToken(context.TODO(), false)
		if err != nil {
			return nil, errors.Wrap(err, "failed to load access token")
		}
		headers.Set(Authorization, fmt.Sprintf("Bearer %s", accessToken))
	default:
		return nil, errors.New("no user password or access token")
	}

	return headers, nil
}

func encode(name string, key string) string {
	return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", name, key)))
}

// databendInsecureTransport is the transport object that doesn't do certificate revocation check.
var databendInsecureTransport = &http.Transport{
	MaxIdleConns:    10,
	IdleConnTimeout: 30 * time.Minute,
	Proxy:           http.ProxyFromEnvironment,
	DialContext: (&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}).DialContext,
}

func (c *APIClient) getPagenationConfig() *PaginationConfig {
	if c.MaxRowsPerPage == 0 && c.MaxRowsInBuffer == 0 && c.WaitTimeSeconds == 0 {
		return nil
	}
	return &PaginationConfig{
		MaxRowsPerPage:  c.MaxRowsPerPage,
		MaxRowsInBuffer: c.MaxRowsInBuffer,
		WaitTime:        c.WaitTimeSeconds,
	}
}

func (c *APIClient) getSessionState() *SessionState {
	return c.sessionState
}

func (c *APIClient) DoQuery(ctx context.Context, query string, args []driver.Value) (*QueryResponse, error) {
	q, err := buildQuery(query, args)
	if err != nil {
		return nil, err
	}
	request := QueryRequest{
		SQL:        q,
		Pagination: c.getPagenationConfig(),
		Session:    c.getSessionState(),
	}

	path := "/v1/query"
	var resp QueryResponse
	err = c.doRequest(ctx, "POST", path, request, &resp)
	if err != nil {
		return nil, errors.Wrap(err, "failed to do query request")
	}
	// try update session as long as resp is not nil, even if query failed (resp.Error != nil)
	// e.g. transaction state need to be updated if commit fail
	c.applySessionState(&resp)
	c.trackStats(&resp)
	return &resp, nil
}

func (c *APIClient) applySessionState(response *QueryResponse) {
	if response == nil || response.Session == nil {
		return
	}
	c.sessionState = response.Session
}

func (c *APIClient) WaitForQuery(ctx context.Context, result *QueryResponse) (*QueryResponse, error) {
	if result.Error != nil {
		return nil, errors.Wrap(result.Error, "query failed")
	}
	var err error
	for result.NextURI != "" {
		schema := result.Schema
		data := result.Data
		result, err = c.QueryPage(ctx, result.NextURI)
		if err != nil {
			return nil, errors.Wrap(err, "failed to query page")
		}
		if result.Error != nil {
			return nil, errors.Wrap(result.Error, "query page failed")
		}
		if len(result.Schema) == 0 {
			result.Schema = schema
		}
		result.Data = append(data, result.Data...)
	}
	c.trackStats(result)
	return result, nil
}

func (c *APIClient) QuerySingle(ctx context.Context, query string, args []driver.Value) (*QueryResponse, error) {
	c.NextQuery()
	result, err := c.DoQuery(ctx, query, args)
	if err != nil {
		return nil, err
	}
	c.trackStats(result)
	return c.WaitForQuery(ctx, result)
}

func buildQuery(query string, params []driver.Value) (string, error) {
	if len(params) > 0 && params[0] != nil {
		result, err := interpolateParams(query, params)
		if err != nil {
			return result, errors.Wrap(err, "buildRequest: failed to interpolate params")
		}
		return result, nil
	}
	return query, nil
}

func (c *APIClient) QuerySync(ctx context.Context, query string, args []driver.Value, respCh chan QueryResponse) error {
	// fmt.Printf("query sync %s", query)
	var r0 *QueryResponse
	err := retry.Do(
		func() error {
			r, err := c.DoQuery(ctx, query, args)
			if err != nil {
				return err
			}
			r0 = r
			return nil
		},
		// other err no need to retry
		retry.RetryIf(func(err error) bool {
			if err != nil && (IsProxyErr(err) || strings.Contains(err.Error(), ProvisionWarehouseTimeout)) {
				return true
			}
			return false
		}),
		retry.Delay(2*time.Second),
		retry.Attempts(5),
		retry.DelayType(retry.FixedDelay),
	)
	if err != nil {
		return errors.Wrap(err, "query sync failed")
	}
	respCh <- *r0
	nextUri := r0.NextURI
	for len(nextUri) != 0 {
		p, err := c.QueryPage(ctx, nextUri)
		if err != nil {
			return err
		}
		if p.Error != nil {
			return errors.Wrap(p.Error, "query page has error")
		}
		nextUri = p.NextURI
		respCh <- *p
	}
	if r0.Error != nil {
		return errors.Wrap(r0.Error, "query has error")
	}
	return nil
}

func (c *APIClient) QueryPage(ctx context.Context, nextURI string) (*QueryResponse, error) {
	var result QueryResponse
	err := retry.Do(
		func() error {
			return c.doRequest(ctx, "GET", nextURI, nil, &result)
		},
		retry.RetryIf(func(err error) bool {
			if err == nil {
				return false
			}
			if errors.Is(err, ErrDoRequest) || errors.Is(err, ErrReadResponse) || IsProxyErr(err) {
				return true
			}
			return false
		}),
		retry.Delay(1*time.Second),
		retry.Attempts(3),
		retry.DelayType(retry.FixedDelay),
	)
	// try update session as long as resp is not nil, even if query failed (resp.Error != nil)
	// e.g. transaction state need to be updated if commit fail
	c.applySessionState(&result)
	c.trackStats(&result)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query page")
	}
	return &result, nil
}

func (c *APIClient) KillQuery(ctx context.Context, killURI string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return c.doRequest(ctx, "POST", killURI, nil, nil)
}

func (c *APIClient) InsertWithStage(ctx context.Context, sql string, stage *StageLocation, fileFormatOptions, copyOptions map[string]string) (*QueryResponse, error) {
	c.NextQuery()
	if stage == nil {
		return nil, errors.New("stage location required for insert with stage")
	}
	if fileFormatOptions == nil {
		fileFormatOptions = c.NewDefaultCSVFormatOptions()
	}
	if copyOptions == nil {
		copyOptions = c.NewDefaultCopyOptions()
	}
	request := QueryRequest{
		SQL:        sql,
		Pagination: c.getPagenationConfig(),
		Session:    c.getSessionState(),
		StageAttachment: &StageAttachmentConfig{
			Location:          stage.String(),
			FileFormatOptions: fileFormatOptions,
			CopyOptions:       copyOptions,
		},
	}

	path := "/v1/query"
	var result QueryResponse
	err := c.doRequest(ctx, "POST", path, request, &result)
	if err != nil {
		return nil, errors.Wrap(err, "failed to insert with stage")
	}
	c.trackStats(&result)
	return c.WaitForQuery(ctx, &result)
}

func (c *APIClient) UploadToStage(ctx context.Context, stage *StageLocation, input *bufio.Reader, size int64) error {
	if c.PresignedURLDisabled {
		return c.UploadToStageByAPI(stage, input)
	} else {
		return c.UploadToStageByPresignURL(ctx, stage, input, size)
	}
}

func (c *APIClient) GetPresignedURL(ctx context.Context, stage *StageLocation) (*PresignedResponse, error) {
	var headers string
	presignUploadSQL := fmt.Sprintf("PRESIGN UPLOAD %s", stage)
	resp, err := c.QuerySingle(ctx, presignUploadSQL, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query presign url")
	}
	if len(resp.Data) < 1 || len(resp.Data[0]) < 2 {
		return nil, errors.Errorf("generate presign url invalid response: %+v", resp.Data)
	}

	result := &PresignedResponse{
		Method:  resp.Data[0][0],
		Headers: make(map[string]string),
		URL:     resp.Data[0][2],
	}
	headers = resp.Data[0][1]
	err = json.Unmarshal([]byte(headers), &result.Headers)
	if err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal headers")
	}
	return result, nil
}

func (c *APIClient) UploadToStageByPresignURL(ctx context.Context, stage *StageLocation, input *bufio.Reader, size int64) error {
	presigned, err := c.GetPresignedURL(ctx, stage)
	if err != nil {
		return errors.Wrap(err, "failed to get presigned url")
	}

	req, err := http.NewRequest("PUT", presigned.URL, input)
	if err != nil {
		return err
	}
	for k, v := range presigned.Headers {
		req.Header.Set(k, v)
	}
	req.ContentLength = size
	// TODO: configurable timeout
	httpClient := &http.Client{
		Timeout: time.Second * 60,
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "failed to upload to stage by presigned url")
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		return errors.Errorf("failed to upload to stage by presigned url, status code: %d, body: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

func (c *APIClient) UploadToStageByAPI(stage *StageLocation, input *bufio.Reader) error {
	body := new(bytes.Buffer)
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("upload", stage.Path)
	if err != nil {
		return errors.Wrap(err, "failed to create multipart writer form file")
	}
	// TODO: do async upload
	_, err = io.Copy(part, input)
	if err != nil {
		return errors.Wrap(err, "failed to copy file to multipart writer form file")
	}
	err = writer.Close()
	if err != nil {
		return errors.Wrap(err, "failed to close multipart writer")
	}

	path := "/v1/upload_to_stage"
	url := c.makeURL(path)
	req, err := http.NewRequest("PUT", url, body)
	if err != nil {
		return errors.Wrap(err, "failed to create http request")
	}

	req.Header, err = c.makeHeaders()
	if err != nil {
		return errors.Wrap(err, "failed to make headers")
	}
	if len(c.host) > 0 {
		req.Host = c.host
	}
	req.Header.Set("stage_name", stage.Name)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	// TODO: configurable timeout
	httpClient := &http.Client{
		Timeout: time.Second * 60,
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "failed http do request")
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrap(err, "failed to read http response body")
	}

	if resp.StatusCode == http.StatusUnauthorized {
		return NewAPIError("please check your user/password.", resp.StatusCode, respBody)
	} else if resp.StatusCode >= 500 {
		return NewAPIError("please retry again later.", resp.StatusCode, respBody)
	} else if resp.StatusCode >= 400 {
		return NewAPIError("please check your arguments.", resp.StatusCode, respBody)
	}

	return nil
}
