package godatabend

import (
	"bufio"
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"fmt"
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

type FileFormatOptions struct {
	Type            string
	FieldDelimiter  string
	RecordDelimiter string
	SkipHeader      int
}

func NewDefaultCSVFormatOptions() *FileFormatOptions {
	return &FileFormatOptions{
		Type:            "CSV",
		FieldDelimiter:  ",",
		RecordDelimiter: "\n",
		SkipHeader:      0,
	}
}

func (o *FileFormatOptions) ToMap() map[string]string {
	opts := map[string]string{}
	if o.Type != "" {
		opts["type"] = o.Type
	}
	if o.FieldDelimiter != "" {
		opts["field_delimiter"] = o.FieldDelimiter
	}
	if o.RecordDelimiter != "" {
		opts["record_delimiter"] = o.RecordDelimiter
	}
	if o.SkipHeader > 0 {
		opts["skip_header"] = fmt.Sprintf("%d", o.SkipHeader)
	}
	return opts
}

type CopyOptions struct {
	Purge bool
}

func NewDefaultCopyOptions() *CopyOptions {
	return &CopyOptions{
		Purge: true,
	}
}

func (o *CopyOptions) ToMap() map[string]string {
	opts := map[string]string{}
	if o.Purge {
		opts["purge"] = "true"
	}
	return opts
}

type APIClient struct {
	cli *http.Client

	apiEndpoint       string
	host              string
	tenant            string
	warehouse         string
	database          string
	user              string
	password          string
	accessTokenLoader AccessTokenLoader

	waitTimeSeconds      int64
	maxRowsInBuffer      int64
	maxRowsPerPage       int64
	presignedURLDisabled bool
}

func NewAPIClientFromConfig(cfg *Config) *APIClient {
	var apiScheme string
	switch cfg.SSLMode {
	case SSL_MODE_DISABLE:
		apiScheme = "http"
	default:
		apiScheme = "https"
	}
	return &APIClient{
		cli: &http.Client{
			Timeout: cfg.Timeout,
		},
		apiEndpoint:       fmt.Sprintf("%s://%s", apiScheme, cfg.Host),
		host:              cfg.Host,
		tenant:            cfg.Tenant,
		warehouse:         cfg.Warehouse,
		database:          cfg.Database,
		user:              cfg.User,
		password:          cfg.Password,
		accessTokenLoader: initAccessTokenLoader(cfg),

		waitTimeSeconds:      cfg.WaitTimeSecs,
		maxRowsInBuffer:      cfg.MaxRowsInBuffer,
		maxRowsPerPage:       cfg.MaxRowsPerPage,
		presignedURLDisabled: cfg.PresignedURLDisabled,
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

func (c *APIClient) doRequest(method, path string, req interface{}, resp interface{}) error {
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
			return errors.Wrap(err, "failed to do http request")
		}
		defer httpResp.Body.Close()

		httpRespBody, err := io.ReadAll(httpResp.Body)
		if err != nil {
			return errors.Wrap(err, "failed to read http response body")
		}

		if httpResp.StatusCode == http.StatusUnauthorized {
			if c.authMethod() == AuthMethodAccessToken && i < maxRetries {
				// retry with a rotated access token
				c.accessTokenLoader.LoadAccessToken(context.Background(), true)
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
				return err
			}
		}
		return nil
	}
	return errors.Errorf("failed to do request after %d retries", maxRetries)
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
	if c.tenant != "" {
		headers.Set(DatabendTenantHeader, c.tenant)
	}
	if c.warehouse != "" {
		headers.Set(DatabendWarehouseHeader, c.warehouse)
	}

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
	if c.maxRowsPerPage == 0 && c.maxRowsInBuffer == 0 && c.waitTimeSeconds == 0 {
		return nil
	}
	return &PaginationConfig{
		MaxRowsPerPage:  c.maxRowsPerPage,
		MaxRowsInBuffer: c.maxRowsInBuffer,
		WaitTime:        c.waitTimeSeconds,
	}
}

func (c *APIClient) getSessionConfig() *SessionConfig {
	if c.database == "" {
		return nil
	}
	return &SessionConfig{
		Database: c.database,
	}
}

func (c *APIClient) DoQuery(query string, args []driver.Value) (*QueryResponse, error) {
	q, err := buildQuery(query, args)
	if err != nil {
		return nil, err
	}
	request := QueryRequest{
		SQL:        q,
		Pagination: c.getPagenationConfig(),
		Session:    c.getSessionConfig(),
	}

	path := "/v1/query"
	var result QueryResponse
	err = c.doRequest("POST", path, request, &result)
	if err != nil {
		return nil, errors.Wrap(err, "failed to do query request")
	}
	return &result, nil
}

func (c *APIClient) waitForQuery(result *QueryResponse) (*QueryResponse, error) {
	if result.Error != nil {
		return nil, errors.Wrap(result.Error, "query failed")
	}
	for result.NextURI != "" {
		ret, err := c.QueryPage(result.NextURI)
		if err != nil {
			return nil, errors.Wrap(err, "failed to query page")
		}
		if ret.Error != nil {
			return nil, errors.Wrap(ret.Error, "query page failed")
		}
		result.Data = append(result.Data, ret.Data...)
		result.NextURI = ret.NextURI
	}
	return result, nil
}

func (c *APIClient) QuerySingle(query string, args []driver.Value) (*QueryResponse, error) {
	result, err := c.DoQuery(query, args)
	if err != nil {
		return nil, err
	}
	return c.waitForQuery(result)
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

func (c *APIClient) QuerySync(query string, args []driver.Value, respCh chan QueryResponse) error {
	// fmt.Printf("query sync %s", query)
	var r0 *QueryResponse
	err := retry.Do(
		func() error {
			r, err := c.DoQuery(query, args)
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
		retry.Attempts(10),
	)
	if err != nil {
		return errors.Wrap(err, "query sync failed")
	}
	if r0.Error != nil {
		return errors.Wrap(r0.Error, "query has error")
	}
	if err != nil {
		return err
	}
	respCh <- *r0
	nextUri := r0.NextURI
	for len(nextUri) != 0 {
		p, err := c.QueryPage(nextUri)
		if err != nil {
			return err
		}
		if p.Error != nil {
			return errors.Wrap(p.Error, "query page has error")
		}
		nextUri = p.NextURI
		respCh <- *p
	}
	return nil
}

func (c *APIClient) QueryPage(nextURI string) (*QueryResponse, error) {
	var result QueryResponse
	err := c.doRequest("GET", nextURI, nil, &result)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query page")
	}
	return &result, nil
}

func (c *APIClient) InsertWithStage(sql string, stage *StageLocation, fileFormatOptions *FileFormatOptions, copyOptions *CopyOptions) (*QueryResponse, error) {
	if stage == nil {
		return nil, errors.New("stage location required for insert with stage")
	}
	if fileFormatOptions == nil {
		fileFormatOptions = NewDefaultCSVFormatOptions()
	}
	if copyOptions == nil {
		copyOptions = NewDefaultCopyOptions()
	}
	request := QueryRequest{
		SQL:        sql,
		Pagination: c.getPagenationConfig(),
		Session:    c.getSessionConfig(),
		StageAttachment: &StageAttachmentConfig{
			Location:          stage.String(),
			FileFormatOptions: fileFormatOptions.ToMap(),
			CopyOptions:       copyOptions.ToMap(),
		},
	}

	path := "/v1/query"
	var result QueryResponse
	err := c.doRequest("POST", path, request, &result)
	if err != nil {
		return nil, errors.Wrap(err, "failed to insert with stage")
	}
	return c.waitForQuery(&result)
}

func (c *APIClient) UploadToStage(stage *StageLocation, input *bufio.Reader, size int64) error {
	if c.presignedURLDisabled {
		return c.uploadToStageByAPI(stage, input, size)
	} else {
		return c.uploadToStageByPresignURL(stage, input, size)
	}
}

func (c *APIClient) getPresignedURL(stage *StageLocation) (*PresignedResponse, error) {
	var headers string
	presignUploadSQL := fmt.Sprintf("PRESIGN UPLOAD %s", stage)
	resp, err := c.QuerySingle(presignUploadSQL, nil)
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

func (c *APIClient) uploadToStageByPresignURL(stage *StageLocation, input *bufio.Reader, size int64) error {
	presigned, err := c.getPresignedURL(stage)
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
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		return errors.Errorf("failed to upload to stage by presigned url, status code: %d, body: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

func (c *APIClient) uploadToStageByAPI(stage *StageLocation, input *bufio.Reader, size int64) error {
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
	defer resp.Body.Close()

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
