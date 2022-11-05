package godatabend

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/avast/retry-go"

	"github.com/databendcloud/bendsql/api/apierrors"
	"github.com/pkg/errors"
)

func (c *APIClient) DoRequest(method, path string, headers http.Header, req interface{}, resp interface{}) error {
	var err error
	reqBody := []byte{}
	if req != nil {
		reqBody, err = json.Marshal(req)
		if err != nil {
			panic(err)
		}
	}

	url := c.makeURL(path)
	httpReq, err := http.NewRequest(method, url, bytes.NewBuffer(reqBody))
	if err != nil {
		return err
	}

	if headers != nil {
		httpReq.Header = headers.Clone()
	}
	httpReq.Header.Set(contentType, jsonContentType)
	httpReq.Header.Set(accept, jsonContentType)
	if len(c.AccessToken) > 0 {
		httpReq.Header.Set(authorization, "Bearer "+c.AccessToken)
	}
	if len(c.Host) > 0 {
		httpReq.Host = c.Host
	}

	httpClient := &http.Client{}
	httpResp, err := httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("failed http do request: %w", err)
	}
	defer httpResp.Body.Close()

	httpRespBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return fmt.Errorf("io read error: %w", err)
	}

	if httpResp.StatusCode == http.StatusUnauthorized {
		return apierrors.New("please check your user/password.", httpResp.StatusCode, httpRespBody)
	} else if httpResp.StatusCode >= 500 {
		return apierrors.New("please retry again later.", httpResp.StatusCode, httpRespBody)
	} else if httpResp.StatusCode >= 400 {
		return apierrors.New("please check your arguments.", httpResp.StatusCode, httpRespBody)
	}

	if resp != nil {
		if err := json.Unmarshal(httpRespBody, &resp); err != nil {
			return err
		}
	}
	return nil
}
func (c *APIClient) makeURL(path string, args ...interface{}) string {
	format := c.ApiEndpoint + path
	return fmt.Sprintf(format, args...)
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

func (c *APIClient) Login() error {
	req := struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}{
		Email:    c.UserEmail,
		Password: c.Password,
	}
	path := "/api/v1/account/sign-in"
	reply := struct {
		Data struct {
			AccessToken  string `json:"accessToken"`
			RefreshToken string `json:"refreshToken"`
		} `json:"data,omitempty"`
	}{}
	err := c.DoRequest("POST", path, nil, &req, &reply)
	var apiErr apierrors.APIError
	if errors.As(err, &apiErr) && apierrors.IsAuthFailed(err) {
		apiErr.Hint = "" // shows the server replied message if auth Err
		return apiErr
	} else if err != nil {
		return err
	}
	c.resetTokens(reply.Data.AccessToken, reply.Data.RefreshToken)
	return nil
}

func (c *APIClient) resetTokens(accessToken string, refreshToken string) {
	c.AccessToken = accessToken
	c.RefreshToken = refreshToken
}

func (c *APIClient) DoQuery(ctx context.Context, query string, args []driver.Value) (*QueryResponse, error) {
	headers := make(http.Header)
	headers.Set("X-DATABENDCLOUD-WAREHOUSE", c.CurrentWarehouse)
	headers.Set("X-DATABENDCLOUD-ORG", c.CurrentOrgSlug)
	q, err := buildQuery(query, args)
	if err != nil {
		return nil, err
	}
	request := QueryRequest{
		SQL: q,
	}
	path := "/v1/query"
	var result QueryResponse
	err = c.DoRequest("POST", path, headers, request, &result)
	if err != nil {
		return nil, err
	}
	if result.Error != nil {
		return nil, fmt.Errorf("query %s in org %s has error: %v", c.CurrentWarehouse, c.CurrentOrgSlug, result.Error)
	}
	return &result, nil
}

func buildQuery(query string, params []driver.Value) (string, error) {
	fmt.Printf("the query is %s,the args is %v", query, params)
	if len(params) > 0 && params[0] != nil {
		result, err := interpolateParams(query, params)
		if err != nil {
			return result, fmt.Errorf("buildRequest: failed to interpolate params: %w", err)
		}
		return result, nil
	}
	return query, nil
}

func (c *APIClient) QuerySync(ctx context.Context, query string, args []driver.Value, respCh chan QueryResponse) error {
	fmt.Printf("query sync %s", query)
	var r0 *QueryResponse
	err := retry.Do(
		func() error {
			r, err := c.DoQuery(ctx, query, args)
			if err != nil {
				return fmt.Errorf("query failed: %w", err)
			}
			r0 = r
			return nil
		},
		// other err no need to retry
		retry.RetryIf(func(err error) bool {
			if err != nil && !(apierrors.IsProxyErr(err) || strings.Contains(err.Error(), apierrors.ProvisionWarehouseTimeout)) {
				return false
			}
			return true
		}),
		retry.Delay(2*time.Second),
		retry.Attempts(10),
	)
	if err != nil {
		return fmt.Errorf("query failed after 10 retries: %w", err)
	}
	if r0.Error != nil {
		return fmt.Errorf("query has error: %+v", r0.Error)
	}
	if err != nil {
		return err
	}
	respCh <- *r0
	nextUri := r0.NextURI
	for len(nextUri) != 0 {
		p, err := c.QueryPage(r0.Id, nextUri)
		if err != nil {
			return err
		}
		if p.Error != nil {
			return fmt.Errorf("query has error: %+v", p.Error)
		}
		nextUri = p.NextURI
		respCh <- *p
	}
	return nil
}

func (c *APIClient) QueryPage(queryId, path string) (*QueryResponse, error) {
	headers := make(http.Header)
	headers.Set("queryID", queryId)
	headers.Set("X-DATABENDCLOUD-WAREHOUSE", c.CurrentWarehouse)
	headers.Set("X-DATABENDCLOUD-ORG", string(c.CurrentOrgSlug))
	var result QueryResponse
	err := retry.Do(
		func() error {
			err := c.DoRequest("GET", path, headers, nil, &result)
			if err != nil {
				return fmt.Errorf("query failed: %w", err)
			}
			return nil
		},
		retry.Delay(2*time.Second),
		retry.Attempts(5),
	)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *APIClient) RefreshTokens() error {
	req := struct {
		RefreshToken string `json:"refreshToken"`
	}{
		RefreshToken: c.RefreshToken,
	}
	resp := struct {
		Data struct {
			AccessToken  string `json:"accessToken"`
			RefreshToken string `json:"refreshToken"`
		} `json:"data"`
	}{}
	path := "/api/v1/account/renew-token"
	err := c.DoRequest("POST", path, nil, &req, &resp)
	if err != nil {
		return err
	}
	c.resetTokens(resp.Data.AccessToken, resp.Data.RefreshToken)
	return nil
}

func (c *APIClient) UploadToStageByPresignURL(presignURL, fileName string, header map[string]interface{}) error {
	fileContent, err := os.ReadFile(fileName)
	if err != nil {
		return err
	}
	file, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer file.Close()
	body := bytes.NewBuffer(fileContent)

	httpReq, err := http.NewRequest("PUT", presignURL, body)
	if err != nil {
		return err
	}
	for k, v := range header {
		httpReq.Header.Set(k, fmt.Sprintf("%v", v))
	}
	httpReq.Header.Set("Content-Length", strconv.FormatInt(int64(len(body.Bytes())), 10))
	httpClient := &http.Client{
		Timeout: time.Second * 60,
	}
	httpResp, err := httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("failed http do request: %w", err)
	}
	defer httpResp.Body.Close()
	httpRespBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return err
	}
	if httpResp.StatusCode >= 400 {
		return fmt.Errorf("request got bad status: %d req=%s resp=%s", httpResp.StatusCode, body, httpRespBody)
	}
	return nil
}

func (c *APIClient) uploadToStage(fileName string) error {
	rootStage := "sjh"
	fmt.Printf("uploading %s to stage %s... \n", fileName, rootStage)
	presignUploadSQL := fmt.Sprintf("PRESIGN UPLOAD @%s/%s", rootStage, filepath.Base(fileName))
	resp, err := c.DoQuery(context.Background(), presignUploadSQL, nil)
	if err != nil {
		return err
	}
	if len(resp.Data) < 1 || len(resp.Data[0]) < 2 {
		return fmt.Errorf("generate presign url failed")
	}
	headers, ok := resp.Data[0][1].(map[string]interface{})
	if !ok {
		return fmt.Errorf("no host for presign url")
	}
	return c.UploadToStageByPresignURL(fmt.Sprintf("%v", resp.Data[0][2]), fileName, headers)
}
