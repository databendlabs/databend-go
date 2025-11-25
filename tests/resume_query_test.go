package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	dc "github.com/datafuselabs/databend-go"
)

func (s *DatabendTestSuite) TestResumeQueryWithSessionState() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	firstClient := dc.NewAPIClientFromConfig(s.cfg)
	firstClient.MaxRowsPerPage = 1

	const settingKey = "max_result_rows"
	const settingValue = 3
	_, err := firstClient.QuerySync(ctx, fmt.Sprintf("SET %s = %d", settingKey, settingValue), nil)
	s.Require().NoError(err)

	startResp, err := firstClient.StartQuery(ctx, "SELECT number FROM numbers(5)", nil)
	s.Require().NoError(err)
	s.Require().NotNil(startResp)
	s.Require().NotEmpty(startResp.NextURI)
	s.False(startResp.ReadFinished())

	clientState := firstClient.GetState()
	s.Require().NotNil(clientState)
	s.Require().NotEmpty(clientState.SessionID)
	s.Require().NotEmpty(clientState.SessionState)

	secondClient := dc.NewAPIClientFromConfig(s.cfg).WithState(clientState)
	secondClient.MaxRowsPerPage = 1

	resumeResp, err := secondClient.PollQuery(ctx, startResp.NextURI)
	s.Require().NoError(err)
	s.Require().NotNil(resumeResp)
	s.Equal(startResp.ID, resumeResp.ID)

	finalResp, err := secondClient.PollUntilQueryEnd(ctx, resumeResp)
	s.Require().NoError(err)
	s.Require().NotNil(finalResp)
	s.Greater(len(finalResp.Data), 0)
	s.NoError(secondClient.CloseQuery(ctx, finalResp))
}

func (s *DatabendTestSuite) TestSessionSettingLoadWithState() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := dc.NewAPIClientFromConfig(s.cfg)

	const settingKey = "max_result_rows"
	const settingValue = 5

	_, err := client.QuerySync(ctx, fmt.Sprintf("SET %s = %d", settingKey, settingValue), nil)
	s.Require().NoError(err)

	state := client.GetState()
	s.Require().NotNil(state)

	client2 := dc.NewAPIClientFromConfig(s.cfg).WithState(state)
	resp, err := client2.QuerySync(ctx, fmt.Sprintf("SELECT value FROM system.settings WHERE name = '%s'", settingKey), nil)
	s.Require().NoError(err)
	s.Require().Greater(len(resp.Data), 0)
	s.Require().Greater(len(resp.Data[0]), 0)
	s.Require().NotNil(resp.Data[0][0])
	s.Equal(fmt.Sprintf("%d", settingValue), *resp.Data[0][0])

	roundedState := client2.GetState()
	s.Require().NotNil(roundedState)
	s.Require().NotEmpty(roundedState.SessionState)

	var sessionState dc.SessionState
	err = json.Unmarshal([]byte(roundedState.SessionState), &sessionState)
	s.Require().NoError(err)
	s.Equal(fmt.Sprintf("%d", settingValue), sessionState.Settings[settingKey])
}

func (s *DatabendTestSuite) TestResumeQueryWithoutStateFails() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := dc.NewAPIClientFromConfig(s.cfg)
	client.MaxRowsPerPage = 1

	startResp, err := client.StartQuery(ctx, "SELECT number FROM numbers(5)", nil)
	s.Require().NoError(err)
	s.Require().NotNil(startResp)

	client2 := dc.NewAPIClientFromConfig(s.cfg)
	client2.MaxRowsPerPage = 1

	_, err = client2.PollQuery(ctx, startResp.NextURI)
	s.Require().Error(err)
}
