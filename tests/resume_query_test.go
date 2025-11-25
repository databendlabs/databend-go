package tests

import (
	"context"
	"time"

	dc "github.com/datafuselabs/databend-go"
)

func (s *DatabendTestSuite) TestResumeQueryWithSessionState() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	firstClient := dc.NewAPIClientFromConfig(s.cfg)
	firstClient.MaxRowsPerPage = 1

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
