package integration_test

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/buildkite/agent-stack-k8s/v2/internal/integration/api"
	"github.com/buildkite/roko"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCleanupOrphanedPipelines(t *testing.T) {
	if !cleanupPipelines {
		t.Skip("not cleaning orphaned pipelines")
	}

	ctx := context.Background()
	graphqlClient := api.NewGraphQLClient(cfg.BuildkiteToken, cfg.GraphQLEndpoint)

	pipelines, err := api.SearchPipelines(ctx, graphqlClient, cfg.Org, "test-", 100)
	require.NoError(t, err)

	numPipelines := len(pipelines.Organization.Pipelines.Edges)
	t.Logf("found %d pipelines to delete", numPipelines)

	var wg sync.WaitGroup
	wg.Add(numPipelines)
	for _, pipeline := range pipelines.Organization.Pipelines.Edges {
		pipeline := pipeline // prevent loop variable capture
		t.Run(pipeline.Node.Name, func(t *testing.T) {
			builds, err := api.GetBuilds(
				ctx,
				graphqlClient,
				fmt.Sprintf("%s/%s", cfg.Org, pipeline.Node.Name),
				[]api.BuildStates{api.BuildStatesRunning},
				100,
			)
			require.NoError(t, err)

			for _, build := range builds.Pipeline.Builds.Edges {
				_, err = api.BuildCancel(
					ctx,
					graphqlClient,
					api.BuildCancelInput{Id: build.Node.Id},
				)
				assert.NoError(t, err)
			}

			tc := testcase{
				T:            t,
				GraphQL:      api.NewGraphQLClient(cfg.BuildkiteToken, cfg.GraphQLEndpoint),
				PipelineName: pipeline.Node.Name,
			}.Init()
			tc.deletePipeline(ctx)
		})
	}
}

func (t testcase) deletePipeline(ctx context.Context) {
	t.Helper()

	EnsureCleanup(t.T, func() {
		if err := roko.NewRetrier(
			roko.WithMaxAttempts(10),
			roko.WithStrategy(roko.Exponential(time.Second, 5*time.Second)),
		).DoWithContext(ctx, func(r *roko.Retrier) error {
			resp, err := t.Buildkite.Pipelines.Delete(cfg.Org, t.PipelineName)
			if err != nil {
				if resp.StatusCode == http.StatusNotFound {
					return nil
				}
				t.Logf("waiting for build to be canceled on pipeline %s", t.PipelineName)
				return err
			}
			return nil
		}); err != nil {
			t.Errorf("failed to cleanup pipeline %s: %v", t.PipelineName, err)
			return
		}

		t.Logf("deleted pipeline! %s", t.PipelineName)
	})
}
