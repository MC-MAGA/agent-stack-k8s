package scheduler

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/buildkite/agent-stack-k8s/v2/api"
	"github.com/buildkite/agent-stack-k8s/v2/internal/controller/config"
	"github.com/google/uuid"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

const testJobUUID = "019f719c-0000-0000-0000-000000000000"

func newTestJobWatcher(t *testing.T, fakeServer *api.FakeAgentServer) (*jobWatcher, *fake.Clientset) {
	t.Helper()
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	agentClient, err := api.NewAgentClient(ctx, api.AgentClientOpts{
		Token:    "fake-token",
		Endpoint: fakeServer.URL(),
		StackID:  "test-stack",
		Logger:   logger,
	})
	if err != nil {
		t.Fatalf("NewAgentClient: %v", err)
	}

	k8sClient := fake.NewSimpleClientset()

	w := NewJobWatcher(logger, k8sClient, agentClient, &config.Config{
		Namespace:           "default",
		EmptyJobGracePeriod: 30 * time.Second,
	})

	return w, k8sClient
}

func newTestK8sJob(jobUUID string) *batchv1.Job {
	// Started long enough ago to be past EmptyJobGracePeriod.
	started := metav1.NewTime(time.Now().Add(-time.Minute))
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "buildkite-job-" + jobUUID[:8],
			Namespace: "default",
			Labels: map[string]string{
				config.UUIDLabel: jobUUID,
			},
		},
		Status: batchv1.JobStatus{
			StartTime: &started,
		},
	}
}

func TestCleanupStalledJobs(t *testing.T) {
	tests := []struct {
		name string
		// bkState is the job state reported by the fake BK API. Empty means
		// the job is unknown to BK (deleted/expired).
		bkState          string
		getJobStateFails bool
		finishJobFails   bool

		wantFinishJobCalls int
		wantCleanedUp      bool
		wantStalling       bool
		wantIgnored        bool
	}{
		{
			name:          "skips cleanup when an agent is working on the job",
			bkState:       "running",
			wantCleanedUp: false,
			wantStalling:  false,
			wantIgnored:   false,
		},
		{
			name:          "skips cleanup when an agent has accepted the job",
			bkState:       "accepted",
			wantCleanedUp: false,
			wantStalling:  false,
			wantIgnored:   false,
		},
		{
			name:               "cleans up when BK job is reserved",
			bkState:            "reserved",
			wantFinishJobCalls: 1,
			wantCleanedUp:      true,
			wantStalling:       false,
			wantIgnored:        true,
		},
		{
			name:               "cleans up when BK job is scheduled",
			bkState:            "scheduled",
			wantFinishJobCalls: 1,
			wantCleanedUp:      true,
			wantStalling:       false,
			wantIgnored:        true,
		},
		{
			name:               "cleans up when BK job has ended",
			bkState:            "finished",
			wantFinishJobCalls: 1,
			wantCleanedUp:      true,
			wantStalling:       false,
			wantIgnored:        true,
		},
		{
			name:               "cleans up when BK job is unknown (deleted/expired)",
			bkState:            "",
			wantFinishJobCalls: 1,
			wantCleanedUp:      true,
			wantStalling:       false,
			wantIgnored:        true,
		},
		{
			name:             "keeps job in stalling set when GetJobState fails, for retry next cycle",
			getJobStateFails: true,
			wantCleanedUp:    false,
			wantStalling:     true,
			wantIgnored:      false,
		},
		{
			name:               "cleans up even when failJob fails",
			bkState:            "reserved",
			finishJobFails:     true,
			wantFinishJobCalls: 1,
			wantCleanedUp:      true,
			wantStalling:       false,
			wantIgnored:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			server := api.NewFakeAgentServer()
			defer server.Close()

			if tt.bkState != "" {
				server.JobStates = map[string]string{testJobUUID: tt.bkState}
			}
			if tt.getJobStateFails {
				server.GetJobStatesStatusCode = 500
				server.GetJobStatesError = "internal error"
			}
			if tt.finishJobFails {
				server.FinishJobStatusCode = 404
				server.FinishJobError = "not found"
			}

			w, k8sClient := newTestJobWatcher(t, server)

			kjob := newTestK8sJob(testJobUUID)
			if _, err := k8sClient.BatchV1().Jobs("default").Create(ctx, kjob, metav1.CreateOptions{}); err != nil {
				t.Fatalf("Create job: %v", err)
			}

			jobUUID := uuid.MustParse(testJobUUID)
			w.addToStalling(jobUUID, kjob)

			w.cleanupStalledJobs(ctx)

			if got := len(server.FinishJobCalls); got != tt.wantFinishJobCalls {
				t.Errorf("len(FinishJobCalls) = %d, want %d", got, tt.wantFinishJobCalls)
			}

			updated, err := k8sClient.BatchV1().Jobs("default").Get(ctx, kjob.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("Get job: %v", err)
			}
			cleanedUp := updated.Spec.ActiveDeadlineSeconds != nil && *updated.Spec.ActiveDeadlineSeconds == 1
			if cleanedUp != tt.wantCleanedUp {
				t.Errorf("job cleaned up (ActiveDeadlineSeconds set) = %t, want %t", cleanedUp, tt.wantCleanedUp)
			}

			w.stallingJobsMu.Lock()
			_, stalling := w.stallingJobs[jobUUID]
			w.stallingJobsMu.Unlock()
			if stalling != tt.wantStalling {
				t.Errorf("job in stallingJobs = %t, want %t", stalling, tt.wantStalling)
			}

			if got := w.isIgnored(jobUUID); got != tt.wantIgnored {
				t.Errorf("isIgnored = %t, want %t", got, tt.wantIgnored)
			}
		})
	}
}
