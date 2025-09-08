package stacksapi

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestRegisterStack(t *testing.T) {
	t.Parallel()
	t.Run("successful registration", func(t *testing.T) {
		t.Parallel()
		server, client := setupTestServer(t, func(w http.ResponseWriter, r *http.Request) {
			verifyAuthAndMethod(t, r, "POST", "/stacks/register")

			// Verify request body
			var params RegisterStackRequest
			if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
				t.Fatalf("failed to decode request body: %v", err)
			}

			expectedParams := RegisterStackRequest{
				Key:      "test-stack",
				Type:     StackTypeCustom,
				QueueKey: "test-queue",
				Metadata: map[string]string{"env": "test"},
			}

			if diff := cmp.Diff(expectedParams, params); diff != "" {
				t.Errorf("request params mismatch (-want +got):\n%s", diff)
			}

			response := RegisterStackResponse{
				ID:               "stack-123",
				OrganizationUUID: "org-456",
				ClusterQueueKey:  "test-queue",
				Key:              "test-stack",
				Type:             StackTypeCustom,
				Metadata:         map[string]string{"env": "test"},
				LastConnectedOn:  nil,
				State:            StackStateConnected,
			}

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(response)
		})
		t.Cleanup(func() { server.Close() })

		ctx := context.Background()
		params := RegisterStackRequest{
			Key:      "test-stack",
			Type:     StackTypeCustom,
			QueueKey: "test-queue",
			Metadata: map[string]string{"env": "test"},
		}

		stack, err := client.RegisterStack(ctx, params)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		expected := RegisterStackResponse{
			ID:               "stack-123",
			OrganizationUUID: "org-456",
			ClusterQueueKey:  "test-queue",
			Key:              "test-stack",
			Type:             StackTypeCustom,
			Metadata:         map[string]string{"env": "test"},
			State:            StackStateConnected,
		}

		if diff := cmp.Diff(expected, stack); diff != "" {
			t.Errorf("stack mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("handles server error", func(t *testing.T) {
		t.Parallel()
		server, client := setupTestServer(t, func(w http.ResponseWriter, r *http.Request) {
			respondWithError(w, http.StatusInternalServerError, "Internal server error")
		})
		t.Cleanup(func() { server.Close() })

		params := RegisterStackRequest{
			Key:      "test-stack",
			Type:     StackTypeCustom,
			QueueKey: "test-queue",
			Metadata: map[string]string{},
		}

		_, err := client.RegisterStack(ctx, params)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !errorIsStatusCode(err, 500) {
			t.Errorf("errorIsStatusCode(err, 500) = false, expected true, err: %v", err)
		}
	})
}
