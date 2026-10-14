package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	controller "peri-loom/controller"
)

func (fixture *p6VerifyFixture) captureStatusFailure() {
	fixture.test.Helper()
	fixture.captureOriginalRefs(fmt.Sprintf("verify-%d", fixture.request))
	fixture.capturePrivatePlacement("before-direct-refs")
	fixture.request++
	identifier := fmt.Sprintf("verify-failure-refs-%d", fixture.request)
	body, err := json.Marshal(map[string]any{"version": 1, "request_id": identifier, "command": map[string]any{"command": "maintenance", "request_id": identifier, "database": fixture.database, "operation": map[string]string{"operation": "refs"}}})
	if err != nil {
		fixture.test.Log("failure diagnostic: unable to encode refs")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, "POST", fixture.config.Controller.AuthorityURL, bytes.NewReader(body))
	if err != nil {
		fixture.test.Log("failure diagnostic: unable to construct refs request")
		return
	}
	request.GetBody = nil
	request.Header.Set("Authorization", "Bearer "+fixture.config.Controller.AuthorityToken)
	request.Header.Set("Content-Type", "application/json")
	response, err := fixture.client.Do(request)
	if err != nil {
		fixture.test.Log("failure diagnostic: direct authority refs transport failed")
		return
	}
	defer response.Body.Close()
	encoded, err := io.ReadAll(io.LimitReader(response.Body, 1024*1024))
	if err != nil {
		fixture.test.Log("failure diagnostic: direct authority refs read failed")
		return
	}
	var reply struct {
		OK                 bool    `json:"ok"`
		Error              *string `json:"error"`
		PreconditionFailed bool    `json:"precondition_failed"`
	}
	if json.Unmarshal(encoded, &reply) != nil {
		fixture.test.Logf("failure diagnostic: direct authority refs invalid envelope status=%d", response.StatusCode)
		return
	}
	message := ""
	if reply.Error != nil {
		message = *reply.Error
	}
	for _, secret := range fixture.secrets {
		message = strings.ReplaceAll(message, secret, "[redacted]")
	}
	if len(message) > 4096 {
		message = message[:4096]
	}
	fixture.test.Logf("failure diagnostic: direct authority refs status=%d ok=%t precondition_failed=%t error=%q", response.StatusCode, reply.OK, reply.PreconditionFailed, message)
	fixture.capturePrivatePlacement("after-direct-refs")
}

func (fixture *p6VerifyFixture) capturePrivatePlacement(stage string) {
	fixture.test.Helper()
	encoded, err := os.ReadFile(filepath.Join(fixture.config.MetadataDirectory, "metadata.json"))
	if err != nil {
		fixture.test.Logf("failure diagnostic: private placement unavailable stage=%s", stage)
		return
	}
	var envelope struct {
		Payload controller.Metadata `json:"payload"`
	}
	if json.Unmarshal(encoded, &envelope) != nil {
		fixture.test.Logf("failure diagnostic: private placement malformed stage=%s", stage)
		return
	}
	placement, found := envelope.Payload.Placements[fixture.database.Key()]
	if !found {
		fixture.test.Logf("failure diagnostic: private placement absent stage=%s", stage)
		return
	}
	fixture.test.Logf("failure diagnostic: private placement stage=%s revision=%d global_epoch=%d phase=%s desired=%s worker=%s epoch=%d grant_present=%t", stage, envelope.Payload.Revision, envelope.Payload.Epoch, placement.Phase, placement.DesiredState, placement.DesiredWorker, placement.Epoch, placement.Grant != nil)
}
