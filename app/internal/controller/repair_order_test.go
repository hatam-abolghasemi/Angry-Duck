package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"angryduck/internal/kube"
	"angryduck/internal/model"
)

func brokenPod(ns, name, node, image, reason, message string) kube.Pod {
	p := stuckPod(ns, name, node, image, reason, "IfNotPresent")
	p.Status.ContainerStatuses[0].State.Waiting.Message = message
	return p
}

const unpackErr = "failed to create containerd container: error unpacking image: failed to extract layer sha256:aa: failed to get reader from content store: content digest sha256:bb: not found"

// A container that can't start on its image's content gets the node a
// repair order, once per RetryAfter; other CreateContainerErrors don't.
func TestRescuer_OrdersRepairOfImagesContainersCantStartOn(t *testing.T) {
	var mu sync.Mutex
	var got []model.RepairOrder
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repair" || r.Header.Get("Authorization") != "Bearer "+rescueToken {
			http.Error(w, "no", http.StatusUnauthorized)
			return
		}
		var o model.RepairOrder
		_ = json.NewDecoder(r.Body).Decode(&o)
		mu.Lock()
		got = append(got, o)
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	reg := NewRegistry(time.Minute, time.Minute)
	report(reg, "worker21", strings.TrimPrefix(srv.URL, "http://"), 0.3, img) // reports it: a name is all it takes

	rs := newTestRescuer(reg, []kube.Pod{
		brokenPod("ns", "a", "worker21", img, "CreateContainerError", unpackErr),
		brokenPod("ns", "b", "worker21", img, "CreateContainerError", unpackErr), // same pair: one order
		brokenPod("ns", "c", "worker21", "registry.example.com/team/other:1", "CreateContainerError", `exec: "/app": no such file or directory`),
	})
	rs.tick(context.Background())
	if len(got) != 1 || got[0].Image != img || got[0].Reason != unpackErr {
		t.Fatalf("orders = %+v, want one for %s carrying the error", got, img)
	}
	rs.tick(context.Background())
	if len(got) != 1 {
		t.Fatalf("re-ordered within RetryAfter: %d orders", len(got))
	}
}

func TestFindBroken_OnlyImageContentErrors(t *testing.T) {
	pods := []kube.Pod{
		brokenPod("ns", "a", "n1", "r.io/app:1", "CreateContainerError", unpackErr),
		brokenPod("ns", "b", "n1", "r.io/app:2", "CreateContainerError", "container name already in use"),
		brokenPod("ns", "c", "n1", "r.io/app:3", "ImagePullBackOff", unpackErr),
	}
	b := findBroken(pods)
	if len(b) != 1 || b[0].Image != "r.io/app:1" || b[0].Node != "n1" {
		t.Fatalf("findBroken = %+v", b)
	}
}
