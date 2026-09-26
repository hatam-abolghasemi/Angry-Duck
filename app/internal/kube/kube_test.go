package kube

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestListPods_PaginatesAndSendsFreshToken(t *testing.T) {
	tokenPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte("tok-1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var auths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auths = append(auths, r.Header.Get("Authorization"))
		if r.URL.Query().Get("fieldSelector") != "status.phase=Pending" {
			http.Error(w, "bad selector", 400)
			return
		}
		page := map[string]interface{}{"items": []map[string]interface{}{{"metadata": map[string]string{"name": "p" + r.URL.Query().Get("continue")}}}}
		if r.URL.Query().Get("continue") == "" {
			page["metadata"] = map[string]string{"continue": "2"}
			_ = os.WriteFile(tokenPath, []byte("tok-2"), 0o600) // kubelet rotates the token
		}
		_ = json.NewEncoder(w).Encode(page)
	}))
	defer srv.Close()

	pods, err := New(srv.URL, tokenPath, srv.Client()).ListPods(context.Background(), "status.phase=Pending")
	if err != nil {
		t.Fatal(err)
	}
	if len(pods) != 2 || pods[0].Metadata.Name != "p" || pods[1].Metadata.Name != "p2" {
		t.Fatalf("pods = %+v", pods)
	}
	if auths[0] != "Bearer tok-1" || auths[1] != "Bearer tok-2" {
		t.Fatalf("auth headers = %v", auths)
	}
}

func TestListPods_APIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `pods is forbidden`, http.StatusForbidden)
	}))
	defer srv.Close()
	if _, err := New(srv.URL, "", srv.Client()).ListPods(context.Background(), "x"); err == nil {
		t.Fatal("expected an error on 403")
	}
}
