package main

import "testing"

func TestListenAddressesOnTheNodesNetwork(t *testing.T) {
	cases := []struct {
		addr string
		host bool
		want string
	}{
		{":18082", true, "127.0.0.1:18082"},
		{"127.0.0.1:18082", true, "127.0.0.1:18082"},
		{"10.0.0.5:9000", true, "10.0.0.5:9000"},
		{":18082", false, ":18082"},
		{"127.0.0.1:18082", false, ":18082"},
		{"garbage", false, ":18082"},
		{"garbage", true, "127.0.0.1:18082"},
	}
	for _, c := range cases {
		if got := mirrorListenAddr(c.addr, "n1", c.host); got != c.want {
			t.Errorf("mirrorListenAddr(%q, host=%t) = %q, want %q", c.addr, c.host, got, c.want)
		}
	}
	if got := bindHost(":18081", "10.0.0.5"); got != "10.0.0.5:18081" {
		t.Errorf("bindHost = %q", got)
	}
	if got := bindHost("0.0.0.0:18081", "10.0.0.5"); got != "0.0.0.0:18081" {
		t.Errorf("an explicit host must stay, got %q", got)
	}
	if got := bindHost(":18081", ""); got != ":18081" {
		t.Errorf("no IP must keep the address, got %q", got)
	}
}
