package metrics

import (
	"testing"

	"github.com/coroot/coroot-cluster-agent/config"
	"github.com/coroot/coroot-cluster-agent/k8s"
)

func TestTargetFromConfig(t *testing.T) {
	target := TargetFromConfig(config.ApplicationInstrumentation{
		Type: "mongodb",
		Host: "db.example.com",
		Port: "27017",
		Sni:  "mongo.example.com",
		Credentials: config.Credentials{
			Username: "user",
			Password: "pass",
		},
		Params: map[string]string{
			"tls":        "true",
			"authSource": "admin",
		},
		Instance: "prod",
	})
	if target.Type != TargetTypeMongodb {
		t.Errorf("Type = %q, want %q", target.Type, TargetTypeMongodb)
	}
	if target.Addr != "db.example.com:27017" {
		t.Errorf("Addr = %q, want %q", target.Addr, "db.example.com:27017")
	}
	if target.Sni != "mongo.example.com" {
		t.Errorf("Sni = %q, want %q", target.Sni, "mongo.example.com")
	}
	if target.Credentials.Username != "user" || target.Credentials.Password != "pass" {
		t.Errorf("Credentials = %+v, want user/pass", target.Credentials)
	}
	if target.Params["tls"] != "true" || target.Params["authSource"] != "admin" {
		t.Errorf("Params = %+v, want tls=true authSource=admin", target.Params)
	}
	if target.Description != "prod" {
		t.Errorf("Description = %q, want %q", target.Description, "prod")
	}
}

func TestTargetFromConfigEmptySni(t *testing.T) {
	target := TargetFromConfig(config.ApplicationInstrumentation{
		Type: "mongodb",
		Host: "10.0.0.1",
		Port: "27017",
	})
	if target.Sni != "" {
		t.Errorf("Sni = %q, want empty", target.Sni)
	}
}

func TestTargetEqualSni(t *testing.T) {
	base := &Target{
		Type:   TargetTypeMongodb,
		Addr:   "db.example.com:27017",
		Sni:    "mongo.example.com",
		Params: map[string]string{"tls": "true"},
	}
	if !base.Equal(base) {
		t.Error("target should equal itself")
	}

	sameSni := &Target{
		Type:   TargetTypeMongodb,
		Addr:   "db.example.com:27017",
		Sni:    "mongo.example.com",
		Params: map[string]string{"tls": "true"},
	}
	if !base.Equal(sameSni) {
		t.Error("targets with identical Sni should be equal")
	}

	differentSni := sameSni
	differentSni.Sni = "other.example.com"
	if base.Equal(differentSni) {
		t.Error("targets with different Sni should not be equal")
	}

	emptySni := &Target{
		Type:   TargetTypeMongodb,
		Addr:   "db.example.com:27017",
		Sni:    "",
		Params: map[string]string{"tls": "true"},
	}
	if base.Equal(emptySni) {
		t.Error("targets with empty vs set Sni should not be equal")
	}
}

func TestTargetFromPodDoesNotSynthesizeSni(t *testing.T) {
	pod := &k8s.Pod{
		Id: k8s.PodId{Namespace: "ns", Name: "mongodb-0", NodeName: "node-1"},
		IP: "10.0.0.1",
		Annotations: map[string]string{
			"coroot.com/mongodb-scrape":           "true",
			"coroot.com/mongodb-scrape-param-tls": "true",
		},
	}
	target := TargetFromPod(pod)
	if target == nil {
		t.Fatal("TargetFromPod returned nil")
	}
	if target.Sni != "" {
		t.Errorf("Sni = %q, want empty: SNI must not be synthesized from pod data", target.Sni)
	}
	if target.Addr != "10.0.0.1:27017" {
		t.Errorf("Addr = %q, want %q", target.Addr, "10.0.0.1:27017")
	}
	if target.Params["tls"] != "true" {
		t.Errorf("Params[tls] = %q, want %q", target.Params["tls"], "true")
	}
}
