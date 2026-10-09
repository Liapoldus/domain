package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// Digests were captured from the current WIP before replacing file-loaded definitions.
func TestContractSemantics(t *testing.T) {
	expected := map[string]string{"v1/model.schema.json": "9f23b3f7f8fa213ed6409ee252b3af8378042e5c11dc605b7e5c5a542b7505f8", "v1/schemas/batch.request.json": "6cea2ca6de2036954c84a9ae576bb1427d6c625a6588b53707dfe79b98c90b0f", "v1/schemas/batch.response.json": "ea43c94a99809ca7a48779f70d067f5c49d56f9674e24cfb30c80cdf1cbadce7", "v1/schemas/cluster-status.request.json": "3b8e2579f049ffc99e1ca3159b65d2951881535862b7a1729246b59fafefd396", "v1/schemas/create.request.json": "8194447a95cf811f1ac45ddbc4c0afa6efb7073011955a70db2efac050b8b880", "v1/schemas/delete.request.json": "2afbe1445c0f03a330d160cb533d31457eb291757c513fc194ae279e467aec5f", "v1/schemas/envelope.json": "fac5f2431739ca3370f3e11351a92589992247f90eed8bca2ca3d0020f11b3e3", "v1/schemas/get.request.json": "ccbc49cc0e88a6c8aac2f18a24f65cd46bc66dea065135d2a20d84109a376fb4", "v1/schemas/get.response.json": "fb3fc10e518485aaa0943dd3c63f908f4500085d358e6b5cd0551b98ef0bb9aa", "v1/schemas/migration-plan.request.json": "7d469ca3e0f378c80562272552abd2a7e3856fb1d9b06c4eb954322b5b217ec5", "v1/schemas/migration-plan.response.json": "07e5671a2f8a1ca0add43cea7c73da3882bc64c60a20c6134cc21a608e6cccfa", "v1/schemas/migration-status.request.json": "977a273f02a031cfdef89fe6f88c2949af0378c72fd98cd2a9844e225ea513ce", "v1/schemas/migration-status.response.json": "5da1ebd9912a472b2dc235ec5ba522b89ac251c285aa7e06b238591788f09f9a", "v1/schemas/query.request.json": "d4985b7cfcc1c2a8ab847b0cf4a06f24169b071a7d094fbeb9ccaac20962d3e6", "v1/schemas/query.response.json": "22c52298e3db5cd49fd5786c94935973b8bf6edddd8acc5c29247a9fa5cddb25", "v1/schemas/status.response.json": "3ac8df67792338cac48c636f7d9b1c7aef29662b1dbd2885fe7b8c07921ba836", "v1/schemas/update.request.json": "3a1751b1c32c3aa51d485c330b8439b85197c1b0a203ff9715fc45207ffe2cab", "v1/schemas/write.response.json": "2ac3e6aa0efc280d415e38f1cd2886f9f16d69706f442561bf746ae78b1d2da3", "v1/plugin.json": "c4cb472ff04bbb139fc96d021c7b636d493e1a85229c5c387fa1605b940f89b3", "v1/raft-peer.json": "533236b6f5294c68175341911570068bb3391e5130043fa66c3bd0e7e40dc4e7"}
	artifacts, err := Artifacts()
	if err != nil {
		t.Fatal(err)
	}
	if len(artifacts) != len(expected) {
		t.Fatalf("artifact set changed: %d", len(artifacts))
	}
	for name, digest := range expected {
		t.Run(name, func(t *testing.T) {
			var value any
			if err := json.Unmarshal(artifacts[name], &value); err != nil {
				t.Fatal(err)
			}
			canonical, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			hash := sha256.Sum256(canonical)
			if hex.EncodeToString(hash[:]) != digest {
				t.Fatalf("contract semantics changed: %s", name)
			}
		})
	}
}
func TestGeneratedArtifacts(t *testing.T) {
	artifacts, err := Artifacts()
	if err != nil {
		t.Fatal(err)
	}
	for name, expected := range artifacts {
		actual, err := os.ReadFile(filepath.Join(name))
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("stale artifact: %s; run make generate", name)
		}
	}
	var actualNames []string
	if err := filepath.WalkDir("v1", func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			actualNames = append(actualNames, p)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(actualNames) != len(artifacts) {
		t.Fatal("unowned public artifact")
	}
	for _, name := range actualNames {
		if _, ok := artifacts[name]; !ok {
			t.Errorf("unowned artifact: %s", name)
		}
	}
}
