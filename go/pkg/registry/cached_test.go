package registry

import (
	"errors"
	"testing"

	schemaregistry "github.com/Landoop/schema-registry"
)

type fakeClient struct {
	withError bool
}

func (f *fakeClient) IsRegistered(subject string, schema string) (bool, schemaregistry.Schema, error) {
	if f.withError {
		return true, schemaregistry.Schema{}, errors.New("Got an error")
	}

	return true, schemaregistry.Schema{ID: 10}, nil
}

func TestFind(t *testing.T) {
	t.Run("Schema found", func(t *testing.T) {
		registry := &CachedRegistry{
			client: &fakeClient{},
			cache:  make(map[int]string),
		}

		schemaID, err := registry.Find("subject", "avroSchema")
		if err != nil {
			t.Fatalf("Error must be nil. Found %v", err)
		}

		if schemaID != 10 {
			t.Errorf("Got wrong schema id")
		}

		_, ok := registry.cache[schemaID]
		if !ok {
			t.Errorf("Cache wasn't set properly")
		}
	})

	t.Run("Schema not found", func(t *testing.T) {
		registry := &CachedRegistry{
			client: &fakeClient{true},
			cache:  make(map[int]string),
		}

		_, err := registry.Find("subject", "avroSchema")
		if err == nil {
			t.Fatalf("Error must not be nil")
		}
	})
}

func TestFetch(t *testing.T) {
	registry := &CachedRegistry{
		cache: map[int]string{
			10: "anAvroSchema",
		},
	}

	schema, err := registry.Fetch(10)
	if err != nil {
		t.Fatalf("Error must be nil. Got %v", err)
	}

	if schema != "anAvroSchema" {
		t.Fatalf("Returned schema is wrong. Got %v", schema)
	}

	schema, err = registry.Fetch(20)
	if err == nil {
		t.Fatalf("Error must not be nil.")
	}

	if schema != "" {
		t.Fatalf("Returned schema is wrong. Got %v", schema)
	}
}
