package registry

import (
	"errors"
	"sync"

	schemaregistry "github.com/Landoop/schema-registry"
)

var ErrSchemaNotFound = errors.New("Schema not found on cache")

// SchemaRegistryClient defines a contract for any client for fetching schema registry data
type SchemaRegistryClient interface {
	IsRegistered(string, string) (bool, schemaregistry.Schema, error)
}

// SchemaRegistry defines a contract for any implementation of schema registry
type SchemaRegistry interface {
	Find(string, string) (int, error)
	Fetch(int) (string, error)
}

// CachedRegistry defines a cached schema registry
type CachedRegistry struct {
	client SchemaRegistryClient
	cache  map[int]string
	mutex  sync.RWMutex
}

func NewCachedRegistry(registryURL string) (*CachedRegistry, error) {
	client, err := schemaregistry.NewClient(registryURL)
	if err != nil {
		return nil, err
	}

	r := &CachedRegistry{
		client: client,
		cache:  make(map[int]string),
	}

	return r, nil
}

// Find finds a schemaID for the given subject and schema
func (c *CachedRegistry) Find(subject string, rawSchema string) (id int, er error) {
	_, schema, err := c.client.IsRegistered(subject, rawSchema)
	if err != nil {
		return 0, err
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.cache[schema.ID] = rawSchema

	return schema.ID, nil
}

// Fetch fetches a schemaID from the internal cache
func (c *CachedRegistry) Fetch(schemaID int) (string, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	schema, ok := c.cache[schemaID]
	if !ok {
		return "", ErrSchemaNotFound
	}

	return schema, nil
}
