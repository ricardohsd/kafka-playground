package avro

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/ricardohsd/kafka-playground/go/pkg/registry"
)

type SerializableEvent interface {
	Serialize(w io.Writer) error
	Schema() string
}

type Avro struct {
	registry registry.SchemaRegistry
}

func New(registryURL string) (*Avro, error) {
	cachedRegistry, err := registry.NewCachedRegistry(registryURL)
	if err != nil {
		return nil, err
	}

	return &Avro{cachedRegistry}, nil
}

func (s *Avro) Serialize(subject string, avroEvent SerializableEvent) ([]byte, error) {
	var msgBuf bytes.Buffer
	avroEvent.Serialize(&msgBuf)

	schemaID, err := s.registry.Find(subject, avroEvent.Schema())
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer

	buf.WriteByte(0)

	err = binary.Write(&buf, binary.BigEndian, uint32(schemaID))
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(msgBuf.Bytes())
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
