package avro

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"

	"github.com/ricardohsd/kafka-playground/go/events"
)

type fakeRegistry struct{}

func (f *fakeRegistry) Find(subject string, schema string) (int, error) {
	return 10, nil
}

func (f *fakeRegistry) Fetch(schemaID int) (string, error) {
	return "avroSchema", nil
}

func TestSerialize(t *testing.T) {
	vehicleUpdated := &events.VehicleUpdated{
		Id:        "id123",
		Latitude:  52.5177399,
		Longitude: 13.401178,
		Producer:  "go",
		CreatedAt: 1551347096,
		Comments:  "A comment",
	}

	serde := &Avro{
		registry: &fakeRegistry{},
	}

	data, err := serde.Serialize("vehicle", vehicleUpdated)
	if err != nil {
		t.Fatalf("Error must be nil. Got %v", err)
	}

	b := bytes.NewBuffer(data)

	magicByte, _ := b.ReadByte()
	if magicByte != 0 {
		t.Fatalf("Wrong magic byte. Got %v", magicByte)
	}

	schemaID := binary.BigEndian.Uint32(b.Next(4))
	if schemaID != 10 {
		t.Fatalf("Wrong schemaID. Got %v", schemaID)
	}

	event, err := events.DeserializeVehicleUpdated(b)
	if err != nil {
		t.Fatalf("Failed to deserializer avro event. Got %v", err)
	}

	if !reflect.DeepEqual(event, vehicleUpdated) {
		t.Fatalf("Avro record don't match. Got %v,\nExpected %v", event, vehicleUpdated)
	}
}
