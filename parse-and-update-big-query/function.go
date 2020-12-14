// Package p contains a Pub/Sub Cloud Function.
package p

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
)

// PubSubMessage is the payload of a Pub/Sub event. Please refer to the docs for
// additional information regarding Pub/Sub events.
type PubSubMessage struct {
	Data []byte `json:"data"`
}

type EnvDeviceData struct {
	env_type             string
	relative_humidity    string
	fan_status_internal  string
	fan_status_exhaust   string
	room_temp            float64
	time_stamp           bigquery.NullTimestamp
	relative_humidity_rh float64
}

func (i *EnvDeviceData) Save() (map[string]bigquery.Value, string, error) {
	return map[string]bigquery.Value{
		"env_type":             i.env_type,
		"relative_humidity":    i.relative_humidity,
		"fan_status_internal":  i.fan_status_internal,
		"fan_status_exhaust":   i.fan_status_exhaust,
		"room_temp":            i.room_temp,
		"time_stamp":           i.time_stamp,
		"relative_humidity_rh": i.relative_humidity_rh,
	}, bigquery.NoDedupeID, nil
}

type Aquaponics struct {
	aquaculture_water_level float64
	reservoir_water_level   float64
	water_temperature       float64
	water_ph                float64
	tds                     float64
	circulation_pump_status string
	reservoir_pump_status   string
	time_stamp              civil.DateTime
}

type Earthworms struct {
	soil_temperature float64
	soil_ph          float64
	lighting_level   float64
	time_stamp       civil.DateTime
}

type Mushrooms struct {
	sprinkler_status string
	lighting_level   float64
	time_stamp       civil.DateTime
}

type Data interface{}

type ParseData map[string]interface{}

// HelloPubSub consumes a Pub/Sub message.
func HelloPubSub(ctx context.Context, m PubSubMessage) error {
	log.Println(m)
	log.Println(ctx)
	log.Println(string(m.Data))
	var jsonData map[string]interface{}
	log.Println("unmarshalling data")
	json.Unmarshal(m.Data, &jsonData)
	deviceType := jsonData["device-type"].(string)
	log.Println(deviceType)
	if deviceType == "env-device" {
		log.Println("saving data for env device")
		saveEnvData(jsonData)
	}
	return nil
}

// InsertData inserts data into bigquery
func insertData(table string, data Data) {
	log.Println("inside insert data")
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, "poc-cloudfaringpirates-797995")
	if err != nil {
		// TODO: Handle error.
		log.Fatalln(err)
	}
	ins := client.Dataset("sustainable_farming").Table(table).Inserter()
	// // Item implements the ValueSaver interface.
	// items := []*Item{
	// 	{Name: "n1", Size: 32.6, Count: 7},
	// 	{Name: "n2", Size: 4, Count: 2},
	// 	{Name: "n3", Size: 101.5, Count: 1},
	// }

	if err := ins.Put(ctx, data); err != nil {
		// TODO: Handle error.
		log.Println(err)
	}
}

func createTimeStamp() bigquery.NullTimestamp {
	retVal := bigquery.NullTimestamp{}
	retVal.Timestamp = time.Now()
	retVal.Valid = true
	return retVal
}

func saveEnvData(data ParseData) {
	log.Println("inside saveEnvData function")
	log.Println(data)
	envData := &EnvDeviceData{}
	envData.env_type = data["env-type"].(string)
	envData.fan_status_exhaust = data["fan-status-exhaust"].(string)
	envData.fan_status_internal = data["fan-status-internal"].(string)
	//envData.relative_humidity = data["relative-humidity"].(string)
	envData.relative_humidity = "87"
	envData.relative_humidity_rh = data["relative-humidity"].(float64)
	envData.room_temp = data["room-temp"].(float64)
	envData.time_stamp = createTimeStamp()
	log.Println(envData)
	log.Println("inserting data")

	insertData("environment", envData)
	log.Println("insert done")
}
