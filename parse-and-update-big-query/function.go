// Package p contains a Pub/Sub Cloud Function.
package p

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"cloud.google.com/go/bigquery"
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
	time_stamp              bigquery.NullTimestamp
}

func (i *Aquaponics) Save() (map[string]bigquery.Value, string, error) {
	return map[string]bigquery.Value{
		"aquaculture_water_level": i.aquaculture_water_level,
		"reservoir_water_level":   i.reservoir_water_level,
		"water_temperature":       i.water_temperature,
		"water_ph":                i.water_ph,
		"tds":                     i.tds,
		"circulation_pump_status": i.circulation_pump_status,
		"reservoir_pump_status":   i.reservoir_pump_status,
		"time_stamp":              i.time_stamp,
	}, bigquery.NoDedupeID, nil
}

type Earthworms struct {
	soil_temperature float64
	soil_ph          float64
	lighting_level   float64
	time_stamp       bigquery.NullTimestamp
}

func (i *Earthworms) Save() (map[string]bigquery.Value, string, error) {
	return map[string]bigquery.Value{
		"soil_temperature": i.soil_temperature,
		"soil_ph":          i.soil_ph,
		"lighting_level":   i.lighting_level,
		"time_stamp":       i.time_stamp,
	}, bigquery.NoDedupeID, nil
}

type Mushrooms struct {
	sprinkler_status string
	lighting_level   float64
	time_stamp       bigquery.NullTimestamp
}

func (i *Mushrooms) Save() (map[string]bigquery.Value, string, error) {
	return map[string]bigquery.Value{
		"sprinkler_status": i.sprinkler_status,
		"lighting_level":   i.lighting_level,
		"time_stamp":       i.time_stamp,
	}, bigquery.NoDedupeID, nil
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
	} else if deviceType == "aquaponics-device" {
		log.Println("saving data for aquaponics device")
		saveAquaponicsData(jsonData)
	} else if deviceType == "mushrooms-device" {
		log.Println("saving data for mushrooms device")
		saveMushroomsData(jsonData)
	} else if deviceType == "earthworms-device" {
		log.Println("saving data for earthworms device")
		saveEarthwormsData(jsonData)
	}
	return nil
}

// InsertData inserts data into bigquery
func insertData(table string, data Data) {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, "poc-cloudfaringpirates-797995")
	if err != nil {
		// TODO: Handle error.
		log.Fatalln(err)
	}
	ins := client.Dataset("sustainable_farming").Table(table).Inserter()

	if err := ins.Put(ctx, data); err != nil {
		// TODO: Handle error.
		log.Println(err)
	}
}

func createTimeStamp() bigquery.NullTimestamp {
	retVal := bigquery.NullTimestamp{}
	retVal.Timestamp = time.Now().UTC()
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

func saveAquaponicsData(data ParseData) {
	log.Println("inside saveAquaponicsData function")
	log.Println(data)
	aquaponicsData := &Aquaponics{}
	aquaponicsData.aquaculture_water_level = data["aquaculture-water-level"].(float64)
	aquaponicsData.circulation_pump_status = data["circulation-pump-status"].(string)
	aquaponicsData.reservoir_pump_status = data["reservoir-pump-status"].(string)
	//envData.relative_humidity = data["relative-humidity"].(string)
	aquaponicsData.reservoir_water_level = data["reservoir-water-level"].(float64)
	aquaponicsData.tds = data["tds"].(float64)
	aquaponicsData.water_ph = data["water-ph"].(float64)
	aquaponicsData.water_temperature = data["water-temperature"].(float64)
	aquaponicsData.time_stamp = createTimeStamp()
	log.Println(aquaponicsData)
	log.Println("inserting data")

	insertData("aquaponics", aquaponicsData)
	log.Println("insert done")
}

func saveMushroomsData(data ParseData) {
	log.Println("inside savemushroomsData function")
	log.Println(data)
	mushroomsData := &Mushrooms{}
	mushroomsData.lighting_level = data["lighting-level"].(float64)
	mushroomsData.sprinkler_status = data["sprinkler-status"].(string)
	mushroomsData.time_stamp = createTimeStamp()
	log.Println(mushroomsData)
	log.Println("inserting data")

	insertData("mushrooms", mushroomsData)
	log.Println("insert done")
}

func saveEarthwormsData(data ParseData) {
	log.Println("inside saveEarthwormsData function")
	log.Println(data)
	mushroomsData := &Earthworms{}
	mushroomsData.lighting_level = data["lighting-level"].(float64)
	mushroomsData.soil_ph = data["soil-ph"].(float64)
	mushroomsData.soil_temperature = data["soil-temperature"].(float64)
	mushroomsData.time_stamp = createTimeStamp()
	log.Println(mushroomsData)
	log.Println("inserting data")

	insertData("earthworms", mushroomsData)
	log.Println("insert done")
}
