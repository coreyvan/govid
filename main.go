package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/esapi"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/pariz/gountries"
)

func main() {
	var numUploaders int = 10
	var batchSize int = 50

	points, err := readDatapoints("us.data", 3000)
	if err != nil {
		log.Fatal("could not read file", err)
	}

	numBatches := int(math.Ceil(float64(len(points) / batchSize)))
	log.Println("Number of records:", len(points))
	log.Println("Number of batches:", numBatches)

	ec, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{
			"http://localhost:9200",
		},
	})
	if err != nil {
		log.Fatal("could not create elasticsearch client", err)
	}

	res, err := ec.Info()
	if err != nil {
		log.Fatal("could not get cluster info", err)
	}

	if res.IsError() {
		log.Fatal("Error:", res.String())
	}

	var r map[string]interface{}
	// Deserialize the response into a map.
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
	}
	// Print client and server version numbers.
	log.Printf("ES Client: %s", elasticsearch.Version)
	log.Printf("ES Server: %s", r["version"].(map[string]interface{})["number"])
	log.Println(strings.Repeat("-", 30))

	q := make(chan batch)
	done := make(chan bool)

	// Initialize workers
	for i := 0; i < numUploaders; i++ {
		log.Println("Initializing worker", i)
		go bulkUploader(q, i, ec, done)
	}

	var payload []datapoint
	var currBatch = 1

	for i, e := range points {
		payload = append(payload, e)
		if (i+1)%batchSize == 0 || i+1 == len(points) {
			log.Printf("Sending batch %d to queue", currBatch)
			go func(b batch) {
				q <- b
			}(batch{ID: currBatch, Payload: payload})
			currBatch++
			payload = nil
		}
	}

	for c := 0; c < (numBatches); c++ {
		<-done
	}

	// uploadPoints(ec, &points, "covid")

}

type datapoint struct {
	Ts           time.Time `json:"@timestamp"`
	CountryName  string    `json:"country_name"`
	CountryCode  string    `json:"country_code"`
	Province     string    `json:"province"`
	ProvinceCode string    `json:"province_code"`
	City         string    `json:"city"`
	CityCode     string    `json:"city_code"`
	Geo          geo       `json:"geo"`
	Cases        int       `json:"cases"`
	Status       string    `json:"status"`
}

type geo struct {
	Lat  float64 `json:"lat"`
	Long float64 `json:"lon"`
}

func (d *datapoint) UnmarshalJSON(b []byte) error {
	var s map[string]interface{}

	err := json.Unmarshal(b, &s)
	if err != nil {
		return err
	}
	d.Ts, err = time.Parse(time.RFC3339, s["Date"].(string))
	if err != nil {
		return err
	}
	d.CountryName = s["Country"].(string)
	d.CountryCode = s["CountryCode"].(string)
	d.Province = s["Province"].(string)

	if _, ok := s["City"]; ok {
		d.City = s["City"].(string)
	}
	if _, ok := s["CityCode"]; ok {
		d.CityCode = s["CityCode"].(string)
	}
	d.Geo.Lat, err = strconv.ParseFloat(s["Lat"].(string), 64)
	if err != nil {
		return err
	}
	d.Geo.Long, err = strconv.ParseFloat(s["Lon"].(string), 64)
	if err != nil {
		return err
	}
	d.Cases = int(s["Cases"].(float64))
	d.Status = s["Status"].(string)

	return nil
}

func readDatapoints(f string, n int) ([]datapoint, error) {
	data, err := ioutil.ReadFile(f)
	if err != nil {
		return nil, err
	}

	var points []datapoint
	err = json.Unmarshal(data, &points)
	if err != nil {
		return nil, err
	}

	query := gountries.New()
	us, err := query.FindCountryByAlpha("US")
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(points); i++ {
		if points[i].Province == "Virgin Islands" {
			points[i].ProvinceCode = "US-VI"
		} else if points[i].Province == "Grand Princess" {
			points[i].ProvinceCode = ""
		} else if points[i].Province == "Diamond Princess" {
			points[i].ProvinceCode = ""
		} else {
			pCode, err := us.FindSubdivisionByName(points[i].Province)
			if err != nil {
				return nil, err
			}

			points[i].ProvinceCode = "US-" + pCode.Code
		}
	}

	return points[:n], nil
}

func uploadPoints(ec *elasticsearch.Client, p *[]datapoint, idx string) (int, error) {
	var count int
	for _, v := range *p {
		ej, err := json.Marshal(v)
		if err != nil {
			return count, err
		}

		req := esapi.IndexRequest{
			Index:   "covid",
			Body:    bytes.NewReader(ej),
			Refresh: "true",
		}

		_, err = req.Do(context.Background(), ec)
		if err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

type batch struct {
	Payload []datapoint
	ID      int
}

func bulkUploader(queue chan batch, wid int, ec *elasticsearch.Client, done chan bool) {
	for {
		batch := <-queue
		log.Printf("Uploading batch %d of %d records\n", batch.ID, len(batch.Payload))
		done <- true
	}
}

func timeTaken(t time.Time, n int) {
	elapsed := time.Since(t)
	log.Printf("Num uploaders: %d\t\t%s\n", n, elapsed)
}
