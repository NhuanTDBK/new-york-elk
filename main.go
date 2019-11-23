package main

import (
	"bytes"
	"os/exec"
	"strings"
	"sync/atomic"

	"github.com/olivere/elastic"

	// "beekit/tools/migration/logger"

	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/spf13/viper"
	"go.uber.org/zap"
)

const (
	// DateFormat --
	DateFormat = "2006-01-02 15:04:05"
)

const mapping = `
{
	"settings":{
		"number_of_shards": 1,
		"number_of_replicas": 0
	},
	"mappings":{
		"properties":{
			"vendor_id":{
				"type":"keyword"
			},
			"pickup_datetime":{
				"type":"date"
			},
			"dropoff_datetime":{
				"type":"date"
			},
			"store_and_fwd_flag":{
				"type": "keyword"
			},
			"payment_type": {
				"type": "keyword"
			},
			"pickup_location":{
				"type": "geo_point"
			},
			"dropoff_location":{
				"type": "geo_point"
			}
		}
	}
}`

// ParseDate --
func ParseDate(date string) (*time.Time, error) {
	t, err := time.Parse(DateFormat, date)
	return &t, err
}

// ConvertFromStringToTrip --
func ConvertFromStringToTrip(line []string) (*Trip, error) {
	vendorID := line[0]
	pickupTime, err := ParseDate(line[1])
	if err != nil {
		return nil, err
	}

	dropoffTime, err := ParseDate(line[2])
	if err != nil {
		return nil, err
	}

	passengerCount, err := strconv.ParseUint(line[3], 10, 64)
	if err != nil {
		return nil, err
	}

	tripDistance, err := strconv.ParseFloat(line[4], 64)
	if err != nil {
		return nil, err
	}

	pickupLongitude, err := strconv.ParseFloat(line[5], 64)
	if err != nil {
		return nil, err
	}

	pickupLatitude, err := strconv.ParseFloat(line[6], 64)
	if err != nil {
		return nil, err
	}

	rateCode, err := strconv.ParseInt(line[7], 10, 64)
	if err != nil {
		return nil, err
	}

	storeAndFwdFlag := line[8]
	dropoffLongitude, err := strconv.ParseFloat(line[9], 64)
	if err != nil {
		return nil, err
	}

	dropoffLatitude, err := strconv.ParseFloat(line[10], 64)
	if err != nil {
		return nil, err
	}
	paymentType := line[11]

	fareAmount, err := strconv.ParseFloat(line[12], 64)
	if err != nil {
		return nil, err
	}

	surchage, err := strconv.ParseFloat(line[13], 64)
	if err != nil {
		return nil, err
	}

	mtaTax, err := strconv.ParseFloat(line[14], 64)
	if err != nil {
		return nil, err
	}

	tipAmount, err := strconv.ParseFloat(line[15], 64)
	if err != nil {
		return nil, err
	}

	tollsAmount, err := strconv.ParseFloat(line[16], 64)
	if err != nil {
		return nil, err
	}

	totalAmount, err := strconv.ParseFloat(line[17], 64)
	if err != nil {
		return nil, err
	}

	return &Trip{
		VendorID:       vendorID,
		PickupTime:     pickupTime,
		DropOffTime:    dropoffTime,
		PassengerCount: passengerCount,
		TripDistance:   tripDistance,
		PickupLocation: &Location{
			Latitude:  pickupLatitude,
			Longitude: pickupLongitude,
		},
		RateCode:        rateCode,
		StoreAndFwdFlag: storeAndFwdFlag,
		DropoffLocation: &Location{
			Latitude:  dropoffLatitude,
			Longitude: dropoffLongitude,
		},
		PaymentType: paymentType,
		FareAmount:  fareAmount,
		Surcharge:   surchage,
		MtaTax:      mtaTax,
		TipAmount:   tipAmount,
		TollsAmount: tollsAmount,
		TotalAmount: totalAmount,
	}, nil

}

// CountLines --
func CountLines(filePath string) int64 {
	cmder := exec.Command("wc", "-l", filePath)
	var out bytes.Buffer
	cmder.Stdout = &out
	err := cmder.Run()
	if err != nil {
		fmt.Println(err)
		return 0
	}
	fmt.Println(strings.TrimSpace(out.String()))
	t := strings.Split(strings.TrimSpace(out.String()), " ")
	count, err := strconv.ParseInt(t[0], 10, 64)
	if err != nil {
		fmt.Println(err)
		return 0
	}

	return count

}

// Location --
type Location struct {
	Latitude  float64 `json:"lat"`
	Longitude float64 `json:"lon"`
}
type Time struct {
	Hour      int `json:"hour"`
	Minute    int `json:"minute"`
	DayOfWeek int `json:"day_of_week"`
}

// Trip --
type Trip struct {
	VendorID        string     `json:"vendor_id"`
	PickupTime      *time.Time `json:"pickup_datetime"`
	DropOffTime     *time.Time `json:"dropoff_datetime"`
	PassengerCount  uint64     `json:"passenger_count"`
	TripDistance    float64    `json:"trip_distance"`
	PickupLocation  *Location  `json:"pickup_location"`
	RateCode        int64      `json:"rate_code"`
	StoreAndFwdFlag string     `json:"store_and_fwd_flag"`
	DropoffLocation *Location  `json:"dropoff_location"`
	PaymentType     string     `json:"payment_type"`
	FareAmount      float64    `json:"fare_amount"`
	Surcharge       float64    `json:"surcharge"`
	MtaTax          float64    `json:"mta_tax"`
	TipAmount       float64    `json:"tip_amount"`
	TollsAmount     float64    `json:"tolls_amount"`
	TotalAmount     float64    `json:"total_amount"`
}

// GetID --
func (t *Trip) GetID() string {
	unix := fmt.Sprintf("%v_%v_%v_%v_%v_%v_%v", t.VendorID, t.PickupLocation.Latitude, t.PickupLocation.Longitude, t.DropoffLocation.Latitude, t.DropoffLocation.Longitude, t.PickupTime.Unix(), t.DropOffTime.Unix())
	return unix
}

// ToJSON --
func (t *Trip) ToJSON() []byte {
	db, _ := json.Marshal(t)
	return db
}

func main() {
	var err error

	ctx := context.Background()
	filePath := os.Args[1]

	viper.SetConfigFile("./conf.toml")
	sLogger, _ := zap.NewProduction()
	defer sLogger.Sync()
	logger := sLogger.Sugar()

	if err = viper.ReadInConfig(); err != nil {
		logger.Errorf("Cannot read file config: %v ", err)
		return
	}

	s := time.Now()

	numWorkers := viper.GetInt("app.workers")
	if numWorkers == 0 {
		numWorkers = 4
	}

	bulkImports := viper.GetInt("app.bulk_imports")
	if bulkImports == 0 {
		bulkImports = 1500
	}

	esAddress := viper.GetString("es.address")
	indexName := viper.GetString("es.index_name")

	logger.Infof("Connect to es: %v", esAddress)
	esClient, err := elastic.NewClient(elastic.SetURL(esAddress), elastic.SetSniff(false))
	if err != nil {
		logger.Error(err)
		return
	}

	exists, err := esClient.IndexExists(indexName).Do(ctx)
	if err != nil {
		// Handle error
		panic(err)
	}

	if !exists {
		logger.Info("Create mapping")
		createIndex, err := esClient.CreateIndex(indexName).BodyString(mapping).Do(ctx)
		if err != nil {
			// Handle error
			panic(err)
		}
		if !createIndex.Acknowledged {
			// Not acknowledged
			logger.Error("Not ack")
		}
	}

	var totalLines = uint32(CountLines(filePath))

	ctx, cancelFunc := context.WithCancel(ctx)
	jobs := make(chan []*Trip, 0)
	var counter uint32

	for i := 0; i < numWorkers; i++ {
		rCtx := context.WithValue(ctx, "a", "b")

		go func(ctx context.Context, client *elastic.Client) {
			isWorking := false
			for {
				select {
				case docs := <-jobs:
					isWorking = true
					index := client.Bulk()
					for i := 0; i < len(docs); i++ {
						if docs[i] != nil {
							index.Add(elastic.NewBulkIndexRequest().Index(indexName).Doc(docs[i]))
						}
					}

					// st := time.Now()
					_, err = index.Do(rCtx)
					if err != nil {
						logger.Errorf("Cannot insert bulk: %v", err)
					}
					// logger.Infof("Duration: %v", time.Since(st).Seconds())

					atomic.AddUint32(&counter, uint32(len(docs)))
					isWorking = false
				case <-ctx.Done():
					if isWorking {
						logger.Infof("Wait for completion")
						time.Sleep(1 * time.Minute)
					}
					break
				}
			}
		}(rCtx, esClient)
		logger.Infof("Start worker %v", i+1)
	}

	f, err := os.Open(filePath)
	defer f.Close()

	r := csv.NewReader(f)
	r.ReuseRecord = true
	// Skip header
	r.Read()
	buffer := make([]*Trip, 0, bulkImports)

	logger.Info("Push line")
	logger.Infof("Consumer: %v", bulkImports)
	for {
		records, err := r.Read()
		if err != nil {
			if err != io.EOF {
				logger.Error(err)
			}
			break
		}

		if len(buffer) == bulkImports {
			copyBuffer := make([]*Trip, bulkImports)
			copy(copyBuffer, buffer)

			go func(b []*Trip) {
				jobs <- b
			}(copyBuffer)

			buffer = make([]*Trip, 0, bulkImports)
		}

		trip, _ := ConvertFromStringToTrip(records)
		// if err != nil {
		// logger.Errorf("Cannot parse trip: %v", err)
		// }

		buffer = append(buffer, trip)
	}

	jobs <- buffer

	logger.Infof("Done published: %v with %v lines", time.Since(s), totalLines)
	logger.Info("Await...")

	for {
		if counter == totalLines {
			break
		}

		time.Sleep(2 * time.Minute)
		logger.Infof("Process %v/%v", counter, totalLines)
	}
	cancelFunc()

	return
}
