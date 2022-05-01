package main

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

// The following is a sample record from the Taxi Trips dataset retrieved from the City of Chicago Data Portal

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

// trip_id	"c354c843908537bbf90997917b714f1c63723785"
// trip_start_timestamp	"2021-11-13T22:45:00.000"
// trip_end_timestamp	"2021-11-13T23:00:00.000"
// trip_seconds	"703"
// trip_miles	"6.83"
// pickup_census_tract	"17031840300"
// dropoff_census_tract	"17031081800"
// pickup_community_area	"59"
// dropoff_community_area	"8"
// fare	"27.5"
// tip	"0"
// additional_charges	"1.02"
// trip_total	"28.52"
// shared_trip_authorized	false
// trips_pooled	"1"
// pickup_centroid_latitude	"41.8335178865"
// pickup_centroid_longitude	"-87.6813558293"
// pickup_centroid_location
// type	"Point"
// coordinates
// 		0	-87.6813558293
// 		1	41.8335178865
// dropoff_centroid_latitude	"41.8932163595"
// dropoff_centroid_longitude	"-87.6378442095"
// dropoff_centroid_location
// type	"Point"
// coordinates
// 		0	-87.6378442095
// 		1	41.8932163595
////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"database/sql"
	"encoding/json"

	"github.com/kelvins/geocoder"
	_ "github.com/lib/pq"
)

// Include both taxi trips and the provider trips
type TaxiTripsJsonRecords []struct {
	Trip_id                    string `json:"trip_id"`
	Trip_start_timestamp       string `json:"trip_start_timestamp"`
	Trip_end_timestamp         string `json:"trip_end_timestamp"`
	Pickup_centroid_latitude   string `json:"pickup_centroid_latitude"`
	Pickup_centroid_longitude  string `json:"pickup_centroid_longitude"`
	Dropoff_centroid_latitude  string `json:"dropoff_centroid_latitude"`
	Dropoff_centroid_longitude string `json:"dropoff_centroid_longitude"`
}

type UnemploymentJsonRecords []struct {
	Community_area      string `json:"community_area"`
	Community_area_name string `json:"community_area_name"`
	Per_capita_income   string `json:"per_capita_income"`
	Unemployment        string `json:"unemployment"`
}

type BuidngPermitJsonRecords []struct {
	Bp_id          string `json:"id"`
	Permit_        string `json:"permit_"`
	Permit_type    string `json:"permit_type"`
	Reported_cost  string `json:"reported_cost"`
	Community_area string `json:"community_area"`
	Latitude       string `json:"latitude"`
	Longitude      string `json:"longitude"`
}

type CovidZipCodeJsonRecords []struct {
	Zip_code                           string `json:"zip_code"`
	Week_number                        string `json:"week_number"`
	Week_start                         string `json:"week_start"`
	Week_end                           string `json:"week_end"`
	Cases_weekly                       string `json:"cases_weekly"`
	Cases_cumulative                   string `json:"cases_cumulative"`
	Percent_tested_positive_weekly     string `json:"percent_tested_positive_weekly"`
	Percent_tested_positive_cumulative string `json:"percent_tested_positive_cumulative"`
	Row_id                             string `json:"row_id"`
}

type CovidDailyJsonRecords []struct {
	Lab_report_date        string `json:"lab_report_date"`
	Cases_total            string `json:"cases_total"`
	Deaths_total           string `json:"deaths_total"`
	Hospitalizations_total string `json:"hospitalizations_total"`
}

type CovidCCVIJsonRecords []struct {
	Geography_type        string `json:"geography_type"`
	Community_area_or_zip string `json:"community_area_or_zip"`
	Community_area_name   string `json:"community_area_name"`
	Ccvi_score            string `json:"ccvi_score"`
	Ccvi_category         string `json:"ccvi_category"`
}

type neighborhoodCommunity []struct {
	Community    string `json:"community"`
	Neighborhood string `json:"neighborhood"`
}

type zipCodeNeighborhood []struct {
	Zipcode      int    `json:"zipcode"`
	Neighborhood string `json:"neighborhood"`
}

func main() {

	// Establish connection to Postgres Database
	// db_connection := "user=postgres dbname=cbi_datalake password=19920420 host=localhost sslmode=disable"

	// Docker image for the microservice - uncomment when deploy
	//db_connection := "user=postgres dbname=chicago_business_intelligence password=root host=host.docker.internal sslmode=disable port = 5433"
	//Option 4
	//Database application running on Google Cloud Platform.
	db_connection := "user=postgres dbname=chicago_business_intelligence password=root host=/cloudsql/chicago-business-intelligence9:us-central1:mypostgres1 sslmode=disable port = 5432"

	db, err := sql.Open("postgres", db_connection)
	if err != nil {
		panic(err)
	}

	// Test the database connection
	err = db.Ping()
	if err != nil {
		fmt.Println("Couldn't Connect to database")
		panic(err)
	}
	//GetTaxiTrips(db)
	//GetUnemploymentRates(db)
	//GetBuildingPermits(db)
	//GetCovidZipCode(db)
	//GetCovidDaily(db)
	//GetCovidCCVI(db)
	// Spin in a loop and pull data from the city of chicago data portal
	// Once every hour, day, week, etc.
	// Though, please note that Not all datasets need to be pulled on daily basis
	// fine-tune the following code-snippet as you see necessary
	for {
		GetTaxiTrips(db)
		GetUnemploymentRates(db)
		GetBuildingPermits(db)
		GetCovidZipCode(db)
		GetCovidDaily(db)
		GetCovidCCVI(db)
		// Pull the data once a day
		// You might need to pull Taxi Trips and COVID data on daily basis
		// but not the unemployment dataset becasue its dataset doesn't change every day
		time.Sleep(24 * time.Hour)
	}

}

func GetTaxiTrips(db *sql.DB) {

	// This function is NOT complete
	// It provides code-snippets for the data source: https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew
	// You need to complete the implmentation and add the data source: https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips/m6dm-c72p

	// Data Collection needed from two data sources:
	// 1. https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew
	// 2. https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips/m6dm-c72p

	fmt.Println("GetTaxiTrips: Collecting Taxi Trips Data")

	// Get your geocoder.ApiKey from here :
	// https://developers.google.com/maps/documentation/geocoding/get-api-key?authuser=2

	geocoder.ApiKey = "AIzaSyCqf4yo1mo2l0HoJ394SeH14YAF6B9SHpA"
	fmt.Println("GetTaxiTrips: Creating database table: as5_taxi_trips")
	drop_table := `drop table if exists as5_taxi_trips`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "as5_taxi_trips" (
						"id"   SERIAL , 
						"trip_id" VARCHAR(255) UNIQUE, 
						"trip_start_timestamp" TIMESTAMP WITH TIME ZONE, 
						"trip_end_timestamp" TIMESTAMP WITH TIME ZONE, 
						"pickup_centroid_latitude" DOUBLE PRECISION, 
						"pickup_centroid_longitude" DOUBLE PRECISION, 
						"dropoff_centroid_latitude" DOUBLE PRECISION, 
						"dropoff_centroid_longitude" DOUBLE PRECISION, 
						"pickup_zip_code" VARCHAR(255), 
						"dropoff_zip_code" VARCHAR(255), 
						PRIMARY KEY ("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	// While doing unit-testing keep the limit value to 500
	// later you could change it to 1000, 2000, 10,000, etc.
	fmt.Println("GetTaxiTrips: Getting taxi data from soda api")
	var url = "https://data.cityofchicago.org/resource/wrvz-psew.json?$limit=500"

	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}

	body, _ := ioutil.ReadAll(res.Body)
	var taxi_trips_list TaxiTripsJsonRecords
	json.Unmarshal(body, &taxi_trips_list)
	fmt.Println("GetTaxiTrips: Writing data to database table")
	for i := 0; i < len(taxi_trips_list); i++ {

		// We will execute definsive coding to check for messy/dirty/missing data values
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		trip_id := taxi_trips_list[i].Trip_id
		if trip_id == "" {
			continue
		}

		// if trip start/end timestamp doesn't have the length of 23 chars in the format "0000-00-00T00:00:00.000"
		// skip this record

		// get Trip_start_timestamp
		trip_start_timestamp := taxi_trips_list[i].Trip_start_timestamp
		if len(trip_start_timestamp) < 23 {
			continue
		}

		// get Trip_end_timestamp
		trip_end_timestamp := taxi_trips_list[i].Trip_end_timestamp
		if len(trip_end_timestamp) < 23 {
			continue
		}

		pickup_centroid_latitude := taxi_trips_list[i].Pickup_centroid_latitude

		if pickup_centroid_latitude == "" {
			continue
		}

		pickup_centroid_longitude := taxi_trips_list[i].Pickup_centroid_longitude
		//pickup_centroid_longitude := taxi_trips_list[i].PICKUP_LONG

		if pickup_centroid_longitude == "" {
			continue
		}

		dropoff_centroid_latitude := taxi_trips_list[i].Dropoff_centroid_latitude
		//dropoff_centroid_latitude := taxi_trips_list[i].DROPOFF_LAT

		if dropoff_centroid_latitude == "" {
			continue
		}

		dropoff_centroid_longitude := taxi_trips_list[i].Dropoff_centroid_longitude
		//dropoff_centroid_longitude := taxi_trips_list[i].DROPOFF_LONG

		if dropoff_centroid_longitude == "" {
			continue
		}

		// Using pickup_centroid_latitude and pickup_centroid_longitude in geocoder.GeocodingReverse
		// we could find the pickup zip-code

		pickup_centroid_latitude_float, _ := strconv.ParseFloat(pickup_centroid_latitude, 64)
		pickup_centroid_longitude_float, _ := strconv.ParseFloat(pickup_centroid_longitude, 64)
		pickup_location := geocoder.Location{
			Latitude:  pickup_centroid_latitude_float,
			Longitude: pickup_centroid_longitude_float,
		}

		pickup_address_list, _ := geocoder.GeocodingReverse(pickup_location)
		pickup_address := pickup_address_list[0]
		pickup_zip_code := pickup_address.PostalCode

		// Using dropoff_centroid_latitude and dropoff_centroid_longitude in geocoder.GeocodingReverse
		// we could find the dropoff zip-code

		dropoff_centroid_latitude_float, _ := strconv.ParseFloat(dropoff_centroid_latitude, 64)
		dropoff_centroid_longitude_float, _ := strconv.ParseFloat(dropoff_centroid_longitude, 64)

		dropoff_location := geocoder.Location{
			Latitude:  dropoff_centroid_latitude_float,
			Longitude: dropoff_centroid_longitude_float,
		}

		dropoff_address_list, _ := geocoder.GeocodingReverse(dropoff_location)
		dropoff_address := dropoff_address_list[0]
		dropoff_zip_code := dropoff_address.PostalCode

		sql := `INSERT INTO as5_taxi_trips ("trip_id", "trip_start_timestamp", "trip_end_timestamp", "pickup_centroid_latitude", "pickup_centroid_longitude", "dropoff_centroid_latitude", "dropoff_centroid_longitude", "pickup_zip_code", 
			"dropoff_zip_code") values($1, $2, $3, $4, $5, $6, $7, $8, $9)`

		_, err = db.Exec(
			sql,
			trip_id,
			trip_start_timestamp,
			trip_end_timestamp,
			pickup_centroid_latitude,
			pickup_centroid_longitude,
			dropoff_centroid_latitude,
			dropoff_centroid_longitude,
			pickup_zip_code,
			dropoff_zip_code)

		if err != nil {
			panic(err)
		}
	}

	fmt.Println("GetTaxiTrips: Getting provider data from soda api")
	var url1 = "https://data.cityofchicago.org/resource/m6dm-c72p.json?$limit=500"

	res1, err1 := http.Get(url1)
	if err1 != nil {
		panic(err1)
	}

	body1, _ := ioutil.ReadAll(res1.Body)
	var provider_trips_list TaxiTripsJsonRecords
	json.Unmarshal(body1, &provider_trips_list)
	fmt.Println("GetTaxiTrips: Writing data to database table")
	for i := 0; i < len(provider_trips_list); i++ {

		// We will execute definsive coding to check for messy/dirty/missing data values
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		trip_id := provider_trips_list[i].Trip_id
		if trip_id == "" {
			continue
		}

		// if trip start/end timestamp doesn't have the length of 23 chars in the format "0000-00-00T00:00:00.000"
		// skip this record

		// get Trip_start_timestamp
		trip_start_timestamp := provider_trips_list[i].Trip_start_timestamp
		if len(trip_start_timestamp) < 23 {
			continue
		}

		// get Trip_end_timestamp
		trip_end_timestamp := provider_trips_list[i].Trip_end_timestamp
		if len(trip_end_timestamp) < 23 {
			continue
		}

		pickup_centroid_latitude := provider_trips_list[i].Pickup_centroid_latitude

		if pickup_centroid_latitude == "" {
			continue
		}

		pickup_centroid_longitude := provider_trips_list[i].Pickup_centroid_longitude
		//pickup_centroid_longitude := taxi_trips_list[i].PICKUP_LONG

		if pickup_centroid_longitude == "" {
			continue
		}

		dropoff_centroid_latitude := provider_trips_list[i].Dropoff_centroid_latitude
		//dropoff_centroid_latitude := taxi_trips_list[i].DROPOFF_LAT

		if dropoff_centroid_latitude == "" {
			continue
		}

		dropoff_centroid_longitude := provider_trips_list[i].Dropoff_centroid_longitude
		//dropoff_centroid_longitude := taxi_trips_list[i].DROPOFF_LONG

		if dropoff_centroid_longitude == "" {
			continue
		}

		// Using pickup_centroid_latitude and pickup_centroid_longitude in geocoder.GeocodingReverse
		// we could find the pickup zip-code

		pickup_centroid_latitude_float, _ := strconv.ParseFloat(pickup_centroid_latitude, 64)
		pickup_centroid_longitude_float, _ := strconv.ParseFloat(pickup_centroid_longitude, 64)
		pickup_location := geocoder.Location{
			Latitude:  pickup_centroid_latitude_float,
			Longitude: pickup_centroid_longitude_float,
		}

		pickup_address_list, _ := geocoder.GeocodingReverse(pickup_location)
		pickup_address := pickup_address_list[0]
		pickup_zip_code := pickup_address.PostalCode

		// Using dropoff_centroid_latitude and dropoff_centroid_longitude in geocoder.GeocodingReverse
		// we could find the dropoff zip-code

		dropoff_centroid_latitude_float, _ := strconv.ParseFloat(dropoff_centroid_latitude, 64)
		dropoff_centroid_longitude_float, _ := strconv.ParseFloat(dropoff_centroid_longitude, 64)

		dropoff_location := geocoder.Location{
			Latitude:  dropoff_centroid_latitude_float,
			Longitude: dropoff_centroid_longitude_float,
		}

		dropoff_address_list, _ := geocoder.GeocodingReverse(dropoff_location)
		dropoff_address := dropoff_address_list[0]
		dropoff_zip_code := dropoff_address.PostalCode

		sql := `INSERT INTO as5_taxi_trips ("trip_id", "trip_start_timestamp", "trip_end_timestamp", "pickup_centroid_latitude", "pickup_centroid_longitude", "dropoff_centroid_latitude", "dropoff_centroid_longitude", "pickup_zip_code", 
			"dropoff_zip_code") values($1, $2, $3, $4, $5, $6, $7, $8, $9)`

		_, err = db.Exec(
			sql,
			trip_id,
			trip_start_timestamp,
			trip_end_timestamp,
			pickup_centroid_latitude,
			pickup_centroid_longitude,
			dropoff_centroid_latitude,
			dropoff_centroid_longitude,
			pickup_zip_code,
			dropoff_zip_code)

		if err != nil {
			panic(err)
		}
	}

	fmt.Println("GetTaxiTrips: all done")
}

func GetUnemploymentRates(db *sql.DB) {
	fmt.Println("GetUnemploymentRates: Collecting Unemployment Rates Data")
	drop_table := `drop table if exists as5_health_unemployment`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "as5_health_unemployment" (
		"id"   SERIAL , 
		"community_area" INTEGER,
		"community_area_name" VARCHAR(255),
		"neighborhood" VARCHAR(255),
		"per_capita_income" INTEGER,
		"unemployment" DOUBLE PRECISION, 
		PRIMARY KEY ("id") 
	);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}
	// While doing unit-testing keep the limit value to 500
	// later you could change it to 1000, 2000, 10,000, etc.
	fmt.Println("GetUnemploymentRates: Getting unemployment data from soda api")
	var url = "https://data.cityofchicago.org/resource/iqnk-2tcu.json?$limit=500"

	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}

	body, _ := ioutil.ReadAll(res.Body)
	var health_unemployment_list UnemploymentJsonRecords
	json.Unmarshal(body, &health_unemployment_list)

	// Import neighborhood name to cummunity name table
	file, _ := ioutil.ReadFile("neighborhoodCommunity.json")
	var neighborhoodCommunity_list neighborhoodCommunity
	json.Unmarshal([]byte(file), &neighborhoodCommunity_list)

	fmt.Println("GetUnemploymentRates: Writing data to database table")
	var neighborhood string
	//fmt.Println(neighborhoodCommunity_list)
	for i := 0; i < len(health_unemployment_list); i++ {

		// We will execute definsive coding to check for messy/dirty/missing data values
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table
		//fmt.Println(health_unemployment_list[i])
		community_area := health_unemployment_list[i].Community_area
		if community_area == "" {
			continue
		}

		community_area_name := health_unemployment_list[i].Community_area_name
		if community_area_name == "" {
			continue
		}
		// Match neighborhood name with cummunity data
		neighborhood = ""
		for j := 0; j < len(neighborhoodCommunity_list); j++ {
			//fmt.Println(neighborhoodCommunity_list[j].Community)
			if strings.ToLower(community_area_name) == strings.ToLower(neighborhoodCommunity_list[j].Community) {
				neighborhood = neighborhoodCommunity_list[j].Neighborhood
			}
		}
		per_capita_income := health_unemployment_list[i].Per_capita_income

		unemployment := health_unemployment_list[i].Unemployment
		if unemployment == "" {
			continue
		}

		sql := `INSERT INTO as5_health_unemployment ("community_area", "community_area_name", "neighborhood", "per_capita_income", "unemployment") values($1, $2, $3, $4, $5)`

		_, err = db.Exec(
			sql,
			community_area,
			community_area_name,
			neighborhood,
			per_capita_income,
			unemployment)

		if err != nil {
			panic(err)
		}
	}

	fmt.Println("GetUnemploymentRates: Implemented Unemployment data done.")
}

func GetBuildingPermits(db *sql.DB) {
	fmt.Println("GetBuildingPermits: Collecting Building Permits Data")
	geocoder.ApiKey = "AIzaSyCqf4yo1mo2l0HoJ394SeH14YAF6B9SHpA"
	fmt.Println("GetBuildingPermits: Creating database table: as5_building_permit")
	drop_table := `drop table if exists as5_building_permit`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "as5_building_permit" (
						"id"   SERIAL , 
						"bp_id" VARCHAR(255) UNIQUE, 
						"permit_" VARCHAR(255), 
						"permit_type" VARCHAR(255), 
						"reported_cost" DOUBLE PRECISION, 
						"community_area" INTEGER, 
						"latitude" DOUBLE PRECISION, 
						"longitude" DOUBLE PRECISION, 
						"zip_code" VARCHAR(255),  
						PRIMARY KEY ("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	// While doing unit-testing keep the limit value to 500
	// later you could change it to 1000, 2000, 10,000, etc.
	fmt.Println("GetBuildingPermits: Getting building permit data from soda api")
	var url = "https://data.cityofchicago.org/resource/ydr8-5enu.json?$limit=500"

	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}

	body, _ := ioutil.ReadAll(res.Body)
	var building_permit_list BuidngPermitJsonRecords
	json.Unmarshal(body, &building_permit_list)
	fmt.Println("GetBuildingPermits: Writing data to database table")
	for i := 0; i < len(building_permit_list); i++ {

		// We will execute definsive coding to check for messy/dirty/missing data values
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table
		//fmt.Println(building_permit_list[i].Permit_type)
		bp_id := building_permit_list[i].Bp_id

		// If no permit data or permit type is not new construction then skip it
		permit_ := building_permit_list[i].Permit_
		if permit_ == "" {
			continue
		}

		permit_type := building_permit_list[i].Permit_type
		if permit_type == "" {
			continue
		}

		if strings.ToLower(permit_type) != strings.ToLower("PERMIT - NEW CONSTRUCTION") {
			continue
		}

		reported_cost := building_permit_list[i].Reported_cost

		community_area := building_permit_list[i].Community_area

		latitude := building_permit_list[i].Latitude
		if latitude == "" {
			continue
		}
		longitude := building_permit_list[i].Longitude
		if longitude == "" {
			continue
		}

		// Using pickup_centroid_latitude and pickup_centroid_longitude in geocoder.GeocodingReverse
		// we could find the pickup zip-code

		latitude_float, _ := strconv.ParseFloat(latitude, 64)
		longitude_float, _ := strconv.ParseFloat(longitude, 64)

		bld_location := geocoder.Location{
			Latitude:  latitude_float,
			Longitude: longitude_float,
		}

		address_list, _ := geocoder.GeocodingReverse(bld_location)
		address := address_list[0]
		zip_code := address.PostalCode

		sql := `INSERT INTO as5_building_permit ("bp_id", "permit_", "permit_type", "reported_cost", "community_area", "latitude", "longitude", "zip_code") values($1, $2, $3, $4, $5, $6, $7, $8)`

		_, err = db.Exec(
			sql,
			bp_id,
			permit_,
			permit_type,
			reported_cost,
			community_area,
			latitude,
			longitude,
			zip_code)

		if err != nil {
			panic(err)
		}
	}

	fmt.Println("GetBuildingPermits: Implement Building Permits")
}

func GetCovidZipCode(db *sql.DB) {
	fmt.Println("GetCovidZipCode: Collecting Covid-19 zipcode Data")

	fmt.Println("GetCovidZipCode: Creating database table: as5_covid_zip_code")
	drop_table := `drop table if exists as5_covid_zip_code`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "as5_covid_zip_code" (
						"id"   SERIAL , 
						"row_id" VARCHAR(255) UNIQUE,
						"zip_code" VARCHAR(255), 
						"week_number" INTEGER, 
						"week_start" DATE, 
						"week_end" DATE, 
						"cases_weekly" INTEGER, 
						"cases_cumulative" INTEGER, 
						"percent_tested_positive_weekly" DOUBLE PRECISION,
						"percent_tested_positive_cumulative" DOUBLE PRECISION,
						PRIMARY KEY ("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	// While doing unit-testing keep the limit value to 500
	// later you could change it to 1000, 2000, 10,000, etc.
	fmt.Println("GetCovidZipCode: Getting building permit data from soda api")
	var url = "https://data.cityofchicago.org/resource/yhhz-zm2v.json"

	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}

	body, _ := ioutil.ReadAll(res.Body)
	var covid_zipcode_list CovidZipCodeJsonRecords
	json.Unmarshal(body, &covid_zipcode_list)
	fmt.Println("GetCovidZipCode: Writing data to database table")
	for i := 0; i < len(covid_zipcode_list); i++ {

		// We will execute definsive coding to check for messy/dirty/missing data values
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table
		//fmt.Println(building_permit_list[i].Permit_type)
		row_id := covid_zipcode_list[i].Row_id
		if row_id == "" {
			continue
		}
		// If no permit data or permit type is not new construction then skip it
		zip_code := covid_zipcode_list[i].Zip_code
		if zip_code == "" {
			continue
		}

		week_number := covid_zipcode_list[i].Week_number
		if week_number == "" {
			continue
		}

		week_start := covid_zipcode_list[i].Week_start
		if week_start == "" {
			continue
		}

		week_end := covid_zipcode_list[i].Week_end
		if week_end == "" {
			continue
		}

		cases_weekly := covid_zipcode_list[i].Cases_weekly
		if cases_weekly == "" {
			continue
		}
		cases_cumulative := covid_zipcode_list[i].Cases_cumulative
		percent_tested_positive_weekly := covid_zipcode_list[i].Percent_tested_positive_weekly
		percent_tested_positive_cumulative := covid_zipcode_list[i].Percent_tested_positive_cumulative

		sql := `INSERT INTO as5_covid_zip_code ("row_id", "zip_code", "week_number", "week_start", "week_end", "cases_weekly", "cases_cumulative", "percent_tested_positive_weekly", "percent_tested_positive_cumulative") values($1, $2, $3, $4, $5, $6, $7, $8, $9)`

		_, err = db.Exec(
			sql,
			row_id,
			zip_code,
			week_number,
			week_start,
			week_end,
			cases_weekly,
			cases_cumulative,
			percent_tested_positive_weekly,
			percent_tested_positive_cumulative)

		if err != nil {
			panic(err)
		}
	}

	fmt.Println("GetCovidZipCode: Implement covid zip code data, all done.")
}

func GetCovidDaily(db *sql.DB) {
	fmt.Println("GetCovidDaily: Collecting Covid daily Data")
	geocoder.ApiKey = "AIzaSyCqf4yo1mo2l0HoJ394SeH14YAF6B9SHpA"
	fmt.Println("GetCovidDaily: Creating database table: as5_covid_daily")
	drop_table := `drop table if exists as5_covid_daily`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "as5_covid_daily" (
						"id"   SERIAL , 
						"lab_report_date" DATE, 
						"cases_total" INTEGER, 
						"deaths_total" INTEGER, 
						"hospitalizations_total" INTEGER, 
						PRIMARY KEY ("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	// While doing unit-testing keep the limit value to 500
	// later you could change it to 1000, 2000, 10,000, etc.
	fmt.Println("GetCovidDaily: Getting covid daily data from soda api")
	var url = "https://data.cityofchicago.org/resource/naz8-j4nc.json?$limit=500"

	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}

	body, _ := ioutil.ReadAll(res.Body)
	var covid_daily_list CovidDailyJsonRecords
	json.Unmarshal(body, &covid_daily_list)
	fmt.Println("GetCovidDaily: Writing data to database table")
	for i := 0; i < len(covid_daily_list); i++ {

		// We will execute definsive coding to check for messy/dirty/missing data values
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table
		//fmt.Println(building_permit_list[i].Permit_type)
		lab_report_date := covid_daily_list[i].Lab_report_date
		if lab_report_date == "" {
			continue
		}

		cases_total := covid_daily_list[i].Cases_total
		if cases_total == "" {
			continue
		}

		deaths_total := covid_daily_list[i].Hospitalizations_total
		if deaths_total == "" {
			deaths_total = "0"
		}

		hospitalizations_total := covid_daily_list[i].Hospitalizations_total
		if hospitalizations_total == "" {
			hospitalizations_total = "0"
		}

		sql := `INSERT INTO as5_covid_daily ("lab_report_date", "cases_total", "deaths_total", "hospitalizations_total") values($1, $2, $3, $4)`

		_, err = db.Exec(
			sql,
			lab_report_date,
			cases_total,
			deaths_total,
			hospitalizations_total)

		if err != nil {
			panic(err)
		}
	}

	fmt.Println("GetCovidDaily: Implement Covid Daily data, all done")
}

func GetCovidCCVI(db *sql.DB) {
	fmt.Println("GetCovidCCVI: Collecting covid CCVI Data")

	fmt.Println("GetCovidCCVI: Creating database table: as5_covid_ccvi")
	drop_table := `drop table if exists as5_covid_ccvi`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "as5_covid_ccvi" (
						"id"   SERIAL , 
						"geography_type" VARCHAR(255), 
						"community_area_or_zip" VARCHAR(255), 
						"community_area_name" VARCHAR(255), 
						"neighborhood" VARCHAR(255), 
						"ccvi_score" DOUBLE PRECISION, 
						"ccvi_category" VARCHAR(255), 
						PRIMARY KEY ("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	// While doing unit-testing keep the limit value to 500
	// later you could change it to 1000, 2000, 10,000, etc.
	fmt.Println("GetBuildingPermits: Getting building permit data from soda api")
	var url = "https://data.cityofchicago.org/resource/xhc6-88s9.json?$limit=500"

	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}

	body, _ := ioutil.ReadAll(res.Body)
	var covid_ccvi_list CovidCCVIJsonRecords
	json.Unmarshal(body, &covid_ccvi_list)

	// Import neighborhood name to cummunity name table
	file1, _ := ioutil.ReadFile("neighborhoodCommunity.json")
	var neighborhoodCommunity_list1 neighborhoodCommunity
	json.Unmarshal([]byte(file1), &neighborhoodCommunity_list1)

	// Import neighborhood name to cummunity name table
	file2, _ := ioutil.ReadFile("zipCodeNeighborhood.json")
	var zipCodeNeighborhood_list zipCodeNeighborhood
	json.Unmarshal([]byte(file2), &zipCodeNeighborhood_list)

	fmt.Println("GetCovidCCVI: Writing data to database table")
	var neighborhood string
	for i := 0; i < len(covid_ccvi_list); i++ {

		// We will execute definsive coding to check for messy/dirty/missing data values
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table
		//fmt.Println(health_unemployment_list[i])
		geography_type := covid_ccvi_list[i].Geography_type
		if geography_type == "" {
			continue
		}

		community_area_or_zip := covid_ccvi_list[i].Community_area_or_zip

		community_area_name := covid_ccvi_list[i].Community_area_name
		// Remove dirty sign from the data
		community_area_name = strings.Replace(community_area_name, "*", "", -1)

		// Match neighborhood name with cummunity data and zip code neighborhood data
		neighborhood = ""
		if geography_type == "CA" {
			for j := 0; j < len(neighborhoodCommunity_list1); j++ {
				//fmt.Println(neighborhoodCommunity_list[j].Community)
				if strings.ToLower(community_area_name) == strings.ToLower(neighborhoodCommunity_list1[j].Community) {
					neighborhood = neighborhoodCommunity_list1[j].Neighborhood
				}
			}
		}
		if geography_type == "ZIP" {
			for j := 0; j < len(zipCodeNeighborhood_list); j++ {
				//fmt.Println(community_area_or_zip, strings.ToLower(strconv.Itoa(zipCodeNeighborhood_list[j].Zipcode)))
				if strings.ToLower(community_area_or_zip) == strings.ToLower(strconv.Itoa(zipCodeNeighborhood_list[j].Zipcode)) {
					neighborhood = zipCodeNeighborhood_list[j].Neighborhood
				}
			}
		}
		ccvi_score := covid_ccvi_list[i].Ccvi_score

		ccvi_category := covid_ccvi_list[i].Ccvi_category

		sql := `INSERT INTO as5_covid_ccvi ("geography_type", "community_area_or_zip", "community_area_name", "neighborhood", "ccvi_score", "ccvi_category") values($1, $2, $3, $4, $5, $6)`

		_, err = db.Exec(
			sql,
			geography_type,
			community_area_or_zip,
			community_area_name,
			neighborhood,
			ccvi_score,
			ccvi_category)

		if err != nil {
			panic(err)
		}
	}

	fmt.Println("GetCovidCCVI: Implement Building Permits")
}
