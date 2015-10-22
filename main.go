package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/lib/pq"
	"gopkg.in/olivere/elastic.v2"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
)

// Options
type options struct {
	URI            string `json:"url"`
	MaxBulkActions int    `json:"max_bulk_actions"`
	MaxFetchRows   int    `json:"max_fetch_rows"`
	DB             struct {
		Host     string `json:"host"`
		Port     int    `json:"port"`
		Database string `json:"database"`
		User     string `json:"user"`
		Password string `json:"password"`
		Table    string `json:"table"`
	} `json:"db"`
	Index    string            `json:"index"`
	Type     string            `json:"type"`
	Mappings []json.RawMessage `json:"mappings"`
}

// Create buffered channel to send inserts through
var indexQ chan string
var status = make(chan int)

// Global counters
var succeded, failed uint64

// Index worker function to insert docs
func index(wg *sync.WaitGroup, opts options) {

	// Connect client
	client, err := elastic.NewClient(elastic.SetURL(opts.URI), elastic.SetSniff(false))

	if err != nil {
		log.Fatalln(err.Error())
	}

	// Create new bulk service request
	bulkService := elastic.NewBulkService(client).Index(opts.Index).Type(opts.Type)
	for doc := range indexQ {

		//Add index to request
		bIndex := elastic.NewBulkIndexRequest().Index(opts.Index).OpType("create").Doc(doc)
		bulkService.Add(bIndex)

		// Send request after MaxBulkActions limit is reached
		if bulkService.NumberOfActions() > opts.MaxBulkActions-1 {
			bulkResponse, _ := bulkService.Do()
			atomic.AddUint64(&succeded, uint64(len(bulkResponse.Succeeded())))
			atomic.AddUint64(&failed, uint64(len(bulkResponse.Failed())))
			status <- 1
		}

	}

	wg.Done()
}

func empty(str string) bool {
	return len(str) == 0
}

// Make sure all required options are passed
func check(opts options) error {

	if empty(opts.Index) {
		return errors.New("No elastic index found in options.")
	}

	if empty(opts.Type) {
		return errors.New("No elastic type found in options.")
	}

	if empty(opts.URI) {
		return errors.New("No elastic url found in options.")
	}

	if empty(opts.DB.Host) {
		return errors.New("No postgres host found in options.")
	}

	if opts.DB.Port == 0 {
		return errors.New("No postgres dataportbase found in options.")
	}

	if empty(opts.DB.User) {
		return errors.New("No postgres user found in options.")
	}

	if empty(opts.DB.Database) {
		return errors.New("No postgres database found in options.")
	}

	if empty(opts.DB.Password) {
		return errors.New("No postgres password found in options.")
	}

	if empty(opts.DB.Table) {
		return errors.New("No table found in options.")
	}

	return nil
}

func print() {
	s := atomic.LoadUint64(&succeded)
	f := atomic.LoadUint64(&failed)
	fmt.Printf("\rSucceded: %d Failed: %d", s, f)
}

func setup(opts options) {
	// Connect client
	client, err := elastic.NewClient(elastic.SetURL(opts.URI), elastic.SetSniff(false))

	if err != nil {
		log.Fatalln(err.Error())
	}

	// Create index if does not exist
	exs := client.IndexExists(opts.Index)
	if ok, err := exs.Do(); !ok {
		if err != nil {
			log.Fatalln(err.Error())
		}

		newIndex := elastic.NewIndexService(client).Index(opts.Index)
		_, err := newIndex.Do()
		if err != nil {
			log.Fatalln(err.Error())
		}
	}

	//Create mappings
	for _, mapping := range opts.Mappings {

		// Send raw json from options
		_, err := client.PutMapping().Index(opts.Index).Type(opts.Type).BodyString(string(mapping)).Do()
		if err != nil {
			log.Fatalln(err.Error())
		}
	}

	client.Stop()

}

func main() {

	if len(os.Args) < 2 {
		log.Fatalln("Usage: postgres2elasticsearch <config.json> [number of workers]")
	}

	workers := 1
	if len(os.Args) >= 3 {
		var err error
		workers, err = strconv.Atoi(os.Args[2])
		if err != nil {
			log.Fatalln(err.Error())
		}
	}

	//Load Input File
	file, err := ioutil.ReadFile(os.Args[1])
	if err != nil {
		log.Fatalln(err.Error())
	}

	// Load options
	var opts options
	if err := json.Unmarshal(file, &opts); err != nil {
		log.Fatalln(err.Error())
	}

	// Check options for required
	if err := check(opts); err != nil {
		log.Fatalln(err.Error())
	}

	if opts.MaxBulkActions == 0 {
		opts.MaxBulkActions = 1000 // Default to 1000 insert actions at one time
	}

	limit := "ALL"
	if opts.MaxFetchRows > 0 {
		limit = strconv.Itoa(opts.MaxFetchRows)
	}

	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%d dbname=%s user=%s sslmode=disable password=%s", opts.DB.Host, opts.DB.Port, opts.DB.Database, opts.DB.User, opts.DB.Password))
	if err != nil {
		log.Fatalln(err.Error())
	}

	// Setup index
	setup(opts)

	wg := new(sync.WaitGroup)

	indexQ = make(chan string, opts.MaxBulkActions*workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go index(wg, opts)
	}

	go func() {
		for {
			<-status
			print()
		}
	}()

	//Postgres go library doesn't allow dynamic table placeholders
	statement := fmt.Sprintf("SELECT row_to_json(t) FROM %s as t LIMIT %s", opts.DB.Table, limit)
	rows, err := db.Query(statement)
	if err != nil {
		log.Fatalln(err.Error())
	}
	defer rows.Close()

	for rows.Next() {

		var doc string
		if err := rows.Scan(&doc); err != nil {
			fmt.Println(err)
		}

		indexQ <- doc

	}
	if err := rows.Err(); err != nil {
		log.Fatalln(err.Error())
	}

	close(indexQ)

	wg.Wait()

	print() // Print last update
}
