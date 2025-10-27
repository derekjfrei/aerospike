package main

import (
	"log"
	"time"

	as "github.com/aerospike/aerospike-client-go/v6"
)

func main() {
	// Connect to the Aerospike server
	client, err := as.NewClient("localhost", 3000)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	key, err := as.NewKey("test", "demo", "demo-key")
	if err != nil {
		log.Fatal(err)
	}

	// Write a record
	bins := as.BinMap{
		"greet": "Hello, Aerospike!",
		"time":  time.Now().String(),
	}
	err = client.Put(nil, key, bins)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Record written successfully")

	// Read the record
	record, err := client.Get(nil, key)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Record read successfully: %v\n", record.Bins)
}
