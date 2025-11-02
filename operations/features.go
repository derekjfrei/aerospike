package main

import (
	"log"
	"time"

	as "github.com/aerospike/aerospike-client-go/v6"
)

func demonstrateBatchOperations(client *as.Client) {
	log.Println("\n=== Batch Operations ===")
	keys := make([]*as.Key, 3)
	for i := 0; i < 3; i++ {
		keys[i], _ = as.NewKey("test", "demo", i)
		bins := as.BinMap{
			"id":   i,
			"data": "batch-data-" + string(rune('A'+i)),
		}
		if err := client.Put(nil, keys[i], bins); err != nil {
			log.Printf("Error writing batch record %d: %v\n", i, err)
			return
		}
	}

	records, err := client.BatchGet(nil, keys)
	if err != nil {
		log.Printf("Error in batch get: %v\n", err)
		return
	}

	for i, record := range records {
		log.Printf("Batch record %d: %v\n", i, record.Bins)
	}
}

func demonstrateListOperations(client *as.Client) {
	log.Println("\n=== List Operations ===")
	listKey, _ := as.NewKey("test", "demo", "list-demo")

	listBin := as.NewBin("numbers", []interface{}{1, 2, 3})
	err := client.PutBins(nil, listKey, listBin)
	if err != nil {
		log.Printf("Error creating list: %v\n", err)
		return
	}

	ops := []*as.Operation{
		as.ListAppendOp("numbers", 4),
	}
	_, err = client.Operate(nil, listKey, ops...)
	if err != nil {
		log.Printf("Error appending to list: %v\n", err)
		return
	}

	if record, err := client.Get(nil, listKey); err == nil {
		log.Printf("Final list: %v\n", record.Bins["numbers"])
	}
}

func demonstrateSecondaryIndex(client *as.Client) {
	// 4. Secondary Index and Query
	log.Println("\n=== Secondary Index and Query ===")

	// Create a secondary index
	task, err := client.CreateIndex(nil, "test", "demo", "age_index", "age", as.NUMERIC)
	if err != nil {
		log.Printf("Error creating index: %v\n", err)
		// Continue anyway as the index might already exist
	}

	if task != nil {
		// Wait for index creation
		for {
			done, err := task.IsDone()
			if err != nil {
				log.Printf("Error checking index status: %v\n", err)
				break
			}
			if done {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Insert some records
	for i := 20; i < 30; i++ {
		key, _ := as.NewKey("test", "demo", "user-"+string(rune('0'+i-20)))
		bins := as.BinMap{
			"name": "user-" + string(rune('A'+i-20)),
			"age":  i,
		}
		if err := client.Put(nil, key, bins); err != nil {
			log.Printf("Error inserting record: %v\n", err)
			return
		}
	}

	// Query records where age > 25
	stmt := as.NewStatement("test", "demo")
	stmt.SetFilter(as.NewRangeFilter("age", 25, 100))

	recordset, err := client.Query(nil, stmt)
	if err != nil {
		log.Printf("Error querying: %v\n", err)
		return
	}

	// Process query results
	for res := range recordset.Results() {
		if res.Err != nil {
			log.Printf("Error in result: %v\n", res.Err)
			continue
		}
		log.Printf("Query result: %v\n", res.Record.Bins)
	}
}