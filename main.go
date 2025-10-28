package main

import (
	"log"

	as "github.com/aerospike/aerospike-client-go/v6"
)

func main() {
	// Connect to the Aerospike server
	client, err := as.NewClient("localhost", 3000)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Create a map of features to demonstrate
	features := map[string]func(*as.Client){
		"TTL":               demonstrateTTL,
		"Batch Operations":  demonstrateBatchOperations,
		"List Operations":   demonstrateListOperations,
		"Secondary Index":   demonstrateSecondaryIndex,
	}

	// iterate and run each feature demonstration
	for name, featureFunc := range features {
		log.Printf("Demonstrating feature: %s", name)
		featureFunc(client)
	}
}