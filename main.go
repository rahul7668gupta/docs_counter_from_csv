package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoDB connection details
const (
	mongoURI       = ""
	databaseName   = "db_name"
	collectionName = "collection_name"
)

func main() {
	// Connect to MongoDB
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(context.Background())

	// Open the CSV file
	csvFile, err := os.Open("input.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer csvFile.Close()

	// Parse the CSV file
	reader := csv.NewReader(csvFile)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	// Open a new CSV file for writing
	outputFile, err := os.Create("output.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer outputFile.Close()

	// Create a CSV writer
	writer := csv.NewWriter(outputFile)
	defer writer.Flush()

	// Write header to the output CSV file
	header := append(records[0], "count")

	if err := writer.Write(header); err != nil {
		log.Fatal(err)
	}

	// Iterate over each row in the CSV file
	for index, row := range records[1:] {
		timeNow := time.Now()
		// Extract the type from the row
		typeVar := row[getColumnIndex("type", records[0])]
		if !strings.EqualFold(typeVar, "NFT_COLLECTION") {
			continue
		}

		// Extract the address from the row
		address := row[getColumnIndex("address", records[0])]

		// Perform MongoDB count documents call
		count, err := getCountFromMongoDB(client, address)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Processing row: ", index, "address: ", address, "count: ", count, "time: ", time.Since(timeNow))
		// Append the count to the row
		row = append(row, strconv.Itoa(count))

		// Write the updated row to the output CSV file
		if err := writer.Write(row); err != nil {
			log.Fatal(err)
		}
	}

	fmt.Println("Processing complete. Results written to output.csv")
}

// getColumnIndex returns the index of the specified column in the header row
func getColumnIndex(columnName string, header []string) int {
	for i, col := range header {
		if strings.EqualFold(col, columnName) {
			return i
		}
	}
	return -1
}

// getCountFromMongoDB performs a count documents call in MongoDB
func getCountFromMongoDB(client *mongo.Client, address string) (int, error) {
	collection := client.Database(databaseName).Collection(collectionName)

	// Construct a filter based on the address
	filter := bson.M{"address": strings.ToLower(address)}

	// Perform the count documents call
	count, err := collection.CountDocuments(context.Background(), filter)
	if err != nil {
		return 0, err
	}

	return int(count), nil
}
