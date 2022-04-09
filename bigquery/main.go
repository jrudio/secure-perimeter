package main

import (
	"context"
	"fmt"
	"os"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

func main() {
	// get project id from environment variable
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")

	if projectID == "" {
		fmt.Println("GOOGLE_CLOUD_PROJECT environment variable must be set.")
		os.Exit(1)
	}

	// set up the bigquery client
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)

	if err != nil {
		fmt.Printf("Failed to create client: %v\n", err)
		os.Exit(1)
	}

	defer client.Close()

	// get the first user
	q := client.Query("SELECT (username, password, is_admin) FROM `" + projectID + ".test_dataset.protected-data` LIMIT 10")

	q.Location = "us-west1"

	job, err := q.Run(ctx)

	if err != nil {
		fmt.Printf("query execution failed: %v\n", err)
		os.Exit(1)
	}

	status, err := job.Wait(ctx)

	if err != nil {
		fmt.Printf("job wait failed: %v\n", err)
		os.Exit(1)
	}

	if err := status.Err(); err != nil {
		fmt.Printf("job failed: %v\n", err)
		os.Exit(1)
	}

	it, err := job.Read(ctx)

	if err != nil {
		fmt.Printf("reading job failed: %v\n", err)
		os.Exit(1)
	}

	// print the user's name
	for {
		var row []bigquery.Value

		if err := it.Next(&row); err == iterator.Done {
			break
		} else if err != nil {
			fmt.Printf("Failed to iterate through results: %v\n", err)
			os.Exit(1)
		}

		// fmt.Printf("%v\n", row)
		// fmt.Printf("username: %s, password: %s, is_admin: %t\n", vals)
	}

}
