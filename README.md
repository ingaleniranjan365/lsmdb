# ChatGPT vs Me : Key-Value Database Implementation in Java

## Problem Statement

Implement a generic key-value database in Java that fulfills the following requirements:

### PUT /element/:id/timestamp/:timestamp

Endpoint for inserting data (key-value pair) into the database.

- `id`: A path parameter that can be any string (e.g., id0005214), which serves as the key.
- `timestamp`: A timestamp in the format "yyyy-MM-dd'T'HH:mm:ss'Z'" (e.g., 2023-07-31T12:42:59Z"), which corresponds to the timestamp for the value being sent.
- Value: The value for the key `id` should be sent as a request body in JSON format.
- Request body must not exceed 1000 bytes.

Request body example:
```json
{
	"vendor_id": 1,
	"passenger_count": 1,
	"pickup_longitude": -74.00548553466797,
	"pickup_latitude": 40.72861862182617,
	"id": "id0015493",
	"pickup_datetime": "2016-06-12T05:11:44Z",
	"store_and_fwd_flag": "N",
	"trip_duration": "2451",
	"dropoff_longitude": -73.9604045545196,
	"dropoff_latitude": 40.76793310379328,
	"dropoff_datetime": "2016-01-03T19:53:41Z",
	"lon": -73.9604045545196,
	"lat": 40.76793310379328,
	"timestamp": "2016-01-03T19:53:41Z"
}
```

## GET /latest/element/:id

Endpoint for retrieving the latest value (value with the latest timestamp) associated with the given key.

- `id`: Path parameter representing the key.

If the database receives a value with an older timestamp for an existing key, it should ignore the received value and not replace the existing value with a more recent timestamp.

The database should handle a load of 100 write or insert requests per second continuously for one hour and 100 read or get requests per second continuously for one hour.

For each key stored in the database, there might be 1 to 200 requests with values at different timestamps. The order of receiving values is not necessarily in chronological order.

The database should persist data across restarts.

## ChatGPT Implementation

This implementation done by ChatGPT consists of a Java program divided into two main classes:

- `KeyValueDatabaseApp`: The main class that sets up the Spark server and handles the REST endpoints.
- `KeyValueDatabase`: The class that implements the key-value database with support for storing and retrieving data.

## My Implementation

I have implemented database as log structure merge trees

## Testing 

Tests are done using gatling - refer to repo https://github.com/ingaleniranjan365/key-value-database-performance-testing

## Running the Application

1. Clone or download this repository to your local machine.
2. Open a terminal and navigate to the project directory.
3. Compile the Java code using your preferred method.
4. Run the compiled Java program.

## Dockerization

A Dockerfile is provided in the repository to containerize the application. To build and run the Docker container:

1. Install Docker on your machine.
2. Open a terminal and navigate to the project directory.
3. Build the Docker image using the following command: docker build -t key-value-database .
4. Run the Docker container with the following command: docker run -p 8080:8080 --memory=1g --cpus=4 --name key-value-container key-value-database

Conclusion

This key-value database implementation showcases how to handle various REST endpoints and requirements while providing a generic key-value storage solution in Java. It can handle insert and retrieval requests efficiently, ensuring data integrity and persistence.
