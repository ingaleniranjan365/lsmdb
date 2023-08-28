# Key-Value Database Implementation(s) in Java

## Problem Statement

Implement a generic key-value database in Java that fulfills the following requirements:

#### PUT /element/:id/timestamp/:timestamp

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

#### GET /latest/element/:id

Endpoint for retrieving the latest value (value with the latest timestamp) associated with the given key.

- `id`: Path parameter representing the key.

#### Implementation constraints and expectations 

Database is only expected to store the latest value received so far for a key and should be able to return the latest value received so far for any key ever written(inserted) to the database.

Database will have enough memory to store all the keys being written to the database but might not have enough memory to store all values being written to the database. You can assume that the database can only be guaranteed to have enough memory to store 25% of the all values being written to it.

For each key to be stored in the database, there might be anywhere between 1 to 200 requests with values at different timestamps (and the database doesn’t necessarily receive these values in order of timestamps).  

Against the same key, the database might receive a value with an older timestamp while the existing value against the key in the database has a more recent timestamp, in such cases the received value should be ignored and not replaced against the existing value (with a more recent timestamp).

The database should be able to persist data across restarts.

Database should have a mechanism for crash recovery in case of unexpected termination.

Database should be as performant as possible

You can’t use any of the existing databases to implement this database.

Performance tests -

Test 1 :
~300K writes (key-value inserts) in 30 mins for 25k unique keys
Following i, 25k reads in 3 minutes to verify if the database stored the latest values for the inserted keys.
Resources assigned to database for this test -
256 MB main memory
2 CPUs
8 GB storage

Test 2:
~100k writes (key-value inserts) in 200 secs
Resources assigned to database for this test -
1024 MB main memory
4 CPUs
8 GB storage

## Testing 

Tests are done using gatling - refer to repo https://github.com/ingaleniranjan365/key-value-database-performance-testing
