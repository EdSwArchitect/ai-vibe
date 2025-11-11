# Citibike Kafka Streams Application

A Java 21 Kafka Streams application that processes Citibike ride data and enriches it with geolocation information from a lookup table.

## Features

- Reads Citibike ride data from CSV files
- Publishes ride data to Kafka topics
- Enriches ride data with geolocation information using a JSON lookup table
- Outputs enriched data to Kafka topics
- Uses Kafka Streams for real-time stream processing

## Prerequisites

- **Java 21 or higher** (required - the project uses Java 21 features)
  - To check your Java version: `java -version`
  - To install Java 21 on macOS: `brew install openjdk@21` or download from [Oracle](https://www.oracle.com/java/technologies/downloads/#java21)
- Maven 3.6+
- Apache Kafka (for running the application)
- Zookeeper (required by Kafka)

## Project Structure

```
.
├── pom.xml                          # Maven build configuration
├── citibike.csv                     # Input CSV file with Citibike ride data
├── locations.json                   # Geolocation lookup table (JSON)
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── com/citibike/kstreams/
│   │   │       ├── CitibikeKStreamsApp.java    # Main Kafka Streams application
│   │   │       ├── CsvToKafkaProducer.java     # CSV to Kafka producer
│   │   │       ├── model/                      # Data models
│   │   │       │   ├── CitibikeRide.java
│   │   │       │   ├── Location.java
│   │   │       │   └── RideWithLocation.java
│   │   │       └── service/
│   │   │           └── LocationLookupService.java
│   │   └── resources/
│   │       └── logback.xml
│   └── test/
│       ├── java/                    # Test classes
│       └── resources/               # Test data files
└── README.md
```

## Building the Project

```bash
mvn clean package
```

This will create a JAR file in the `target/` directory.

## Running the Application

### 1. Start Kafka

Start Kafka and Zookeeper using Docker Compose:

```bash
# Start Kafka, Zookeeper, and Kafka UI
docker-compose up -d

# Check that services are running
docker-compose ps

# View logs if needed
docker-compose logs -f kafka

# Stop services when done
docker-compose down
```

The Docker Compose setup includes:
- **Zookeeper** on port 2181
- **Kafka** on port 9092
- **Kafka UI** on port 8080 (web interface for managing topics and viewing messages)

You can access Kafka UI at http://localhost:8080 to view topics, messages, and manage your Kafka cluster.

### 2. Create Kafka Topics

Topics will be auto-created when you start producing messages, but you can also create them manually:

```bash
# Using Docker exec
docker exec -it kafka kafka-topics.sh --create --topic citibike-rides --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

docker exec -it kafka kafka-topics.sh --create --topic citibike-rides-with-locations --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# Or use Kafka UI at http://localhost:8080 to create topics via the web interface
```

Alternatively, if you have Kafka installed locally:
```bash
kafka-topics.sh --create --topic citibike-rides --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
kafka-topics.sh --create --topic citibike-rides-with-locations --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### 3. Produce CSV Data to Kafka

```bash
java -cp target/citibike-kstreams-1.0.0.jar com.citibike.kstreams.CsvToKafkaProducer citibike.csv
```

### 4. Run the Kafka Streams Application

```bash
java -cp target/citibike-kstreams-1.0.0.jar com.citibike.kstreams.CitibikeKStreamsApp locations.json
```

### 5. Consume Output

```bash
# Using Docker exec
docker exec -it kafka kafka-console-consumer.sh --topic citibike-rides-with-locations --from-beginning --bootstrap-server localhost:9092

# Or if you have Kafka installed locally
kafka-console-consumer.sh --topic citibike-rides-with-locations --from-beginning --bootstrap-server localhost:9092
```

You can also view messages in Kafka UI at http://localhost:8080 by navigating to the `citibike-rides-with-locations` topic.

## Data Format

### Input (Citibike CSV)
- `ride_id`: Unique ride identifier
- `rideable_type`: Type of bike (classic_bike, electric_bike)
- `started_at`, `ended_at`: Timestamps
- `start_station_name`, `end_station_name`: Station names
- `start_lat`, `start_lng`, `end_lat`, `end_lng`: Coordinates
- `member_casual`: Membership type

### Location Lookup (JSON)
Array of location objects with:
- `lat`, `lon`: Coordinates
- `display_name`: Human-readable location name
- `address`: Detailed address information

### Output (JSON)
Enriched ride data with:
- Original ride information
- `start_location`: Matched location for start coordinates
- `end_location`: Matched location for end coordinates

## Testing

Run tests with:

```bash
mvn test
```

## Configuration

The application uses the following Kafka topics:
- **Input**: `citibike-rides`
- **Output**: `citibike-rides-with-locations`

Kafka broker is configured to connect to `localhost:9092` by default. You can modify this in the code if needed.

## Location Matching

The application uses a distance-based matching algorithm (Haversine formula) to find the closest location within a threshold of approximately 1km from the ride coordinates.

## License

This project is for educational purposes.
