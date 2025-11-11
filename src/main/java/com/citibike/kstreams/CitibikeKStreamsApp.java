package com.citibike.kstreams;

import com.citibike.kstreams.model.CitibikeRide;
import com.citibike.kstreams.model.Location;
import com.citibike.kstreams.model.RideWithLocation;
import com.citibike.kstreams.service.LocationLookupService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class CitibikeKStreamsApp {
    private static final Logger logger = LoggerFactory.getLogger(CitibikeKStreamsApp.class);
    
    private static final String INPUT_TOPIC = "citibike-rides";
    private static final String OUTPUT_TOPIC = "citibike-rides-with-locations";
    private static final String LOCATIONS_FILE = "locations.json";

    public static void main(String[] args) {
        String locationsPath = args.length > 0 ? args[0] : LOCATIONS_FILE;
        
        try {
            CitibikeKStreamsApp app = new CitibikeKStreamsApp();
            app.run(locationsPath);
        } catch (Exception e) {
            logger.error("Error running application", e);
            System.exit(1);
        }
    }

    public void run(String locationsFilePath) throws IOException {
        logger.info("Starting Citibike Kafka Streams Application");
        logger.info("Loading locations from: {}", locationsFilePath);

        // Load location lookup service
        LocationLookupService locationService = new LocationLookupService(locationsFilePath);

        // Configure Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "citibike-kstreams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        // Create ObjectMapper for JSON serialization
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

        // Build the stream topology
        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues(value -> {
                    try {
                        // Deserialize CitibikeRide from JSON
                        CitibikeRide ride = objectMapper.readValue(value, CitibikeRide.class);
                        
                        // Lookup start location
                        Location startLocation = locationService.findClosestLocation(
                                ride.getStartLat(), ride.getStartLng());
                        
                        // Lookup end location
                        Location endLocation = locationService.findClosestLocation(
                                ride.getEndLat(), ride.getEndLng());
                        
                        // Create enriched object
                        RideWithLocation rideWithLocation = new RideWithLocation(
                                ride, startLocation, endLocation);
                        
                        // Serialize to JSON
                        return objectMapper.writeValueAsString(rideWithLocation);
                    } catch (Exception e) {
                        logger.error("Error processing ride: {}", value, e);
                        return null;
                    }
                })
                .filter((key, value) -> value != null) // Filter out errors
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        // Create and start Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        
        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down Kafka Streams application");
            streams.close();
        }));

        streams.start();
        logger.info("Kafka Streams application started. Processing from topic: {}", INPUT_TOPIC);
        logger.info("Output topic: {}", OUTPUT_TOPIC);
    }
}

