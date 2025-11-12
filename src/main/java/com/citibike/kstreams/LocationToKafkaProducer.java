package com.citibike.kstreams;

import com.citibike.kstreams.metrics.MetricsService;
import com.citibike.kstreams.metrics.MetricsServer;
import com.citibike.kstreams.model.Location;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * Producer that loads locations from JSON file and publishes them to Kafka topic.
 * Locations are keyed by a coordinate grid for efficient lookups.
 */
public class LocationToKafkaProducer {
    private static final Logger logger = LoggerFactory.getLogger(LocationToKafkaProducer.class);
    private static final String TOPIC = "locations";
    private static final double GRID_PRECISION = 0.01; // ~1km grid cells

    public static void main(String[] args) {
        String locationsFile = args.length > 0 ? args[0] : "locations.json";
        
        try {
            LocationToKafkaProducer producer = new LocationToKafkaProducer();
            producer.produce(locationsFile);
        } catch (Exception e) {
            logger.error("Error producing locations", e);
            System.exit(1);
        }
    }

    public void produce(String locationsFilePath) throws IOException {
        logger.info("Loading locations from: {}", locationsFilePath);
        
        // Initialize metrics
        MetricsService metricsService = MetricsService.getInstance();
        MetricsServer metricsServer = new MetricsServer(metricsService);
        metricsServer.start();
        
        // Load locations from JSON with lenient parsing
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
        objectMapper.configure(com.fasterxml.jackson.databind.DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
        
        File file = new File(locationsFilePath);
        Location[] locations;
        
        try {
            locations = objectMapper.readValue(file, Location[].class);
            logger.info("Loaded {} locations", locations.length);
            metricsService.setLocationsCount(locations.length);
        } catch (Exception e) {
            logger.error("Error parsing locations JSON file: {}", locationsFilePath, e);
            throw new IOException("Failed to parse locations JSON file", e);
        }
        
        // Configure Kafka Producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props, 
                new StringSerializer(), new StringSerializer())) {

            int count = 0;
            int skipped = 0;
            for (Location location : locations) {
                try {
                    // Skip locations without valid coordinates
                    Double lat = location.getLatAsDouble();
                    Double lon = location.getLonAsDouble();
                    
                    if (lat == null || lon == null) {
                        skipped++;
                        logger.debug("Skipping location {} - missing coordinates", location.getPlaceId());
                        continue;
                    }
                    
                    // Create key based on coordinate grid for efficient lookups
                    String key = createGridKey(lat, lon);
                    
                    // Also include place_id in key for uniqueness
                    if (location.getPlaceId() != null) {
                        key = location.getPlaceId() + "_" + key;
                    } else {
                        // Use coordinates as fallback if place_id is missing
                        key = "no_id_" + key;
                    }
                    
                    String jsonValue = objectMapper.writeValueAsString(location);
                    
                    ProducerRecord<String, String> record = new ProducerRecord<>(
                            TOPIC, key, jsonValue);
                    
                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            logger.error("Error sending location record for place_id: {}", location.getPlaceId(), exception);
                        }
                    });
                    
                    count++;
                    if (count % 1000 == 0) {
                        logger.info("Produced {} location records ({} skipped)", count, skipped);
                        producer.flush();
                    }
                } catch (Exception e) {
                    skipped++;
                    logger.warn("Error processing location (place_id: {}): {}", 
                            location != null ? location.getPlaceId() : "unknown", e.getMessage());
                }
            }
            
            if (skipped > 0) {
                logger.warn("Skipped {} locations due to errors or missing data", skipped);
            }
            
            producer.flush();
            logger.info("Finished producing {} location records to topic: {}", count, TOPIC);
        }
    }

    /**
     * Create a grid key based on rounded coordinates for efficient lookups
     */
    private String createGridKey(Double lat, Double lng) {
        if (lat == null || lng == null) {
            return "unknown";
        }
        // Round to grid precision (e.g., 0.01 degrees ≈ 1km)
        double gridLat = Math.round(lat / GRID_PRECISION) * GRID_PRECISION;
        double gridLng = Math.round(lng / GRID_PRECISION) * GRID_PRECISION;
        return String.format("lat_%.4f_lng_%.4f", gridLat, gridLng);
    }
}

