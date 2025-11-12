package com.citibike.kstreams;

import com.citibike.kstreams.metrics.MetricsService;
import com.citibike.kstreams.metrics.MetricsServer;
import com.citibike.kstreams.model.CitibikeRide;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.exceptions.CsvValidationException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class CsvToKafkaProducer {
    private static final Logger logger = LoggerFactory.getLogger(CsvToKafkaProducer.class);
    private static final String TOPIC = "citibike-rides";

    public static void main(String[] args) {
        String csvFile = args.length > 0 ? args[0] : "citibike.csv";
        
        try {
            CsvToKafkaProducer producer = new CsvToKafkaProducer();
            producer.produce(csvFile);
        } catch (Exception e) {
            logger.error("Error producing messages", e);
            System.exit(1);
        }
    }

    public void produce(String csvFilePath) throws IOException, CsvValidationException {
        logger.info("Reading CSV file: {}", csvFilePath);
        
        // Initialize metrics
        MetricsService metricsService = MetricsService.getInstance();
        MetricsServer metricsServer = new MetricsServer(metricsService);
        metricsServer.start();
        
        // Configure Kafka Producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props, 
                new StringSerializer(), new StringSerializer());
             FileReader fileReader = new FileReader(csvFilePath, StandardCharsets.UTF_8);
             CSVReader csvReader = new CSVReaderBuilder(fileReader)
                     .withSkipLines(0)  // We'll skip header manually
                     .build()) {

            // Skip header
            try {
                csvReader.readNext();
            } catch (CsvValidationException e) {
                logger.warn("Error reading header", e);
            }

            String[] line;
            int count = 0;
            int skipped = 0;
            
            try {
                while ((line = csvReader.readNext()) != null) {
                    // Skip empty lines or lines with insufficient columns
                    if (line == null || line.length < 13) {
                        skipped++;
                        continue;
                    }

                    try {
                        CitibikeRide ride = parseCsvLine(line);
                        metricsService.incrementRidesProcessed();
                        
                        // Skip rides without valid ride_id
                        if (ride.getRideId() == null || ride.getRideId().trim().isEmpty()) {
                            skipped++;
                            metricsService.incrementRidesParseFailed();
                            logger.debug("Skipping ride with missing ride_id");
                            continue;
                        }
                        
                        metricsService.incrementRidesParsed();
                        String jsonValue = objectMapper.writeValueAsString(ride);
                        
                        ProducerRecord<String, String> record = new ProducerRecord<>(
                                TOPIC, ride.getRideId(), jsonValue);
                        
                        producer.send(record, (metadata, exception) -> {
                            if (exception != null) {
                                logger.error("Error sending record for ride_id: {}", ride.getRideId(), exception);
                            }
                        });
                        
                        count++;
                        metricsService.setRidesCount(count);
                        if (count % 1000 == 0) {
                            logger.info("Produced {} records ({} skipped)", count, skipped);
                            producer.flush();
                        }
                    } catch (Exception e) {
                        skipped++;
                        metricsService.incrementRidesParseFailed();
                        logger.warn("Error parsing line {}: {}", count + skipped, e.getMessage());
                        if (logger.isDebugEnabled()) {
                            logger.debug("Failed line content: {}", String.join(",", line));
                        }
                    }
                }
            } catch (CsvValidationException e) {
                logger.error("Error reading CSV", e);
            }
            
            if (skipped > 0) {
                logger.warn("Skipped {} records due to parsing errors or missing data", skipped);
            }
            
            producer.flush();
            logger.info("Finished producing {} records to topic: {} ({} skipped)", count, TOPIC, skipped);
            logger.info("Metrics server running at http://localhost:8081/metrics");
        }
    }

    private CitibikeRide parseCsvLine(String[] line) {
        CitibikeRide ride = new CitibikeRide();
        
        // Parse basic fields
        ride.setRideId(cleanField(line, 0));
        ride.setRideableType(cleanField(line, 1));
        ride.setStartedAt(cleanField(line, 2));
        ride.setEndedAt(cleanField(line, 3));
        ride.setStartStationName(cleanField(line, 4));
        ride.setStartStationId(cleanField(line, 5));
        ride.setEndStationName(cleanField(line, 6));
        ride.setEndStationId(cleanField(line, 7));
        
        // Parse coordinates with robust error handling
        ride.setStartLat(parseDouble(cleanField(line, 8)));
        ride.setStartLng(parseDouble(cleanField(line, 9)));
        ride.setEndLat(parseDouble(cleanField(line, 10)));
        ride.setEndLng(parseDouble(cleanField(line, 11)));
        
        ride.setMemberCasual(cleanField(line, 12));
        return ride;
    }

    /**
     * Clean and extract field value from CSV line
     */
    private String cleanField(String[] line, int index) {
        if (line == null || index < 0 || index >= line.length) {
            return null;
        }
        String value = line[index];
        if (value == null) {
            return null;
        }
        // Remove quotes and trim whitespace
        value = value.replaceAll("^\"|\"$", "").trim();
        return value.isEmpty() ? null : value;
    }

    /**
     * Parse double value with robust error handling
     */
    private Double parseDouble(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        
        try {
            // Remove any whitespace and parse
            String cleaned = value.trim();
            return Double.parseDouble(cleaned);
        } catch (NumberFormatException e) {
            logger.debug("Failed to parse double value: '{}'", value);
            return null;
        }
    }
}

