package com.citibike.kstreams;

import com.citibike.kstreams.model.CitibikeRide;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.opencsv.CSVReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;
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

    public void produce(String csvFilePath) throws IOException {
        logger.info("Reading CSV file: {}", csvFilePath);
        
        // Configure Kafka Producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERDE_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERDE_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props, 
                new StringSerializer(), new StringSerializer());
             FileReader fileReader = new FileReader(csvFilePath);
             CSVReader csvReader = new CSVReader(fileReader)) {

            // Skip header
            csvReader.readNext();

            String[] line;
            int count = 0;
            
            while ((line = csvReader.readNext()) != null) {
                if (line.length < 13) {
                    continue;
                }

                try {
                    CitibikeRide ride = parseCsvLine(line);
                    String jsonValue = objectMapper.writeValueAsString(ride);
                    
                    ProducerRecord<String, String> record = new ProducerRecord<>(
                            TOPIC, ride.getRideId(), jsonValue);
                    
                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            logger.error("Error sending record", exception);
                        }
                    });
                    
                    count++;
                    if (count % 1000 == 0) {
                        logger.info("Produced {} records", count);
                        producer.flush();
                    }
                } catch (Exception e) {
                    logger.warn("Error parsing line: {}", String.join(",", line), e);
                }
            }
            
            producer.flush();
            logger.info("Finished producing {} records to topic: {}", count, TOPIC);
        }
    }

    private CitibikeRide parseCsvLine(String[] line) {
        CitibikeRide ride = new CitibikeRide();
        ride.setRideId(removeQuotes(line[0]));
        ride.setRideableType(removeQuotes(line[1]));
        ride.setStartedAt(removeQuotes(line[2]));
        ride.setEndedAt(removeQuotes(line[3]));
        ride.setStartStationName(removeQuotes(line[4]));
        ride.setStartStationId(removeQuotes(line[5]));
        ride.setEndStationName(removeQuotes(line[6]));
        ride.setEndStationId(removeQuotes(line[7]));
        
        try {
            ride.setStartLat(Double.parseDouble(removeQuotes(line[8])));
            ride.setStartLng(Double.parseDouble(removeQuotes(line[9])));
            ride.setEndLat(Double.parseDouble(removeQuotes(line[10])));
            ride.setEndLng(Double.parseDouble(removeQuotes(line[11])));
        } catch (NumberFormatException e) {
            logger.warn("Error parsing coordinates", e);
        }
        
        ride.setMemberCasual(removeQuotes(line[12]));
        return ride;
    }

    private String removeQuotes(String str) {
        if (str == null) {
            return null;
        }
        return str.replaceAll("^\"|\"$", "");
    }
}

