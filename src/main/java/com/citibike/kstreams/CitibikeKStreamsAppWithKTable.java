package com.citibike.kstreams;

import com.citibike.kstreams.metrics.MetricsService;
import com.citibike.kstreams.metrics.MetricsServer;
import com.citibike.kstreams.transformer.LocationLookupTransformer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Kafka Streams application that uses GlobalKTable for location lookups.
 * Locations are loaded from a Kafka topic into a GlobalKTable for efficient lookups.
 */
public class CitibikeKStreamsAppWithKTable {
    private static final Logger logger = LoggerFactory.getLogger(CitibikeKStreamsAppWithKTable.class);
    
    private static final String INPUT_TOPIC = "citibike-rides";
    private static final String OUTPUT_TOPIC = "citibike-rides-with-locations";
    private static final String LOCATIONS_TOPIC = "locations";

    public static void main(String[] args) {
        try {
            CitibikeKStreamsAppWithKTable app = new CitibikeKStreamsAppWithKTable();
            app.run();
        } catch (Exception e) {
            logger.error("Error running application", e);
            System.exit(1);
        }
    }

    public void run() {
        logger.info("Starting Citibike Kafka Streams Application with KTable lookup");
        logger.info("Loading locations from topic: {}", LOCATIONS_TOPIC);

        // Initialize metrics
        MetricsService metricsService = MetricsService.getInstance();
        MetricsServer metricsServer = new MetricsServer(metricsService);
        try {
            metricsServer.start();
        } catch (Exception e) {
            logger.error("Failed to start metrics server", e);
        }

        // Configure Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "citibike-kstreams-app-ktable");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        // Build the stream topology
        StreamsBuilder builder = new StreamsBuilder();

        // Create GlobalKTable from locations topic with a materialized store
        // The materialized store will be accessible by the transformer
        String storeName = "locations-store";
        builder.globalTable(
                LOCATIONS_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String()),
                org.apache.kafka.streams.kstream.Materialized.as(storeName)
        );

        // Process rides stream using transformer to access the location store
        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .transform(
                        new TransformerSupplier<String, String, org.apache.kafka.streams.KeyValue<String, String>>() {
                            @Override
                            public org.apache.kafka.streams.kstream.Transformer<String, String, org.apache.kafka.streams.KeyValue<String, String>> get() {
                                return new LocationLookupTransformer(storeName);
                            }
                        },
                        storeName
                )
                .filter((key, value) -> value != null) // Filter out errors
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        // Create and start Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        
        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down Kafka Streams application");
            metricsServer.stop();
            streams.close();
        }));

        streams.start();
        logger.info("Kafka Streams application started. Processing from topic: {}", INPUT_TOPIC);
        logger.info("Output topic: {}", OUTPUT_TOPIC);
        logger.info("Locations loaded from topic: {}", LOCATIONS_TOPIC);
    }
}

