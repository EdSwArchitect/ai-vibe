package com.citibike.kstreams.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Service for managing application metrics using Micrometer with Prometheus.
 */
public class MetricsService {
    private static final Logger logger = LoggerFactory.getLogger(MetricsService.class);
    
    private final PrometheusMeterRegistry registry;
    private final Counter ridesProcessedCounter;
    private final Counter ridesParsedCounter;
    private final Counter ridesParseFailedCounter;
    private final Counter lookupsCounter;
    private final Counter lookupsSuccessfulCounter;
    private final Counter lookupsFailedCounter;
    private final Timer lookupTimer;
    private final AtomicLong locationsCount = new AtomicLong(0);
    private final AtomicLong ridesCount = new AtomicLong(0);

    private static MetricsService instance;

    private MetricsService() {
        this.registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        
        // Counters
        this.ridesProcessedCounter = Counter.builder("citibike.rides.processed")
                .description("Total number of rides processed")
                .register(registry);
        
        this.ridesParsedCounter = Counter.builder("citibike.rides.parsed")
                .description("Total number of rides successfully parsed")
                .register(registry);
        
        this.ridesParseFailedCounter = Counter.builder("citibike.rides.parse.failed")
                .description("Total number of rides that failed to parse")
                .register(registry);
        
        this.lookupsCounter = Counter.builder("citibike.location.lookups")
                .description("Total number of location lookups attempted")
                .register(registry);
        
        this.lookupsSuccessfulCounter = Counter.builder("citibike.location.lookups.successful")
                .description("Total number of successful location lookups")
                .register(registry);
        
        this.lookupsFailedCounter = Counter.builder("citibike.location.lookups.failed")
                .description("Total number of failed location lookups")
                .register(registry);
        
        // Timer for lookup duration (histogram)
        this.lookupTimer = Timer.builder("citibike.location.lookup.duration")
                .description("Duration of location lookups in seconds")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);
        
        // Gauges
        Gauge.builder("citibike.locations.count", locationsCount, AtomicLong::get)
                .description("Current number of locations loaded")
                .register(registry);
        
        Gauge.builder("citibike.rides.count", ridesCount, AtomicLong::get)
                .description("Current number of rides")
                .register(registry);
        
        logger.info("MetricsService initialized");
    }

    public static synchronized MetricsService getInstance() {
        if (instance == null) {
            instance = new MetricsService();
        }
        return instance;
    }

    public void incrementRidesProcessed() {
        ridesProcessedCounter.increment();
        ridesCount.incrementAndGet();
    }

    public void incrementRidesParsed() {
        ridesParsedCounter.increment();
    }

    public void incrementRidesParseFailed() {
        ridesParseFailedCounter.increment();
    }

    public void incrementLookups() {
        lookupsCounter.increment();
    }

    public void incrementLookupsSuccessful() {
        lookupsSuccessfulCounter.increment();
    }

    public void incrementLookupsFailed() {
        lookupsFailedCounter.increment();
    }

    public Timer.Sample startLookupTimer() {
        return Timer.start(registry);
    }

    public void recordLookupDuration(Timer.Sample sample) {
        sample.stop(lookupTimer);
    }

    public void setLocationsCount(long count) {
        locationsCount.set(count);
    }

    public void setRidesCount(long count) {
        ridesCount.set(count);
    }

    public PrometheusMeterRegistry getRegistry() {
        return registry;
    }

    public String scrape() {
        return registry.scrape();
    }
}

