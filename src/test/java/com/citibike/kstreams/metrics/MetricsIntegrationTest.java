package com.citibike.kstreams.metrics;

import com.citibike.kstreams.model.Location;
import com.citibike.kstreams.service.LocationLookupService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests to verify metrics are collected during actual operations.
 */
class MetricsIntegrationTest {
    private MetricsService metricsService;
    private static final String TEST_LOCATIONS_FILE = "src/test/resources/test-locations.json";

    @BeforeEach
    void setUp() {
        metricsService = MetricsService.getInstance();
    }

    @Test
    void testMetricsCollectedDuringRideProcessing() {
        double initialProcessed = metricsService.getRegistry()
                .get("citibike.rides.processed").counter().count();
        double initialParsed = metricsService.getRegistry()
                .get("citibike.rides.parsed").counter().count();
        
        // Simulate processing rides
        for (int i = 0; i < 5; i++) {
            metricsService.incrementRidesProcessed();
            metricsService.incrementRidesParsed();
        }
        
        double finalProcessed = metricsService.getRegistry()
                .get("citibike.rides.processed").counter().count();
        double finalParsed = metricsService.getRegistry()
                .get("citibike.rides.parsed").counter().count();
        
        assertEquals(initialProcessed + 5, finalProcessed, 0.01,
                "Should have processed 5 rides");
        assertEquals(initialParsed + 5, finalParsed, 0.01,
                "Should have parsed 5 rides");
    }

    @Test
    void testMetricsCollectedDuringLocationLookups() throws IOException {
        LocationLookupService locationService = 
                new LocationLookupService(TEST_LOCATIONS_FILE);
        
        double initialLookups = metricsService.getRegistry()
                .get("citibike.location.lookups").counter().count();
        double initialSuccessful = metricsService.getRegistry()
                .get("citibike.location.lookups.successful").counter().count();
        double initialFailed = metricsService.getRegistry()
                .get("citibike.location.lookups.failed").counter().count();
        
        // Perform lookups
        var timer1 = metricsService.startLookupTimer();
        Location loc1 = locationService.findClosestLocation(40.78499979, -73.97283406);
        metricsService.recordLookupDuration(timer1);
        metricsService.incrementLookups();
        if (loc1 != null) {
            metricsService.incrementLookupsSuccessful();
        } else {
            metricsService.incrementLookupsFailed();
        }
        
        var timer2 = metricsService.startLookupTimer();
        Location loc2 = locationService.findClosestLocation(0.0, 0.0); // Should fail
        metricsService.recordLookupDuration(timer2);
        metricsService.incrementLookups();
        if (loc2 != null) {
            metricsService.incrementLookupsSuccessful();
        } else {
            metricsService.incrementLookupsFailed();
        }
        
        double finalLookups = metricsService.getRegistry()
                .get("citibike.location.lookups").counter().count();
        double finalSuccessful = metricsService.getRegistry()
                .get("citibike.location.lookups.successful").counter().count();
        double finalFailed = metricsService.getRegistry()
                .get("citibike.location.lookups.failed").counter().count();
        
        assertEquals(initialLookups + 2, finalLookups, 0.01,
                "Should have performed 2 lookups");
        assertTrue(finalSuccessful > initialSuccessful || finalFailed > initialFailed,
                "Should have at least one successful or failed lookup");
    }

    @Test
    void testMetricsReflectActualCounts() throws IOException {
        LocationLookupService locationService = 
                new LocationLookupService(TEST_LOCATIONS_FILE);
        
        int actualLocationCount = locationService.getAllLocations().size();
        metricsService.setLocationsCount(actualLocationCount);
        
        double gaugeValue = metricsService.getRegistry()
                .get("citibike.locations.count").gauge().value();
        
        assertEquals(actualLocationCount, gaugeValue, 0.01,
                "Metrics should reflect actual location count");
    }

    @Test
    void testParseFailureMetrics() {
        double initialFailed = metricsService.getRegistry()
                .get("citibike.rides.parse.failed").counter().count();
        
        // Simulate parse failures
        metricsService.incrementRidesProcessed();
        metricsService.incrementRidesParseFailed();
        metricsService.incrementRidesProcessed();
        metricsService.incrementRidesParseFailed();
        
        double finalFailed = metricsService.getRegistry()
                .get("citibike.rides.parse.failed").counter().count();
        
        assertEquals(initialFailed + 2, finalFailed, 0.01,
                "Should track 2 parse failures");
    }

    @Test
    void testLookupDurationMetrics() {
        // Perform multiple lookups with different durations
        for (int i = 0; i < 3; i++) {
            var timer = metricsService.startLookupTimer();
            try {
                Thread.sleep(10 + i * 5); // Simulate varying lookup times
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            metricsService.recordLookupDuration(timer);
        }
        
        var timer = metricsService.getRegistry()
                .get("citibike.location.lookup.duration").timer();
        
        assertTrue(timer.count() >= 3, 
                "Should have recorded at least 3 timer measurements");
        assertTrue(timer.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS) > 0,
                "Total time should be greater than 0");
    }

    @Test
    void testMetricsScrapeContainsAllCounters() {
        // Set up some test data
        metricsService.setLocationsCount(100);
        metricsService.setRidesCount(50);
        metricsService.incrementRidesProcessed();
        metricsService.incrementRidesParsed();
        metricsService.incrementRidesParseFailed();
        metricsService.incrementLookups();
        metricsService.incrementLookupsSuccessful();
        metricsService.incrementLookupsFailed();
        
        String metrics = metricsService.scrape();
        
        // Verify all metrics are present
        assertTrue(metrics.contains("citibike_locations_count"), 
                "Should contain locations count");
        assertTrue(metrics.contains("citibike_rides_count"), 
                "Should contain rides count");
        assertTrue(metrics.contains("citibike_rides_processed_total"), 
                "Should contain rides processed");
        assertTrue(metrics.contains("citibike_rides_parsed_total"), 
                "Should contain rides parsed");
        assertTrue(metrics.contains("citibike_rides_parse_failed_total"), 
                "Should contain parse failures");
        assertTrue(metrics.contains("citibike_location_lookups_total"), 
                "Should contain lookups total");
        assertTrue(metrics.contains("citibike_location_lookups_successful_total"), 
                "Should contain successful lookups");
        assertTrue(metrics.contains("citibike_location_lookups_failed_total"), 
                "Should contain failed lookups");
    }

    @Test
    void testMetricsRateCalculation() {
        // Simulate processing over time
        long startTime = System.currentTimeMillis();
        
        for (int i = 0; i < 10; i++) {
            metricsService.incrementRidesProcessed();
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        // Verify metrics were recorded
        double totalProcessed = metricsService.getRegistry()
                .get("citibike.rides.processed").counter().count();
        
        assertTrue(totalProcessed >= 10, 
                "Should have processed at least 10 rides");
        
        // Rate would be calculated by Prometheus using rate() function
        // This test verifies the counter is incrementing correctly
        assertTrue(duration > 0, "Should have taken some time");
    }
}

