package com.citibike.kstreams.metrics;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MetricsServiceTest {
    private MetricsService metricsService;

    @BeforeEach
    void setUp() {
        // Reset singleton for testing
        metricsService = MetricsService.getInstance();
    }

    @Test
    void testMetricsServiceSingleton() {
        MetricsService instance1 = MetricsService.getInstance();
        MetricsService instance2 = MetricsService.getInstance();
        assertSame(instance1, instance2, "MetricsService should be a singleton");
    }

    @Test
    void testIncrementRidesProcessed() {
        double initialCount = metricsService.getRegistry().get("citibike.rides.processed").counter().count();
        metricsService.incrementRidesProcessed();
        double newCount = metricsService.getRegistry().get("citibike.rides.processed").counter().count();
        assertEquals(initialCount + 1, newCount, 0.01, "Rides processed counter should increment");
    }

    @Test
    void testIncrementRidesParsed() {
        double initialCount = metricsService.getRegistry().get("citibike.rides.parsed").counter().count();
        metricsService.incrementRidesParsed();
        double newCount = metricsService.getRegistry().get("citibike.rides.parsed").counter().count();
        assertEquals(initialCount + 1, newCount, 0.01, "Rides parsed counter should increment");
    }

    @Test
    void testIncrementRidesParseFailed() {
        double initialCount = metricsService.getRegistry().get("citibike.rides.parse.failed").counter().count();
        metricsService.incrementRidesParseFailed();
        double newCount = metricsService.getRegistry().get("citibike.rides.parse.failed").counter().count();
        assertEquals(initialCount + 1, newCount, 0.01, "Rides parse failed counter should increment");
    }

    @Test
    void testIncrementLookups() {
        double initialCount = metricsService.getRegistry().get("citibike.location.lookups").counter().count();
        metricsService.incrementLookups();
        double newCount = metricsService.getRegistry().get("citibike.location.lookups").counter().count();
        assertEquals(initialCount + 1, newCount, 0.01, "Lookups counter should increment");
    }

    @Test
    void testIncrementLookupsSuccessful() {
        double initialCount = metricsService.getRegistry().get("citibike.location.lookups.successful").counter().count();
        metricsService.incrementLookupsSuccessful();
        double newCount = metricsService.getRegistry().get("citibike.location.lookups.successful").counter().count();
        assertEquals(initialCount + 1, newCount, 0.01, "Successful lookups counter should increment");
    }

    @Test
    void testIncrementLookupsFailed() {
        double initialCount = metricsService.getRegistry().get("citibike.location.lookups.failed").counter().count();
        metricsService.incrementLookupsFailed();
        double newCount = metricsService.getRegistry().get("citibike.location.lookups.failed").counter().count();
        assertEquals(initialCount + 1, newCount, 0.01, "Failed lookups counter should increment");
    }

    @Test
    void testSetLocationsCount() {
        long testCount = 1000L;
        metricsService.setLocationsCount(testCount);
        double gaugeValue = metricsService.getRegistry().get("citibike.locations.count").gauge().value();
        assertEquals(testCount, gaugeValue, 0.01, "Locations count gauge should be set correctly");
    }

    @Test
    void testSetRidesCount() {
        long testCount = 500L;
        metricsService.setRidesCount(testCount);
        double gaugeValue = metricsService.getRegistry().get("citibike.rides.count").gauge().value();
        assertEquals(testCount, gaugeValue, 0.01, "Rides count gauge should be set correctly");
    }

    @Test
    void testLookupTimer() {
        var sample = metricsService.startLookupTimer();
        assertNotNull(sample, "Timer sample should not be null");
        
        // Simulate some work
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        metricsService.recordLookupDuration(sample);
        
        // Verify timer was recorded
        var timer = metricsService.getRegistry().get("citibike.location.lookup.duration").timer();
        assertTrue(timer.count() > 0, "Timer should have recorded at least one measurement");
    }

    @Test
    void testScrapeReturnsPrometheusFormat() {
        String metrics = metricsService.scrape();
        assertNotNull(metrics, "Scrape should return non-null string");
        assertFalse(metrics.isEmpty(), "Scrape should return non-empty string");
        
        // Verify Prometheus format
        assertTrue(metrics.contains("# HELP"), "Should contain Prometheus HELP comments");
        assertTrue(metrics.contains("# TYPE"), "Should contain Prometheus TYPE comments");
        assertTrue(metrics.contains("citibike_"), "Should contain citibike metrics");
    }

    @Test
    void testAllMetricsAreExposed() {
        String metrics = metricsService.scrape();
        
        // Verify all expected metrics are present
        assertTrue(metrics.contains("citibike_locations_count"), "Should contain locations count");
        assertTrue(metrics.contains("citibike_rides_count"), "Should contain rides count");
        assertTrue(metrics.contains("citibike_rides_processed_total"), "Should contain rides processed");
        assertTrue(metrics.contains("citibike_rides_parsed_total"), "Should contain rides parsed");
        assertTrue(metrics.contains("citibike_rides_parse_failed_total"), "Should contain parse failures");
        assertTrue(metrics.contains("citibike_location_lookups_total"), "Should contain lookups total");
        assertTrue(metrics.contains("citibike_location_lookups_successful_total"), "Should contain successful lookups");
        assertTrue(metrics.contains("citibike_location_lookups_failed_total"), "Should contain failed lookups");
        assertTrue(metrics.contains("citibike_location_lookup_duration_seconds"), "Should contain lookup duration");
    }

    @Test
    void testMetricsAccumulation() {
        // Test that metrics accumulate correctly
        double initialProcessed = metricsService.getRegistry().get("citibike.rides.processed").counter().count();
        
        metricsService.incrementRidesProcessed();
        metricsService.incrementRidesProcessed();
        metricsService.incrementRidesProcessed();
        
        double finalProcessed = metricsService.getRegistry().get("citibike.rides.processed").counter().count();
        assertEquals(initialProcessed + 3, finalProcessed, 0.01, "Counter should accumulate correctly");
    }
}

