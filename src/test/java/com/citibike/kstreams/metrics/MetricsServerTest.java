package com.citibike.kstreams.metrics;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;

import static org.junit.jupiter.api.Assertions.*;

class MetricsServerTest {
    private MetricsService metricsService;
    private MetricsServer metricsServer;
    private static final int TEST_PORT = 18081;

    @BeforeEach
    void setUp() {
        metricsService = MetricsService.getInstance();
        metricsServer = new MetricsServer(metricsService);
    }

    @AfterEach
    void tearDown() {
        if (metricsServer != null) {
            metricsServer.stop();
        }
    }

    @Test
    void testMetricsServerStart() throws Exception {
        metricsServer.start(TEST_PORT);
        
        // Verify server is running by making HTTP request
        URI uri = new URI("http://localhost:" + TEST_PORT + "/metrics");
        HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
        connection.setRequestMethod("GET");
        
        int responseCode = connection.getResponseCode();
        assertEquals(200, responseCode, "Metrics endpoint should return 200 OK");
        
        connection.disconnect();
    }

    @Test
    void testMetricsEndpointReturnsPrometheusFormat() throws Exception {
        metricsServer.start(TEST_PORT);
        
        URI uri = new URI("http://localhost:" + TEST_PORT + "/metrics");
        HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
        connection.setRequestMethod("GET");
        
        assertEquals(200, connection.getResponseCode());
        
        // Read response
        String response = new String(connection.getInputStream().readAllBytes());
        assertNotNull(response, "Response should not be null");
        assertFalse(response.isEmpty(), "Response should not be empty");
        
        // Verify Prometheus format
        assertTrue(response.contains("# HELP"), "Should contain Prometheus HELP");
        assertTrue(response.contains("# TYPE"), "Should contain Prometheus TYPE");
        assertTrue(response.contains("citibike_"), "Should contain citibike metrics");
        
        // Verify Content-Type header
        String contentType = connection.getContentType();
        assertTrue(contentType.contains("text/plain"), "Content-Type should be text/plain");
        
        connection.disconnect();
    }

    @Test
    void testMetricsServerStop() throws Exception {
        metricsServer.start(TEST_PORT);
        assertTrue(isServerRunning(TEST_PORT), "Server should be running");
        
        metricsServer.stop();
        
        // Give server time to stop
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Server should be stopped (connection will fail)
        assertThrows(Exception.class, () -> {
            URI uri = new URI("http://localhost:" + TEST_PORT + "/metrics");
            HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
            connection.setRequestMethod("GET");
            connection.getResponseCode();
        }, "Connection should fail after server stop");
    }

    @Test
    void testMetricsEndpointWithData() throws Exception {
        // Add some test data
        metricsService.setLocationsCount(100);
        metricsService.setRidesCount(50);
        metricsService.incrementRidesProcessed();
        metricsService.incrementRidesParsed();
        metricsService.incrementLookups();
        metricsService.incrementLookupsSuccessful();
        
        metricsServer.start(TEST_PORT);
        
        URI uri = new URI("http://localhost:" + TEST_PORT + "/metrics");
        HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
        connection.setRequestMethod("GET");
        
        String response = new String(connection.getInputStream().readAllBytes());
        
        // Verify metrics are in the response (Prometheus format may vary)
        assertTrue(response.contains("citibike_locations_count"), 
                "Should contain locations count metric");
        assertTrue(response.contains("100"), 
                "Should contain locations count value");
        assertTrue(response.contains("citibike_rides_count"), 
                "Should contain rides count metric");
        assertTrue(response.contains("50"), 
                "Should contain rides count value");
        assertTrue(response.contains("citibike_rides_processed_total"), 
                "Should contain processed rides metric");
        assertTrue(response.contains("citibike_location_lookups_total"), 
                "Should contain lookups total metric");
        
        connection.disconnect();
    }

    @Test
    void testNonGetMethodReturns405() throws Exception {
        metricsServer.start(TEST_PORT);
        
        URI uri = new URI("http://localhost:" + TEST_PORT + "/metrics");
        HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
        connection.setRequestMethod("POST");
        
        int responseCode = connection.getResponseCode();
        assertEquals(405, responseCode, "Non-GET methods should return 405 Method Not Allowed");
        
        connection.disconnect();
    }

    private boolean isServerRunning(int port) {
        try {
            URI uri = new URI("http://localhost:" + port + "/metrics");
            HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(1000);
            int responseCode = connection.getResponseCode();
            connection.disconnect();
            return responseCode == 200;
        } catch (Exception e) {
            return false;
        }
    }
}

