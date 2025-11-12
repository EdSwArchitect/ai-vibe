package com.citibike.kstreams.metrics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Grafana dashboard configuration and Prometheus queries.
 */
class GrafanaDashboardTest {
    private static final String DASHBOARD_PATH = "grafana/dashboards/citibike-metrics.json";
    private static final String PROMETHEUS_CONFIG_PATH = "prometheus.yml";
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testDashboardJsonIsValid() throws IOException {
        File dashboardFile = new File(DASHBOARD_PATH);
        assertTrue(dashboardFile.exists(), "Dashboard file should exist");
        
        JsonNode dashboard = objectMapper.readTree(dashboardFile);
        assertNotNull(dashboard, "Dashboard JSON should be valid");
    }

    @Test
    void testDashboardHasRequiredFields() throws IOException {
        File dashboardFile = new File(DASHBOARD_PATH);
        JsonNode dashboard = objectMapper.readTree(dashboardFile);
        
        assertTrue(dashboard.has("title"), "Dashboard should have title");
        assertTrue(dashboard.has("panels"), "Dashboard should have panels");
        assertTrue(dashboard.has("refresh"), "Dashboard should have refresh interval");
        
        assertEquals("Citibike Kafka Streams Metrics", dashboard.get("title").asText(),
                "Dashboard title should match");
    }

    @Test
    void testDashboardHasAllRequiredPanels() throws IOException {
        File dashboardFile = new File(DASHBOARD_PATH);
        JsonNode dashboard = objectMapper.readTree(dashboardFile);
        JsonNode panels = dashboard.get("panels");
        
        assertNotNull(panels, "Dashboard should have panels array");
        assertTrue(panels.isArray(), "Panels should be an array");
        assertTrue(panels.size() > 0, "Dashboard should have at least one panel");
        
        // Collect panel titles
        List<String> panelTitles = new ArrayList<>();
        for (JsonNode panel : panels) {
            if (panel.has("title")) {
                panelTitles.add(panel.get("title").asText());
            }
        }
        
        // Verify key panels exist
        assertTrue(panelTitles.stream().anyMatch(t -> t.contains("Locations")),
                "Should have locations panel");
        assertTrue(panelTitles.stream().anyMatch(t -> t.contains("Rides")),
                "Should have rides panel");
        assertTrue(panelTitles.stream().anyMatch(t -> t.contains("Lookups")),
                "Should have lookups panel");
    }

    @Test
    void testDashboardPrometheusQueriesAreValid() throws IOException {
        File dashboardFile = new File(DASHBOARD_PATH);
        JsonNode dashboard = objectMapper.readTree(dashboardFile);
        JsonNode panels = dashboard.get("panels");
        
        List<String> queries = new ArrayList<>();
        
        for (JsonNode panel : panels) {
            if (panel.has("targets")) {
                JsonNode targets = panel.get("targets");
                for (JsonNode target : targets) {
                    if (target.has("expr")) {
                        queries.add(target.get("expr").asText());
                    }
                }
            }
        }
        
        assertFalse(queries.isEmpty(), "Should have at least one Prometheus query");
        
        // Verify queries reference our metrics
        boolean hasLocationsQuery = queries.stream()
                .anyMatch(q -> q.contains("citibike_locations_count"));
        assertTrue(hasLocationsQuery, "Should have locations count query");
        
        boolean hasRidesQuery = queries.stream()
                .anyMatch(q -> q.contains("citibike_rides"));
        assertTrue(hasRidesQuery, "Should have rides query");
        
        boolean hasLookupsQuery = queries.stream()
                .anyMatch(q -> q.contains("citibike_location_lookups"));
        assertTrue(hasLookupsQuery, "Should have lookups query");
    }

    @Test
    void testDashboardQueriesMatchMetrics() throws IOException {
        File dashboardFile = new File(DASHBOARD_PATH);
        JsonNode dashboard = objectMapper.readTree(dashboardFile);
        JsonNode panels = dashboard.get("panels");
        
        // Expected metric names
        String[] expectedMetrics = {
            "citibike_locations_count",
            "citibike_rides_count",
            "citibike_rides_processed_total",
            "citibike_rides_parsed_total",
            "citibike_rides_parse_failed_total",
            "citibike_location_lookups_total",
            "citibike_location_lookups_successful_total",
            "citibike_location_lookups_failed_total",
            "citibike_location_lookup_duration_seconds"
        };
        
        // Collect all queries
        List<String> allQueries = new ArrayList<>();
        for (JsonNode panel : panels) {
            if (panel.has("targets")) {
                JsonNode targets = panel.get("targets");
                for (JsonNode target : targets) {
                    if (target.has("expr")) {
                        allQueries.add(target.get("expr").asText());
                    }
                }
            }
        }
        
        // Verify each expected metric is referenced in at least one query
        for (String metric : expectedMetrics) {
            boolean found = allQueries.stream()
                    .anyMatch(q -> q.contains(metric));
            assertTrue(found, 
                    String.format("Dashboard should have query referencing metric: %s", metric));
        }
    }

    @Test
    void testDashboardHasRateQueries() throws IOException {
        File dashboardFile = new File(DASHBOARD_PATH);
        JsonNode dashboard = objectMapper.readTree(dashboardFile);
        JsonNode panels = dashboard.get("panels");
        
        List<String> rateQueries = new ArrayList<>();
        for (JsonNode panel : panels) {
            if (panel.has("targets")) {
                JsonNode targets = panel.get("targets");
                for (JsonNode target : targets) {
                    if (target.has("expr")) {
                        String expr = target.get("expr").asText();
                        if (expr.contains("rate(")) {
                            rateQueries.add(expr);
                        }
                    }
                }
            }
        }
        
        assertFalse(rateQueries.isEmpty(), 
                "Dashboard should have rate() queries for per-second calculations");
        
        // Verify rate queries use appropriate time windows
        boolean hasOneMinuteRate = rateQueries.stream()
                .anyMatch(q -> q.contains("[1m]"));
        assertTrue(hasOneMinuteRate, 
                "Should have rate queries with 1 minute window");
    }

    @Test
    void testPrometheusConfigIsValid() throws IOException {
        File prometheusFile = new File(PROMETHEUS_CONFIG_PATH);
        assertTrue(prometheusFile.exists(), "Prometheus config file should exist");
        
        String content = Files.readString(Paths.get(PROMETHEUS_CONFIG_PATH));
        assertNotNull(content, "Prometheus config should not be null");
        assertFalse(content.isEmpty(), "Prometheus config should not be empty");
        
        // Verify key configuration
        assertTrue(content.contains("scrape_configs"), 
                "Should contain scrape_configs");
        assertTrue(content.contains("citibike-app"), 
                "Should contain citibike-app job");
        assertTrue(content.contains("8081"), 
                "Should contain metrics port 8081");
    }

    @Test
    void testPrometheusConfigHasCorrectTarget() throws IOException {
        String content = Files.readString(Paths.get(PROMETHEUS_CONFIG_PATH));
        
        // Verify target configuration
        assertTrue(content.contains("host.docker.internal:8081") || 
                  content.contains("localhost:8081"),
                "Should have correct metrics endpoint target");
    }

    @Test
    void testDashboardRefreshInterval() throws IOException {
        File dashboardFile = new File(DASHBOARD_PATH);
        JsonNode dashboard = objectMapper.readTree(dashboardFile);
        
        if (dashboard.has("refresh")) {
            String refresh = dashboard.get("refresh").asText();
            assertNotNull(refresh, "Refresh interval should be set");
            // Should be a valid Grafana refresh interval (e.g., "10s", "30s", "1m")
            assertTrue(refresh.matches("\\d+[smh]"), 
                    "Refresh interval should be in valid format (e.g., 10s, 1m)");
        }
    }

    @Test
    void testDashboardTimeRange() throws IOException {
        File dashboardFile = new File(DASHBOARD_PATH);
        JsonNode dashboard = objectMapper.readTree(dashboardFile);
        
        if (dashboard.has("time")) {
            JsonNode time = dashboard.get("time");
            if (time.has("from") && time.has("to")) {
                String from = time.get("from").asText();
                String to = time.get("to").asText();
                
                assertNotNull(from, "Time 'from' should be set");
                assertNotNull(to, "Time 'to' should be set");
            }
        }
    }

    @Test
    void testDashboardPanelsHaveValidDataSources() throws IOException {
        File dashboardFile = new File(DASHBOARD_PATH);
        JsonNode dashboard = objectMapper.readTree(dashboardFile);
        JsonNode panels = dashboard.get("panels");
        
        for (JsonNode panel : panels) {
            if (panel.has("datasource")) {
                JsonNode datasource = panel.get("datasource");
                if (datasource.has("type")) {
                    String type = datasource.get("type").asText();
                    assertEquals("prometheus", type, 
                            "Panel datasource should be Prometheus");
                }
            }
        }
    }
}

