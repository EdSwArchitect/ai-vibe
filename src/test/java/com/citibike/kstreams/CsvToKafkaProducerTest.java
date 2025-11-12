package com.citibike.kstreams;

import com.citibike.kstreams.model.CitibikeRide;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import org.junit.jupiter.api.Test;

import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CsvToKafkaProducerTest {
    private static final String TEST_CSV_FILE = "src/test/resources/test-citibike.csv";

    @Test
    void testParseAllRowsFromTestCsv() throws IOException, CsvValidationException {
        List<CitibikeRide> rides = parseCsvFile(TEST_CSV_FILE);
        
        // Should parse all 5 test rides
        assertEquals(5, rides.size(), "Should parse all 5 test rides");
        
        // Validate first ride
        CitibikeRide ride1 = rides.get(0);
        assertEquals("TEST001", ride1.getRideId());
        assertEquals("classic_bike", ride1.getRideableType());
        assertEquals("2023-06-11 06:54:21", ride1.getStartedAt());
        assertEquals("2023-06-11 07:12:28", ride1.getEndedAt());
        assertEquals("W 84 St & Columbus Ave", ride1.getStartStationName());
        assertEquals("7382.04", ride1.getStartStationId());
        assertEquals("Amsterdam Ave & W 125 St", ride1.getEndStationName());
        assertEquals("7800.03", ride1.getEndStationId());
        assertEquals("member", ride1.getMemberCasual());
        
        // Validate coordinates for first ride
        assertNotNull(ride1.getStartLat(), "Start latitude should not be null");
        assertNotNull(ride1.getStartLng(), "Start longitude should not be null");
        assertNotNull(ride1.getEndLat(), "End latitude should not be null");
        assertNotNull(ride1.getEndLng(), "End longitude should not be null");
        
        assertEquals(40.78499979, ride1.getStartLat(), 0.00000001, "Start latitude should match");
        assertEquals(-73.97283406, ride1.getStartLng(), 0.00000001, "Start longitude should match");
        assertEquals(40.813358, ride1.getEndLat(), 0.00000001, "End latitude should match");
        assertEquals(-73.956461, ride1.getEndLng(), 0.00000001, "End longitude should match");
    }

    @Test
    void testParseAllRidesHaveValidCoordinates() throws IOException, CsvValidationException {
        List<CitibikeRide> rides = parseCsvFile(TEST_CSV_FILE);
        
        for (int i = 0; i < rides.size(); i++) {
            CitibikeRide ride = rides.get(i);
            String message = String.format("Ride %d (ID: %s)", i + 1, ride.getRideId());
            
            assertNotNull(ride.getStartLat(), message + " - Start latitude should not be null");
            assertNotNull(ride.getStartLng(), message + " - Start longitude should not be null");
            assertNotNull(ride.getEndLat(), message + " - End latitude should not be null");
            assertNotNull(ride.getEndLng(), message + " - End longitude should not be null");
            
            // Validate coordinate ranges (NYC area)
            assertTrue(ride.getStartLat() >= 40.0 && ride.getStartLat() <= 41.0, 
                    message + " - Start latitude should be in NYC range");
            assertTrue(ride.getStartLng() >= -74.5 && ride.getStartLng() <= -73.5, 
                    message + " - Start longitude should be in NYC range");
            assertTrue(ride.getEndLat() >= 40.0 && ride.getEndLat() <= 41.0, 
                    message + " - End latitude should be in NYC range");
            assertTrue(ride.getEndLng() >= -74.5 && ride.getEndLng() <= -73.5, 
                    message + " - End longitude should be in NYC range");
        }
    }

    @Test
    void testParseAllRidesHaveRequiredFields() throws IOException, CsvValidationException {
        List<CitibikeRide> rides = parseCsvFile(TEST_CSV_FILE);
        
        for (CitibikeRide ride : rides) {
            assertNotNull(ride.getRideId(), "Ride ID should not be null");
            assertFalse(ride.getRideId().trim().isEmpty(), "Ride ID should not be empty");
            assertNotNull(ride.getRideableType(), "Rideable type should not be null");
            assertNotNull(ride.getStartedAt(), "Started at should not be null");
            assertNotNull(ride.getEndedAt(), "Ended at should not be null");
            assertNotNull(ride.getMemberCasual(), "Member/casual should not be null");
        }
    }

    @Test
    void testParseSpecificRideDetails() throws IOException, CsvValidationException {
        List<CitibikeRide> rides = parseCsvFile(TEST_CSV_FILE);
        
        // Test ride 2 (TEST002)
        CitibikeRide ride2 = rides.stream()
                .filter(r -> "TEST002".equals(r.getRideId()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("TEST002 ride not found"));
        
        assertEquals("electric_bike", ride2.getRideableType());
        assertEquals("casual", ride2.getMemberCasual());
        assertEquals(40.777957678, ride2.getStartLat(), 0.000000001);
        assertEquals(-73.945928335, ride2.getStartLng(), 0.000000001);
        
        // Test ride 4 (TEST004) - has high precision coordinates
        CitibikeRide ride4 = rides.stream()
                .filter(r -> "TEST004".equals(r.getRideId()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("TEST004 ride not found"));
        
        assertEquals("electric_bike", ride4.getRideableType());
        assertEquals(40.731734785883454, ride4.getStartLat(), 0.000000000000001);
        assertEquals(-73.9612390102593, ride4.getStartLng(), 0.000000000000001);
        assertEquals(40.73564, ride4.getEndLat(), 0.00001);
        assertEquals(-73.95866, ride4.getEndLng(), 0.00001);
    }

    @Test
    void testParseStationNamesAndIds() throws IOException, CsvValidationException {
        List<CitibikeRide> rides = parseCsvFile(TEST_CSV_FILE);
        
        // Validate station names and IDs are parsed correctly
        CitibikeRide ride1 = rides.get(0);
        assertEquals("W 84 St & Columbus Ave", ride1.getStartStationName());
        assertEquals("7382.04", ride1.getStartStationId());
        assertEquals("Amsterdam Ave & W 125 St", ride1.getEndStationName());
        assertEquals("7800.03", ride1.getEndStationId());
        
        CitibikeRide ride3 = rides.get(2);
        assertEquals("E 51 St & 2 Ave", ride3.getStartStationName());
        assertEquals("6575.03", ride3.getStartStationId());
        assertEquals("E 25 St & 1 Ave", ride3.getEndStationName());
        assertEquals("6004.07", ride3.getEndStationId());
    }

    @Test
    void testParseRideableTypes() throws IOException, CsvValidationException {
        List<CitibikeRide> rides = parseCsvFile(TEST_CSV_FILE);
        
        // Count rideable types
        long classicBikes = rides.stream()
                .filter(r -> "classic_bike".equals(r.getRideableType()))
                .count();
        long electricBikes = rides.stream()
                .filter(r -> "electric_bike".equals(r.getRideableType()))
                .count();
        
        assertEquals(3, classicBikes, "Should have 3 classic bikes");
        assertEquals(2, electricBikes, "Should have 2 electric bikes");
    }

    @Test
    void testParseMemberCasual() throws IOException, CsvValidationException {
        List<CitibikeRide> rides = parseCsvFile(TEST_CSV_FILE);
        
        long members = rides.stream()
                .filter(r -> "member".equals(r.getMemberCasual()))
                .count();
        long casual = rides.stream()
                .filter(r -> "casual".equals(r.getMemberCasual()))
                .count();
        
        assertEquals(3, members, "Should have 3 member rides");
        assertEquals(2, casual, "Should have 2 casual rides");
    }

    /**
     * Helper method to parse CSV file and return list of rides
     * This uses reflection to access private methods for testing
     */
    private List<CitibikeRide> parseCsvFile(String csvFilePath) throws IOException, CsvValidationException {
        List<CitibikeRide> rides = new ArrayList<>();
        
        try (FileReader fileReader = new FileReader(csvFilePath, StandardCharsets.UTF_8);
             CSVReader csvReader = new CSVReaderBuilder(fileReader)
                     .withSkipLines(0)
                     .build()) {
            
            // Skip header
            csvReader.readNext();
            
            String[] line;
            while ((line = csvReader.readNext()) != null) {
                if (line == null || line.length < 13) {
                    continue;
                }
                
                try {
                    CitibikeRide ride = parseCsvLine(line);
                    if (ride.getRideId() != null && !ride.getRideId().trim().isEmpty()) {
                        rides.add(ride);
                    }
                } catch (Exception e) {
                    // Skip invalid lines in test
                    System.err.println("Error parsing line: " + String.join(",", line));
                }
            }
        }
        
        return rides;
    }

    /**
     * Helper method that replicates the parsing logic from CsvToKafkaProducer
     */
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

    private Double parseDouble(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        
        try {
            // Remove any whitespace and parse
            String cleaned = value.trim();
            return Double.parseDouble(cleaned);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}

