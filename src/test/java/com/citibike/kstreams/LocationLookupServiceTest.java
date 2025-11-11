package com.citibike.kstreams;

import com.citibike.kstreams.model.Location;
import com.citibike.kstreams.service.LocationLookupService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LocationLookupServiceTest {
    private LocationLookupService locationService;

    @BeforeEach
    void setUp() throws Exception {
        String testLocationsPath = "src/test/resources/test-locations.json";
        locationService = new LocationLookupService(testLocationsPath);
    }

    @Test
    void testFindClosestLocation() {
        // Test with coordinates close to W 84th St location
        Location location = locationService.findClosestLocation(40.78499979, -73.97283406);
        
        assertNotNull(location);
        assertEquals("Citi Bike, West 84th Street", location.getDisplayName().substring(0, 30));
    }

    @Test
    void testFindClosestLocationForEast89th() {
        // Test with coordinates close to E 89th St location
        Location location = locationService.findClosestLocation(40.777957678, -73.945928335);
        
        assertNotNull(location);
        assertTrue(location.getDisplayName().contains("East 89th Street"));
    }

    @Test
    void testFindClosestLocationWithNullCoordinates() {
        Location location = locationService.findClosestLocation(null, null);
        assertNull(location);
    }

    @Test
    void testFindClosestLocationWithDistantCoordinates() {
        // Coordinates far from any location
        Location location = locationService.findClosestLocation(0.0, 0.0);
        assertNull(location);
    }
}

