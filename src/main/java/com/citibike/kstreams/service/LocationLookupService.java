package com.citibike.kstreams.service;

import com.citibike.kstreams.model.Location;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocationLookupService {
    private static final Logger logger = LoggerFactory.getLogger(LocationLookupService.class);
    private static final double DISTANCE_THRESHOLD_KM = 1.0; // 1 kilometer threshold
    
    private final Map<String, Location> locationCache;
    private final List<Location> locations;
    private final ObjectMapper objectMapper;

    public LocationLookupService(String locationsFilePath) throws IOException {
        this.objectMapper = new ObjectMapper();
        this.locationCache = new ConcurrentHashMap<>();
        this.locations = loadLocations(locationsFilePath);
        logger.info("Loaded " + locations.size() + " locations from " + locationsFilePath);
    }

    private List<Location> loadLocations(String filePath) throws IOException {
        File file = new File(filePath);
        Location[] locationArray = objectMapper.readValue(file, Location[].class);
        return List.of(locationArray);
    }

    /**
     * Find the closest location to the given coordinates
     */
    public Location findClosestLocation(Double lat, Double lng) {
        if (lat == null || lng == null) {
            return null;
        }

        String cacheKey = String.format("%.4f,%.4f", lat, lng);
        Location cached = locationCache.get(cacheKey);
        if (cached != null) {
            return cached;
        }

        Location closest = null;
        double minDistance = Double.MAX_VALUE;

        for (Location location : locations) {
            Double locLat = location.getLatAsDouble();
            Double locLng = location.getLonAsDouble();

            if (locLat == null || locLng == null) {
                continue;
            }

            double distance = calculateDistance(lat, lng, locLat, locLng);
            if (distance < minDistance && distance <= DISTANCE_THRESHOLD_KM) {
                minDistance = distance;
                closest = location;
            }
        }

        if (closest != null) {
            locationCache.put(cacheKey, closest);
        }

        return closest;
    }

    /**
     * Calculate distance between two coordinates using Haversine formula
     */
    private double calculateDistance(double lat1, double lon1, double lat2, double lon2) {
        final int R = 6371; // Radius of the earth in km
        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c;
    }

    public List<Location> getAllLocations() {
        return new ArrayList<>(locations);
    }
}

