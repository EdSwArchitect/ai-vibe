package com.citibike.kstreams.transformer;

import com.citibike.kstreams.metrics.MetricsService;
import com.citibike.kstreams.model.CitibikeRide;
import com.citibike.kstreams.model.Location;
import com.citibike.kstreams.model.RideWithLocation;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Transformer that enriches CitibikeRide with location data from a GlobalKTable store.
 */
public class LocationLookupTransformer implements Transformer<String, String, KeyValue<String, String>> {
    private static final Logger logger = LoggerFactory.getLogger(LocationLookupTransformer.class);
    private static final double DISTANCE_THRESHOLD_KM = 1.0;
    
    private KeyValueStore<String, String> locationStore;
    private final ObjectMapper objectMapper;
    private final String storeName;
    private final MetricsService metricsService;

    public LocationLookupTransformer(String storeName) {
        this.storeName = storeName;
        this.objectMapper = new ObjectMapper();
        this.metricsService = MetricsService.getInstance();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.locationStore = (KeyValueStore<String, String>) context.getStateStore(storeName);
    }

    @Override
    public KeyValue<String, String> transform(String key, String value) {
        metricsService.incrementRidesProcessed();
        try {
            // Deserialize CitibikeRide from JSON
            CitibikeRide ride = objectMapper.readValue(value, CitibikeRide.class);
            metricsService.incrementRidesParsed();
            
            // Lookup start location
            var startTimer = metricsService.startLookupTimer();
            metricsService.incrementLookups();
            Location startLocation = findClosestLocation(ride.getStartLat(), ride.getStartLng());
            metricsService.recordLookupDuration(startTimer);
            if (startLocation != null) {
                metricsService.incrementLookupsSuccessful();
            } else {
                metricsService.incrementLookupsFailed();
            }
            
            // Lookup end location
            var endTimer = metricsService.startLookupTimer();
            metricsService.incrementLookups();
            Location endLocation = findClosestLocation(ride.getEndLat(), ride.getEndLng());
            metricsService.recordLookupDuration(endTimer);
            if (endLocation != null) {
                metricsService.incrementLookupsSuccessful();
            } else {
                metricsService.incrementLookupsFailed();
            }
            
            // Create enriched object
            RideWithLocation rideWithLocation = new RideWithLocation(
                    ride, startLocation, endLocation);
            
            // Serialize to JSON
            String outputValue = objectMapper.writeValueAsString(rideWithLocation);
            
            return KeyValue.pair(key, outputValue);
        } catch (Exception e) {
            metricsService.incrementRidesParseFailed();
            logger.error("Error processing ride: {}", value, e);
            return null; // Filter out errors
        }
    }

    @Override
    public void close() {
        // Cleanup if needed
    }

    /**
     * Find the closest location to the given coordinates by scanning the location store.
     */
    private Location findClosestLocation(Double lat, Double lng) {
        if (lat == null || lng == null || locationStore == null) {
            return null;
        }

        Location closest = null;
        double minDistance = Double.MAX_VALUE;

        // Iterate through all locations in the store
        // Note: This is a full scan, which may be slow for large datasets
        // In production, consider using a spatial index or pre-computed grid
        try {
            List<Location> nearbyLocations = getNearbyLocations(lat, lng);
            
            for (Location location : nearbyLocations) {
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
        } catch (Exception e) {
            logger.error("Error finding closest location", e);
        }

        return closest;
    }

    /**
     * Get locations from nearby grid cells to optimize lookup.
     * This reduces the number of locations we need to check.
     */
    private List<Location> getNearbyLocations(Double lat, Double lng) {
        List<Location> locations = new ArrayList<>();
        double gridPrecision = 0.01; // ~1km
        
        // Check current grid cell and adjacent cells
        for (double latOffset = -gridPrecision; latOffset <= gridPrecision; latOffset += gridPrecision) {
            for (double lngOffset = -gridPrecision; lngOffset <= gridPrecision; lngOffset += gridPrecision) {
                // Note: KeyValueStore doesn't support prefix queries directly,
                // so we'll need to scan all entries. For better performance in production,
                // consider using a custom state store with spatial indexing
            }
        }
        
        // Since KeyValueStore doesn't support efficient prefix queries,
        // we'll need to scan all entries. For better performance, consider:
        // 1. Using a custom state store with spatial indexing
        // 2. Pre-computing location mappings
        // 3. Using a different data structure
        
        // For this implementation, we'll scan all locations
        // In production, you'd want to optimize this
        try {
            // Get all locations from store
            // Note: This requires iterating through all keys, which may be slow
            locationStore.all().forEachRemaining(entry -> {
                try {
                    Location location = objectMapper.readValue(entry.value, Location.class);
                    locations.add(location);
                } catch (Exception e) {
                    logger.warn("Error deserializing location", e);
                }
            });
        } catch (Exception e) {
            logger.error("Error reading locations from store", e);
        }
        
        return locations;
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
}

