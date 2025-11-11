package com.citibike.kstreams.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RideWithLocation {
    private CitibikeRide ride;
    
    @JsonProperty("start_location")
    private Location startLocation;
    
    @JsonProperty("end_location")
    private Location endLocation;

    public RideWithLocation() {
    }

    public RideWithLocation(CitibikeRide ride, Location startLocation, Location endLocation) {
        this.ride = ride;
        this.startLocation = startLocation;
        this.endLocation = endLocation;
    }

    public CitibikeRide getRide() {
        return ride;
    }

    public void setRide(CitibikeRide ride) {
        this.ride = ride;
    }

    public Location getStartLocation() {
        return startLocation;
    }

    public void setStartLocation(Location startLocation) {
        this.startLocation = startLocation;
    }

    public Location getEndLocation() {
        return endLocation;
    }

    public void setEndLocation(Location endLocation) {
        this.endLocation = endLocation;
    }

    @Override
    public String toString() {
        return "RideWithLocation{" +
                "rideId='" + (ride != null ? ride.getRideId() : "null") + '\'' +
                ", startLocation=" + (startLocation != null ? startLocation.getDisplayName() : "null") +
                ", endLocation=" + (endLocation != null ? endLocation.getDisplayName() : "null") +
                '}';
    }
}

