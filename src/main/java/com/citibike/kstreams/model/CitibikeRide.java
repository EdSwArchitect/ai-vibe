package com.citibike.kstreams.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;

public class CitibikeRide {
    @JsonProperty("ride_id")
    private String rideId;
    
    @JsonProperty("rideable_type")
    private String rideableType;
    
    @JsonProperty("started_at")
    private String startedAt;
    
    @JsonProperty("ended_at")
    private String endedAt;
    
    @JsonProperty("start_station_name")
    private String startStationName;
    
    @JsonProperty("start_station_id")
    private String startStationId;
    
    @JsonProperty("end_station_name")
    private String endStationName;
    
    @JsonProperty("end_station_id")
    private String endStationId;
    
    @JsonProperty("start_lat")
    private Double startLat;
    
    @JsonProperty("start_lng")
    private Double startLng;
    
    @JsonProperty("end_lat")
    private Double endLat;
    
    @JsonProperty("end_lng")
    private Double endLng;
    
    @JsonProperty("member_casual")
    private String memberCasual;

    public CitibikeRide() {
    }

    public String getRideId() {
        return rideId;
    }

    public void setRideId(String rideId) {
        this.rideId = rideId;
    }

    public String getRideableType() {
        return rideableType;
    }

    public void setRideableType(String rideableType) {
        this.rideableType = rideableType;
    }

    public String getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(String startedAt) {
        this.startedAt = startedAt;
    }

    public String getEndedAt() {
        return endedAt;
    }

    public void setEndedAt(String endedAt) {
        this.endedAt = endedAt;
    }

    public String getStartStationName() {
        return startStationName;
    }

    public void setStartStationName(String startStationName) {
        this.startStationName = startStationName;
    }

    public String getStartStationId() {
        return startStationId;
    }

    public void setStartStationId(String startStationId) {
        this.startStationId = startStationId;
    }

    public String getEndStationName() {
        return endStationName;
    }

    public void setEndStationName(String endStationName) {
        this.endStationName = endStationName;
    }

    public String getEndStationId() {
        return endStationId;
    }

    public void setEndStationId(String endStationId) {
        this.endStationId = endStationId;
    }

    public Double getStartLat() {
        return startLat;
    }

    public void setStartLat(Double startLat) {
        this.startLat = startLat;
    }

    public Double getStartLng() {
        return startLng;
    }

    public void setStartLng(Double startLng) {
        this.startLng = startLng;
    }

    public Double getEndLat() {
        return endLat;
    }

    public void setEndLat(Double endLat) {
        this.endLat = endLat;
    }

    public Double getEndLng() {
        return endLng;
    }

    public void setEndLng(Double endLng) {
        this.endLng = endLng;
    }

    public String getMemberCasual() {
        return memberCasual;
    }

    public void setMemberCasual(String memberCasual) {
        this.memberCasual = memberCasual;
    }

    @Override
    public String toString() {
        return "CitibikeRide{" +
                "rideId='" + rideId + '\'' +
                ", startLat=" + startLat +
                ", startLng=" + startLng +
                ", endLat=" + endLat +
                ", endLng=" + endLng +
                '}';
    }
}

