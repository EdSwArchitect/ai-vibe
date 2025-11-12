package com.citibike.kstreams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Location {
    @JsonProperty("place_id")
    private Long placeId;
    
    private String licence;
    
    @JsonProperty("osm_type")
    private String osmType;
    
    @JsonProperty("osm_id")
    private Long osmId;
    
    private String lat;
    
    private String lon;
    
    @JsonProperty("display_name")
    private String displayName;
    
    private Address address;
    
    private String[] boundingbox;

    public Location() {
    }

    public Long getPlaceId() {
        return placeId;
    }

    public void setPlaceId(Long placeId) {
        this.placeId = placeId;
    }

    public String getLicence() {
        return licence;
    }

    public void setLicence(String licence) {
        this.licence = licence;
    }

    public String getOsmType() {
        return osmType;
    }

    public void setOsmType(String osmType) {
        this.osmType = osmType;
    }

    public Long getOsmId() {
        return osmId;
    }

    public void setOsmId(Long osmId) {
        this.osmId = osmId;
    }

    public String getLat() {
        return lat;
    }

    public void setLat(String lat) {
        this.lat = lat;
    }

    public String getLon() {
        return lon;
    }

    public void setLon(String lon) {
        this.lon = lon;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public Address getAddress() {
        return address;
    }

    public void setAddress(Address address) {
        this.address = address;
    }

    public String[] getBoundingbox() {
        return boundingbox;
    }

    public void setBoundingbox(String[] boundingbox) {
        this.boundingbox = boundingbox;
    }

    public Double getLatAsDouble() {
        if (lat == null || lat.trim().isEmpty()) {
            return null;
        }
        try {
            return Double.parseDouble(lat);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public Double getLonAsDouble() {
        if (lon == null || lon.trim().isEmpty()) {
            return null;
        }
        try {
            return Double.parseDouble(lon);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Address {
        private String amenity;
        @JsonProperty("house_number")
        private String houseNumber;
        private String road;
        private String neighbourhood;
        private String suburb;
        private String county;
        private String city;
        private String state;
        @JsonProperty("ISO3166-2-lvl4")
        private String iso3166;
        private String postcode;
        private String country;
        @JsonProperty("country_code")
        private String countryCode;
        private String building;
        private String tourism;

        public Address() {
        }

        public String getAmenity() {
            return amenity;
        }

        public void setAmenity(String amenity) {
            this.amenity = amenity;
        }

        public String getHouseNumber() {
            return houseNumber;
        }

        public void setHouseNumber(String houseNumber) {
            this.houseNumber = houseNumber;
        }

        public String getRoad() {
            return road;
        }

        public void setRoad(String road) {
            this.road = road;
        }

        public String getNeighbourhood() {
            return neighbourhood;
        }

        public void setNeighbourhood(String neighbourhood) {
            this.neighbourhood = neighbourhood;
        }

        public String getSuburb() {
            return suburb;
        }

        public void setSuburb(String suburb) {
            this.suburb = suburb;
        }

        public String getCounty() {
            return county;
        }

        public void setCounty(String county) {
            this.county = county;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }

        public String getState() {
            return state;
        }

        public void setState(String state) {
            this.state = state;
        }

        public String getIso3166() {
            return iso3166;
        }

        public void setIso3166(String iso3166) {
            this.iso3166 = iso3166;
        }

        public String getPostcode() {
            return postcode;
        }

        public void setPostcode(String postcode) {
            this.postcode = postcode;
        }

        public String getCountry() {
            return country;
        }

        public void setCountry(String country) {
            this.country = country;
        }

        public String getCountryCode() {
            return countryCode;
        }

        public void setCountryCode(String countryCode) {
            this.countryCode = countryCode;
        }

        public String getBuilding() {
            return building;
        }

        public void setBuilding(String building) {
            this.building = building;
        }

        public String getTourism() {
            return tourism;
        }

        public void setTourism(String tourism) {
            this.tourism = tourism;
        }
    }
}

