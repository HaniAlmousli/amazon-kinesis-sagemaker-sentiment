
package com.amazonaws.services.kinesis.amazonreviews.model;

import java.io.IOException;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 	Captures basic information about an amazon review
 */
public class AmazonReview {

    private final static ObjectMapper JSON = new ObjectMapper();
    static {
        JSON.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

  
    private Float overall;
    private String reviewerID;
    private String asin;
    private String reviewText;
    private Long unixReviewTime;
    

    public AmazonReview() {
    }

    public AmazonReview(Float overall, String reviewerID, String asin, String reviewText, long unixReviewTime) {
        this.overall= overall;
        this.reviewerID = reviewerID;
        this.asin= asin;
        this.reviewText = reviewText;
        this.unixReviewTime = unixReviewTime;
    }

    
    
    public Float getOverall() {
        return this.overall;
    }
    public String getReviewerID() {
        return this.reviewerID;
    }
    public String getAsin() {
        return this.asin;
    }
    public String getReviewText() {
        return this.reviewText;
    }
    public Long getUnixReviewTime() {
        return this.unixReviewTime;
    }

    
    public byte[] toJsonAsBytes() {
        try {
            return JSON.writeValueAsBytes(this);
        } catch (IOException e) {
            return null;
        }
    }

    public static AmazonReview fromJsonAsBytes(byte[] bytes) {
        try {
            return JSON.readValue(bytes, AmazonReview.class);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public String toString() {
        return String.format("Overall: %s, ReviwerID: %s, ASIN:  %s, ReviwerText:%s  ReviwerTime: %d",
                overall, reviewerID, asin, reviewText, unixReviewTime);
    }

}
