package com.amazonaws.services.kinesis.amazonreview.writer;

//AmazonReviewsStream us-east2

import java.util.concurrent.ExecutionException;



import software.amazon.awssdk.core.SdkBytes;

import software.amazon.awssdk.regions.Region;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.amazonreviews.model.AmazonReview;

import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.KinesisClientUtil;
import java.io.BufferedReader;
import java.io.InputStreamReader; 
/**
 * Continuously sends simulated stock trades to Kinesis
 *
 */
public class AmazonReviewsWriter {

    private static final Log LOG = LogFactory.getLog(AmazonReviewsWriter .class);

    private static void checkUsage(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: " + AmazonReviewsWriter .class.getSimpleName()
                    + " <stream name> <region>");
            System.exit(1);
        }
    }

    /**
     * Checks if the stream exists and is active
     *
     * @param kinesisClient Amazon Kinesis client instance
     * @param streamName Name of stream
     */
    private static void validateStream(KinesisAsyncClient kinesisClient, String streamName) {
        try {
            DescribeStreamRequest describeStreamRequest =  DescribeStreamRequest.builder().streamName(streamName).build();
            DescribeStreamResponse describeStreamResponse = kinesisClient.describeStream(describeStreamRequest).get();
            if(!describeStreamResponse.streamDescription().streamStatus().toString().equals("ACTIVE")) {
                System.err.println("Stream " + streamName + " is not active. Please wait a few moments and try again.");
                System.exit(1);
            }
        }catch (Exception e) {
            System.err.println("Error found while describing the stream " + streamName);
            System.err.println(e);
            System.exit(1);
        }
    }

    /**
     * Uses the Kinesis client to send the stock trade to the given stream.
     *
     * @param review instance representing the amazon review
     * @param kinesisClient Amazon Kinesis client
     * @param streamName Name of stream
     */

    private static void sendAmazonReview(AmazonReview review, KinesisAsyncClient kinesisClient,
            String streamName) {
        byte[] bytes = review.toJsonAsBytes();
        // The bytes could be null if there is an issue with the JSON serialization by the Jackson JSON library.
        if (bytes == null) {
            LOG.warn("Could not get JSON bytes for amazon review");
            return;
        }
        LOG.info("Putting a review: " + review.toString());
        PutRecordRequest request = PutRecordRequest.builder()
                .partitionKey(review.getReviewerID()) // The reviewer id will be used as a partition key
                .streamName(streamName)
                .data(SdkBytes.fromByteArray(bytes))
                .build();
        try {
            PutRecordResponse response=  kinesisClient.putRecord(request).get();
            LOG.info("Added at Shard: "+ response.shardId());
        } catch (InterruptedException e) {
            LOG.info("Interrupted, assuming shutdown.");
        } catch (ExecutionException e) {
            LOG.error("Exception while sending data to Kinesis. Will try again next cycle.", e);
        }
    }
                    
    public static void main(String[] args) throws Exception {
    	
        checkUsage(args);
        
        String streamName = args[0];
        String regionName = args[1];
        Region region = Region.of(regionName);
        if (region == null) {
            System.err.println(regionName + " is not a valid AWS region.");
            System.exit(1);
        }

        KinesisAsyncClient kinesisClient = KinesisClientUtil.createKinesisAsyncClient(KinesisAsyncClient.builder().region(region));

        // Validate that the stream exists and is active
        validateStream(kinesisClient, streamName);

        // Repeatedly send reviews 
        AmazonReviewGenerator amazonReviewGenerator = new AmazonReviewGenerator();
        //Take input from a user to add one review at a time. This shows easily what is going on
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        while(br.readLine().equals("c")) {
            AmazonReview review = amazonReviewGenerator.getRandomReview();
            sendAmazonReview(review, kinesisClient, streamName);
            Thread.sleep(100);
        }
    }

}
