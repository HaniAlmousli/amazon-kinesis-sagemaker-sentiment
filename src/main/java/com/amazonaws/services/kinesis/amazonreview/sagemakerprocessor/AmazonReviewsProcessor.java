package com.amazonaws.services.kinesis.amazonreview.sagemakerprocessor;

//AmazonReviewsPred AmazonReviewsStream us-east2 bert-sentiment-poc-2020-12-07-19-15-27-426

import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.KinesisClientUtil;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.retrieval.RetrievalConfig;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

public class AmazonReviewsProcessor {
	private static final Log LOG = LogFactory.getLog(AmazonReviewsProcessor.class);

    private static final Logger ROOT_LOGGER = Logger.getLogger("");
    private static final Logger PROCESSOR_LOGGER =
            Logger.getLogger("com.amazonaws.services.kinesis.amazonreview.sagemakerprocessor.AmazonReviewRecordProcessor");

    private static void checkUsage(String[] args) {
        if (args.length != 4) {
            System.err.println("Usage: " + AmazonReviewsProcessor.class.getSimpleName()
                    + " <application name> <stream name> <region> <sagemaker deployment endpoint>");
            System.exit(1);
        }
    }

    /**
     * Sets the global log level to WARNING and the log level for this package to INFO,
     * so that we only see INFO messages for this processor. This is just for the purpose
     * of this tutorial, and should not be considered as best practice.
     *
     */
    private static void setLogLevels() {
        ROOT_LOGGER.setLevel(Level.WARNING);
        // Set this to INFO for logging at INFO level. Suppressed for this example as it can be noisy.
        PROCESSOR_LOGGER.setLevel(Level.WARNING);
    }

    public static void main(String[] args) throws Exception {
        checkUsage(args);

        setLogLevels();

        String applicationName = args[0];
        String streamName = args[1];
        Region region = Region.of(args[2]);
        String sageMakerEndpoint= args[3];

        if (region == null) {
            System.err.println(args[2] + " is not a valid AWS region.");
            System.exit(1);
        }
        
        KinesisAsyncClient kinesisClient = KinesisClientUtil.createKinesisAsyncClient(KinesisAsyncClient.builder().region(region));
        DynamoDbAsyncClient dynamoClient = DynamoDbAsyncClient.builder().region(region).build();
        CloudWatchAsyncClient cloudWatchClient = CloudWatchAsyncClient.builder().region(region).build();
        AmazonReviewRecordProcessorFactory shardRecordProcessor = new AmazonReviewRecordProcessorFactory(sageMakerEndpoint);
        
        ConfigsBuilder configsBuilder = new ConfigsBuilder(streamName, applicationName, 
        													kinesisClient, dynamoClient, 
        													cloudWatchClient, UUID.randomUUID().toString(), 
        													shardRecordProcessor);
	    InitialPositionInStreamExtended initialPositionInStreamExtended = InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON);
	    RetrievalConfig retrievalConfig = configsBuilder.retrievalConfig().retrievalSpecificConfig(new PollingConfig(streamName, kinesisClient));
	    retrievalConfig.initialPositionInStreamExtended(initialPositionInStreamExtended);
	
	    
	    /*	
        CoordinatorConfig c= configsBuilder.coordinatorConfig();
        c.shardConsumerDispatchPollIntervalMillis(400);
        
        configsBuilder.retrievalConfig().initialPositionInStreamExtended(
        	    InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON)
        	);
	     */  
	   
	    Scheduler scheduler = new Scheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig(),
                configsBuilder.processorConfig(),
                retrievalConfig
        );
        
        
        int exitCode = 0;
        try {
            scheduler.run();
        } catch (Throwable t) {
            LOG.error("Caught throwable while processing data.", t);
            exitCode = 1;
        }
        System.exit(exitCode);

    }
}
