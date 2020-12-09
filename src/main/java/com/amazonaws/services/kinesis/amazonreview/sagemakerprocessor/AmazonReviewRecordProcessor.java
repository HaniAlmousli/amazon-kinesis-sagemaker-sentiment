package com.amazonaws.services.kinesis.amazonreview.sagemakerprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.amazonreviews.model.AmazonReview;
import com.amazonaws.services.kinesis_sagemaker.SageMakerEndpointConnection;

import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

public class AmazonReviewRecordProcessor implements ShardRecordProcessor {
	

	private static final Log log = LogFactory.getLog(AmazonReviewRecordProcessor.class);
	private SageMakerEndpointConnection sagemaker_connection;
	
	private String sagemaker_endpoint; 
	public AmazonReviewRecordProcessor(String endpointName) {
		this.sagemaker_endpoint = endpointName;
		sagemaker_connection = new SageMakerEndpointConnection(this.sagemaker_endpoint, "text/csv");
	}
	
	private String kinesisShardId;

	
	@Override
	public void initialize(InitializationInput initializationInput) {
		kinesisShardId = initializationInput.shardId();
		log.info("Initializing record processor for shard: " + kinesisShardId);
		log.info("Initializing @ Sequence: " + initializationInput.extendedSequenceNumber().toString());

		System.out.println("Initializing record processor for shard: " + kinesisShardId);
		System.out.println("Initializing @ Sequence: " + initializationInput.extendedSequenceNumber().toString());

	}

	private void processRecord(KinesisClientRecord record) {
		byte[] arr = new byte[record.data().remaining()];
		record.data().get(arr);
		
		AmazonReview review = AmazonReview.fromJsonAsBytes(arr);
		if (review == null) {
			log.warn("Skipping record. Unable to parse record into AmazonReview. Partition Key: "
					+ record.partitionKey());
			return;
		}
		String res = this.sagemaker_connection.Invoke(review.getReviewText());
		log.info("Processed " + review.getReviewerID() + ", " + review.getAsin());
		System.out.println("Processed " + review.getReviewerID() + ", " + review.getAsin()+ ", Predictions:" + res);
	}

	@Override
	public void processRecords(ProcessRecordsInput processRecordsInput) {
		try {
			System.out.println("Processing " + processRecordsInput.records().size() + " record(s)");
			processRecordsInput.records().forEach(r -> processRecord(r));

			checkpoint(processRecordsInput.checkpointer());

		} catch (Exception t) {
			log.error("Caught throwable while processing records. Aborting.");
			System.out.println("Caught throwable while processing records. Aborting.");
			Runtime.getRuntime().halt(1);
		}
	}

	private void checkpoint(RecordProcessorCheckpointer checkpointer) {
		log.info("Checkpointing shard " + kinesisShardId);
		try {
			checkpointer.checkpoint();
		} catch (ShutdownException se) {
			// Ignore checkpoint if the processor instance has been shutdown (fail over).
			log.info("Caught shutdown exception, skipping checkpoint.", se);
		} catch (ThrottlingException e) {
			// Skip checkpoint when throttled. In practice, consider a backoff and retry
			// policy.
			log.error("Caught throttling exception, skipping checkpoint.", e);
		} catch (InvalidStateException e) {
			// This indicates an issue with the DynamoDB table (check for table, provisioned
			// IOPS).
			log.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
		}
	}

	@Override
	public void leaseLost(LeaseLostInput leaseLostInput) {
		log.info("Lost lease, so terminating.");
	}

	@Override
	public void shardEnded(ShardEndedInput shardEndedInput) {
		try {
			// Important to checkpoint after reaching end of shard, so we can start
			// processing data from child shards.
			log.info("Reached shard end checkpointing.");
			shardEndedInput.checkpointer().checkpoint();
		} catch (ShutdownException | InvalidStateException e) {
			log.error("Exception while checkpointing at shard end. Giving up.", e);
		}
	}

	@Override
	public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
		log.info("Scheduler is shutting down, checkpointing.");
		checkpoint(shutdownRequestedInput.checkpointer());

	}
}
