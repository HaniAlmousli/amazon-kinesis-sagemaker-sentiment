package com.amazonaws.services.kinesis.amazonreview.sagemakerprocessor;

import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

public class AmazonReviewRecordProcessorFactory implements ShardRecordProcessorFactory {

	private String sagemaker_endpoint;

	public AmazonReviewRecordProcessorFactory(String sagemaker_endpoint) {
		this.sagemaker_endpoint=sagemaker_endpoint;
	}

	@Override
	public ShardRecordProcessor shardRecordProcessor() {
		return new AmazonReviewRecordProcessor(this.sagemaker_endpoint);
	}

}
