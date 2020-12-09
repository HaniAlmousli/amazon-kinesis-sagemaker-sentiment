package com.amazonaws.services.kinesis_sagemaker;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.sagemakerruntime.AmazonSageMakerRuntime;
import com.amazonaws.services.sagemakerruntime.AmazonSageMakerRuntimeClientBuilder;
import com.amazonaws.services.sagemakerruntime.model.InvokeEndpointRequest;
import com.amazonaws.services.sagemakerruntime.model.InvokeEndpointResult;

public class SageMakerEndpointConnection {

	private String endpointName;
	private AmazonSageMakerRuntime client;
	private String content_type;

	public SageMakerEndpointConnection(String endpointName, String content_type) {
		this.client = AmazonSageMakerRuntimeClientBuilder.standard()
				.withCredentials(new DefaultAWSCredentialsProviderChain()).build();
		this.endpointName = endpointName;
		this.content_type = content_type;
	}

	public String Invoke(String data) {
		
		InvokeEndpointRequest invokeEndpointRequest = new InvokeEndpointRequest();
		invokeEndpointRequest.setContentType(this.content_type);

		try {
			invokeEndpointRequest.setBody(ByteBuffer.wrap(data.getBytes("UTF-8")));
		} catch (java.io.UnsupportedEncodingException e) {
		}

		invokeEndpointRequest.setEndpointName(this.endpointName);

		InvokeEndpointResult result = client.invokeEndpoint(invokeEndpointRequest);
		String body = StandardCharsets.UTF_8.decode(result.getBody()).toString();
		return body;
	}
}
