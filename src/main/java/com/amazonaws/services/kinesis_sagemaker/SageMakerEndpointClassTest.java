package com.amazonaws.services.kinesis_sagemaker;



import java.nio.ByteBuffer;

import com.amazonaws.services.sagemakerruntime.AmazonSageMakerRuntime;
import com.amazonaws.services.sagemakerruntime.AmazonSageMakerRuntimeClientBuilder;
import com.amazonaws.services.sagemakerruntime.model.InvokeEndpointRequest;
import com.amazonaws.services.sagemakerruntime.model.InvokeEndpointResult;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;

import java.nio.charset.StandardCharsets;




public class SageMakerEndpointClassTest {
	
   
                           
   public static void main(String[] args) {
	   
       AmazonSageMakerRuntime client = AmazonSageMakerRuntimeClientBuilder
               .standard()
               .withCredentials(new DefaultAWSCredentialsProviderChain())
               .build();

       InvokeEndpointRequest invokeEndpointRequest = new InvokeEndpointRequest();
       invokeEndpointRequest.setContentType("text/csv");

       // Prepare the JSON payload.
       
       String data = "The item was really good\n I would not buy this again. very bad experience";

       System.out.println(data);

       try {
           invokeEndpointRequest.setBody(ByteBuffer.wrap(data.getBytes("UTF-8")));
       } 
       catch (java.io.UnsupportedEncodingException e) {}
       
       
       invokeEndpointRequest.setEndpointName("bert-sentiment-poc-2020-12-07-19-15-27-426");

       InvokeEndpointResult result = client.invokeEndpoint(invokeEndpointRequest);
       String body = StandardCharsets.UTF_8.decode(result.getBody()).toString();
       System.out.println("\n\n--------- Raw Results ----------");
       System.out.println(body);

           }
}
