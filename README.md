# Amazon Kinesis with Sagemaker for Sentiment Analysis

This tutorial uses Kinesis as a streaming channel for ingesting Amazon Reviews. Kinesis Client Library (KCL) is used to simulate pushing records to Kinesis. A producer is also running to take 
the amazon review json data, extract the review text of the item and calls a deployed Sagemaker endpoint which predicts the sentiment of the text between 1-5 scale.
