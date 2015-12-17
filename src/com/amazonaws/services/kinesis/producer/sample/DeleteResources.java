package com.amazonaws.services.kinesis.producer.sample;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;

public class DeleteResources {

	public static void main(String[] args) {
		Region region = RegionUtils.getRegion(SampleProducer.REGION);
		AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
		AmazonDynamoDB dynamoDB = new AmazonDynamoDBClient(credentialsProvider,
				new ClientConfiguration());
		dynamoDB.setRegion(region);
		DynamoDBUtils dbUtils = new DynamoDBUtils(dynamoDB);
		dbUtils.deleteTable(SampleConsumer.DB_NAME);

		AmazonKinesis kinesis = new AmazonKinesisClient(credentialsProvider,
				new ClientConfiguration());
		kinesis.setRegion(region);
		StreamUtils streamUtils = new StreamUtils(kinesis);
		streamUtils.deleteStream(SampleProducer.STREAM_NAME);
	}
}
