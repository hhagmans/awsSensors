package com.innoq.hagmans.bachelor;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;

/**
 * Deletes all resources used in this example (Kinesis stream and Dynamo DBs)
 * 
 * @author hhagmans
 * 
 */
public class DeleteResources {

	public static void main(String[] args) {

		String streamName = TemperatureProducer.streamName;
		String db_name = TemperatureConsumer.db_name;

		if (args.length == 2) {
			streamName = args[0];
			db_name = args[1];
		}

		Region region = RegionUtils.getRegion(TemperatureProducer.REGION);
		AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
		AmazonDynamoDB amazonDynamoDB = new AmazonDynamoDBClient(
				credentialsProvider, new ClientConfiguration());
		AmazonDynamoDBClient client = new AmazonDynamoDBClient();
		client.setRegion(region);
		DynamoDB dynamoDB = new DynamoDB(client);
		amazonDynamoDB.setRegion(region);
		DynamoDBUtils dbUtils = new DynamoDBUtils(dynamoDB, amazonDynamoDB,
				client);
		dbUtils.deleteTable(db_name);
		dbUtils.deleteTable(TemperatureConsumer.tableName);

		AmazonKinesis kinesis = new AmazonKinesisClient(credentialsProvider,
				new ClientConfiguration());
		kinesis.setRegion(region);
		StreamUtils streamUtils = new StreamUtils(kinesis);
		streamUtils.deleteStream(streamName);
	}
}
