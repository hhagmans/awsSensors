/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.innoq.hagmans.bachelor;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;

/**
 * If you haven't looked at {@link TemperatureProducer}, do so first.
 * 
 * <p>
 * As mentioned in SampleProducer, we will check that all records are received
 * correctly by the KCL by verifying that there are no gaps in the sequence
 * numbers.
 * 
 * 
 * <p>
 * The consumer continues running until manually terminated, even if there are
 * no more records to consume.
 * 
 * @see TemperatureProducer
 * @author hhagmans
 * 
 */
public class TemperatureConsumer implements IRecordProcessorFactory {
	private static final Logger log = LoggerFactory
			.getLogger(TemperatureConsumer.class);

	// All records from a run of the producer have the same timestamp in their
	// partition keys. Since this value increases for each run, we can use it
	// determine which run is the latest and disregard data from earlier runs.
	private final AtomicLong largestTimestamp = new AtomicLong(0);

	/**
	 * Name of the table, that holds the data of the current run
	 */
	public static String db_name = "SensorConsumer";

	/**
	 * Name of the table that holds the temperatures
	 */
	public static String tableName = "Temperatures";

	/**
	 * Name of the Kinesis stream
	 */
	public static String streamName = "test";

	// A mutex for largestTimestamp and temperatures. largestTimestamp is
	// nevertheless an AtomicLong because we cannot capture non-final variables
	// in the child class.
	private final Object lock = new Object();

	/**
	 * One instance of RecordProcessor is created for every shard in the stream.
	 * All instances of RecordProcessor share state by capturing variables from
	 * the enclosing TemperatureConsumer instance. This is a simple way to
	 * combine the data from multiple shards.
	 */
	private class RecordProcessor implements IRecordProcessor {

		DynamoDBUtils dbUtils;

		@Override
		public void initialize(String shardId) {

			Region region = RegionUtils.getRegion(TemperatureProducer.REGION);
			AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
			AmazonDynamoDB amazonDynamoDB = new AmazonDynamoDBClient(
					credentialsProvider, new ClientConfiguration());
			AmazonDynamoDBClient client = new AmazonDynamoDBClient();
			client.setRegion(region);
			DynamoDB dynamoDB = new DynamoDB(client);
			amazonDynamoDB.setRegion(region);
			dbUtils = new DynamoDBUtils(dynamoDB, amazonDynamoDB, client);
		}

		@Override
		public void processRecords(List<Record> records,
				IRecordProcessorCheckpointer checkpointer) {
			long timestamp = 0;
			HashMap<String, HashMap<String, String>> allTemperatures = new HashMap<>();
			int count = 0;
			for (Record r : records) {
				// Get the timestamp of this run from the partition key.
				timestamp = Math.max(timestamp,
						Long.parseLong(r.getPartitionKey()));
				// Extract the data. All data are sperated with a semicolon
				try {
					byte[] b = new byte[r.getData().remaining()];
					r.getData().get(b);
					String[] splittedString = new String(b, "UTF-8").split(";");
					String currentTemperature = splittedString[0];
					String sensorName = (splittedString[1]);
					String currentTimeStamp = (splittedString[2]);

					// Create a new hashmap, if there isn't already one, and
					// combine the old and new temperature data
					HashMap<String, String> tempMap;
					if (allTemperatures.containsKey(sensorName)) {
						tempMap = allTemperatures.get(sensorName);
					} else {
						tempMap = new HashMap<>();
					}
					tempMap.put(currentTimeStamp, currentTemperature);
					allTemperatures.put(sensorName, tempMap);
					logResults(timestamp, count, sensorName, currentTemperature);
					count++;

				} catch (Exception e) {
					log.error("Error parsing record", e);
					System.exit(1);
				}
			}

			try {
				// Persist tempertures in DynamoDB
				dbUtils.putTemperatures(tableName, allTemperatures, timestamp);
				checkpointer.checkpoint();
			} catch (Exception e) {
				log.error(
						"Error while trying to checkpoint during ProcessRecords",
						e);
			}
		}

		@Override
		public void shutdown(IRecordProcessorCheckpointer checkpointer,
				ShutdownReason reason) {
			log.info("Shutting down, reason: " + reason);
			try {
				checkpointer.checkpoint();
			} catch (Exception e) {
				log.error("Error while trying to checkpoint during Shutdown", e);
			}
		}
	}

	/**
	 * Log a message indicating the current state.
	 */
	public void logResults(long timestamp, int count, String sensorName,
			String currentTemperature) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(timestamp);
		DateFormat df = new SimpleDateFormat(
				"dd.MM.yyyy HH:mm:ss 'and' SSS 'milliseconds'");
		log.info("Current temperature #" + count + " of " + sensorName
				+ " at timestamp " + df.format(cal.getTime()) + " is "
				+ currentTemperature);
	}

	@Override
	public IRecordProcessor createProcessor() {
		return this.new RecordProcessor();
	}

	public static void main(String[] args) throws InterruptedException {
		if (args.length == 2) {
			streamName = args[0];
			db_name = args[1];
		}

		// Initialize Utils
		KinesisClientLibConfiguration config = new KinesisClientLibConfiguration(
				db_name, streamName, new DefaultAWSCredentialsProviderChain(),
				"KinesisProducerLibSampleConsumer").withRegionName(
				TemperatureProducer.REGION).withInitialPositionInStream(
				InitialPositionInStream.TRIM_HORIZON);

		Region region = RegionUtils.getRegion(TemperatureProducer.REGION);
		AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
		AmazonDynamoDB amazonDynamoDB = new AmazonDynamoDBClient(
				credentialsProvider, new ClientConfiguration());
		AmazonDynamoDBClient client = new AmazonDynamoDBClient(
				credentialsProvider);
		client.setRegion(region);
		DynamoDB dynamoDB = new DynamoDB(client);
		amazonDynamoDB.setRegion(region);
		DynamoDBUtils dbUtils = new DynamoDBUtils(dynamoDB, amazonDynamoDB,
				client);
		AmazonKinesis kinesis = new AmazonKinesisClient(credentialsProvider,
				new ClientConfiguration());
		kinesis.setRegion(region);
		StreamUtils streamUtils = new StreamUtils(kinesis);
		try {
			if (!streamUtils.isActive(kinesis.describeStream(streamName))) {
				log.info("Stream is not active. Waiting for Stream to become active....");
				streamUtils.waitForStreamToBecomeActive(streamName);
			}
		} catch (ResourceNotFoundException e) {
			log.info("Stream is not created right now. Waiting for stream to get created and become active....");
			streamUtils.waitForStreamToBecomeActive(streamName);
		}
		dbUtils.deleteTable(db_name);
		dbUtils.createTemperatureTableIfNotExists(tableName);

		Thread.sleep(1000);

		final TemperatureConsumer consumer = new TemperatureConsumer();

		new Worker.Builder().recordProcessorFactory(consumer).config(config)
				.build().run();
	}
}
