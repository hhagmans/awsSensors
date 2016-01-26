package com.innoq.hagmans.bachelor;

/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

/**
 * Utility methods for interacting with Amazon DynamoDB for this application.
 */
public class DynamoDBUtils {
	private static final Log LOG = LogFactory.getLog(DynamoDBUtils.class);

	private static final String ATTRIBUTE_NAME_HASH_KEY = "sensor";
	private static final String ATTRIBUTE_NAME_RANGE_KEY = "time_stamp";
	private static final String ATTRIBUTE_NAME_TEMPERATURE = "temperatures";

	private AmazonDynamoDB amazonDynamoDB;
	private DynamoDB dynamoDB;
	private AmazonDynamoDBClient client;

	/**
	 * Create a new utility instance that uses the provided Amazon DynamoDB
	 * client.
	 * 
	 * @param dynamoDB
	 * 
	 * @param amazonDynamoDB
	 * 
	 * @param client
	 * 
	 */
	public DynamoDBUtils(DynamoDB dynamoDB, AmazonDynamoDB amazonDynamoDB,
			AmazonDynamoDBClient client) {
		if (amazonDynamoDB == null || dynamoDB == null || client == null) {
			throw new NullPointerException("dynamoDB must not be null");
		}
		this.amazonDynamoDB = amazonDynamoDB;
		this.dynamoDB = dynamoDB;
		this.client = client;
	}

	/**
	 * Creates the table to store our temperatures in with a hash key of
	 * "sensor" and a range key of "timestamp" so we can query counts for a
	 * given sensor by time. This uses an initial provisioned throughput of 10
	 * read capacity units and 5 write capacity units
	 * 
	 * @param tableName
	 *            The name of the table to create.
	 */
	public void createTemperatureTableIfNotExists(String tableName) {
		List<KeySchemaElement> ks = new ArrayList<>();
		ks.add(new KeySchemaElement().withKeyType(KeyType.HASH)
				.withAttributeName(ATTRIBUTE_NAME_HASH_KEY));
		ks.add(new KeySchemaElement().withKeyType(KeyType.RANGE)
				.withAttributeName(ATTRIBUTE_NAME_RANGE_KEY));

		ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<>();
		attributeDefinitions.add(new AttributeDefinition().withAttributeName(
				ATTRIBUTE_NAME_HASH_KEY).withAttributeType(
				ScalarAttributeType.S));
		// Range key must be a String. DynamoDBMapper translates Dates to
		// ISO8601 strings.
		attributeDefinitions.add(new AttributeDefinition().withAttributeName(
				ATTRIBUTE_NAME_RANGE_KEY).withAttributeType(
				ScalarAttributeType.S));

		// Create the table with enough write IOPS to handle 5 distinct
		// resources updated every 1 second:
		// 1 update/second * 5 resources = 5 write IOPS.
		// The provisioned throughput will need to be adjusted if the cadinality
		// of the input data or the interval for
		// updates changes.
		CreateTableRequest createTableRequest = new CreateTableRequest()
				.withTableName(tableName)
				.withProvisionedThroughput(new ProvisionedThroughput(10L, 5L))
				.withKeySchema(ks)
				.withAttributeDefinitions(attributeDefinitions);

		try {
			amazonDynamoDB.createTable(createTableRequest);

			LOG.info(String
					.format("Created DynamoDB table: %s. Waiting up to 5 minutes for it to become ACTIVE...",
							tableName));
			// Wait 5 minutes for the table to become ACTIVE
			if (!waitUntilTableIsActive(tableName, 10,
					TimeUnit.MINUTES.toSeconds(5))) {
				throw new IllegalStateException(
						String.format(
								"Timed out while waiting for DynamoDB table %s to become ready",
								tableName));
			}
		} catch (ResourceInUseException ex) {
			// Assume table exists and is ready to use
		}
	}

	/**
	 * Persists the given temperatures on DynamoDB
	 * 
	 * @param tableName
	 *            The name of the table, where the records will be persisted
	 * @param temperatureMap
	 *            A map containing the sensor names as the key, and as the value
	 *            a hashmap with the timestamp of the temperature as the key and
	 *            the temperature as the value
	 * @param timestamp
	 *            The timestamp of the run
	 */
	public void putTemperatures(String tableName,
			HashMap<String, HashMap<String, String>> temperatureMap,
			long timestamp) {

		Table table = dynamoDB.getTable(tableName);

		for (String sensor : temperatureMap.keySet()) {
			QuerySpec spec = new QuerySpec().withHashKey(
					ATTRIBUTE_NAME_HASH_KEY, sensor).withRangeKeyCondition(
					new RangeKeyCondition(ATTRIBUTE_NAME_RANGE_KEY).eq(String
							.valueOf(timestamp)));

			ItemCollection<QueryOutcome> items = table.query(spec);

			Iterator<Item> iterator = items.iterator();
			Item item = null;
			Map<String, String> temperatures = null;
			while (iterator.hasNext()) {
				item = iterator.next();
				temperatures = item.getMap(ATTRIBUTE_NAME_TEMPERATURE);
			}

			if (temperatures == null) {
				temperatures = new HashMap<>();
			}
			temperatures.putAll(temperatureMap.get(sensor));
			table.putItem(new Item()
					.withPrimaryKey(ATTRIBUTE_NAME_HASH_KEY, sensor,
							ATTRIBUTE_NAME_RANGE_KEY, String.valueOf(timestamp))
					.withMap(ATTRIBUTE_NAME_TEMPERATURE, temperatures));
			System.out.println("PutItem succeeded!");
		}
	}

	/**
	 * Gibt eine @HashMap mit allen Temperaturen zurück für den übergebenen
	 * Sensor
	 * 
	 * @param sensor
	 * @param tableName
	 * @return @Hashmap, die als Key einen Timestamp enthalten, zu dessen
	 *         Zeitpunkt die Daten des Sensors erfasst werden und die Values
	 *         sind eine Liste der Temperaturen des Sensors zum Timestamp
	 */
	public HashMap<String, HashMap<String, Object>> getTemperaturesForSensor(
			String sensor, String tableName) {
		Table table = dynamoDB.getTable(tableName);

		QuerySpec spec = new QuerySpec().withHashKey(ATTRIBUTE_NAME_HASH_KEY,
				sensor);

		ItemCollection<QueryOutcome> items = table.query(spec);

		Iterator<Item> iterator = items.iterator();
		Item item = null;
		HashMap<String, HashMap<String, Object>> temperatureMap = new HashMap<>();
		while (iterator.hasNext()) {
			item = iterator.next();
			temperatureMap.put(item.getString(ATTRIBUTE_NAME_RANGE_KEY),
					new HashMap<>(item.getMap(ATTRIBUTE_NAME_TEMPERATURE)));
		}

		return temperatureMap;
	}

	/**
	 * Returns a @HashMap with all temperatures for all sensors
	 * 
	 * @param tableName
	 * @return @HashMap, which key is the name of the sensor. The values are
	 *         another @HashMap, which has timestamps of the date that the
	 *         temperatures were created as keys, and the values are a list of
	 *         temperatures of the sensor at the given timestamp
	 */
	public HashMap<String, HashMap<String, HashMap<String, Object>>> getAllSensorTemperatures(
			String tableName) {
		ScanRequest scanRequest = new ScanRequest().withTableName(tableName);

		ScanResult result = client.scan(scanRequest);
		HashMap<String, HashMap<String, HashMap<String, Object>>> allTemperatures = new HashMap<>();
		for (Map<String, AttributeValue> item : result.getItems()) {
			String sensorName = item.get(ATTRIBUTE_NAME_HASH_KEY).getS();
			HashMap<String, HashMap<String, Object>> currentHashMap = getTemperaturesForSensor(
					sensorName, tableName);
			allTemperatures.put(sensorName, currentHashMap);
		}

		return allTemperatures;
	}

	/**
	 * Delete a DynamoDB table.
	 * 
	 * @param tableName
	 *            The name of the table to delete.
	 */
	public void deleteTable(String tableName) {
		LOG.info(String.format("Deleting DynamoDB table %s", tableName));
		try {
			amazonDynamoDB.deleteTable(tableName);
		} catch (ResourceNotFoundException ex) {
			// Ignore, table could not be found.
		} catch (AmazonClientException ex) {
			LOG.error(String.format("Error deleting DynamoDB table %s",
					tableName), ex);
		}
	}

	/**
	 * Wait for a DynamoDB table to become active and ready for use.
	 * 
	 * @param tableName
	 *            The name of the table to wait until it becomes active.
	 * @param secondsBetweenPolls
	 *            Seconds to wait between requests to DynamoDB.
	 * @param timeoutSeconds
	 *            Maximum amount of time, in seconds, to wait for a table to
	 *            become ready.
	 * @return {@code true} if the table is ready. False if our timeout exceeded
	 *         or we were interrupted.
	 */
	public boolean waitUntilTableIsActive(String tableName,
			long secondsBetweenPolls, long timeoutSeconds) {
		long sleepTimeRemaining = timeoutSeconds * 1000;

		while (!doesTableExist(tableName)) {
			if (sleepTimeRemaining <= 0) {
				return false;
			}

			long timeToSleepMillis = Math.min(1000 * secondsBetweenPolls,
					sleepTimeRemaining);

			try {
				Thread.sleep(timeToSleepMillis);
			} catch (InterruptedException ex) {
				LOG.warn(
						"Interrupted while waiting for count table to become ready",
						ex);
				Thread.currentThread().interrupt();
				return false;
			}

			sleepTimeRemaining -= timeToSleepMillis;
		}

		return true;
	}

	/**
	 * Determines if the table exists and is ACTIVE.
	 * 
	 * @param tableName
	 *            The name of the table to check.
	 * @return {@code true} if the table exists and is in the ACTIVE state
	 */
	public boolean doesTableExist(String tableName) {
		try {
			return "ACTIVE".equals(amazonDynamoDB.describeTable(tableName)
					.getTable().getTableStatus());
		} catch (AmazonClientException ex) {
			LOG.warn(String.format("Unable to describe table %s", tableName),
					ex);
			return false;
		}
	}
}