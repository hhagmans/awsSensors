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

import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;

/**
 * A collection of functions to manipulate Amazon Kinesis streams.
 */
public class StreamUtils {
	private static final Log LOG = LogFactory.getLog(StreamUtils.class);

	private AmazonKinesis kinesis;
	private static final long CREATION_WAIT_TIME_IN_SECONDS = TimeUnit.SECONDS
			.toMillis(30);
	private static final long DELAY_BETWEEN_STATUS_CHECKS_IN_SECONDS = TimeUnit.SECONDS
			.toMillis(30);

	/**
	 * Creates a new utility instance.
	 * 
	 * @param kinesis
	 *            The Amazon Kinesis client to use for all operations.
	 */
	public StreamUtils(AmazonKinesis kinesis) {
		if (kinesis == null) {
			throw new NullPointerException(
					"Amazon Kinesis client must not be null");
		}
		this.kinesis = kinesis;
	}

	/**
	 * Create a stream if it doesn't already exist.
	 * 
	 * @param streamName
	 *            Name of stream
	 * @param shards
	 *            Number of shards to create stream with. This is ignored if the
	 *            stream already exists.
	 * @throws AmazonServiceException
	 *             Error communicating with Amazon Kinesis.
	 */
	public void createStream(String streamName, int shards)
			throws AmazonClientException {
		try {
			if (isActive(kinesis.describeStream(streamName))) {
				LOG.info(String.format("Stream %s was already created...",
						streamName));
				return;
			}
		} catch (ResourceNotFoundException ex) {
			LOG.info(String.format("Creating stream %s...", streamName));
			// No stream, create
			kinesis.createStream(streamName, shards);
			// Initially wait a number of seconds before checking for completion
			try {
				Thread.sleep(CREATION_WAIT_TIME_IN_SECONDS);
			} catch (InterruptedException e) {
				LOG.warn(String
						.format("Interrupted while waiting for %s stream to become active. Aborting.",
								streamName));
				return;
			}
		}

		waitForStreamToBecomeActive(streamName);

	}

	/**
	 * Create a stream if it doesn't already exist or deletes the old stream
	 * with the same name and creates a new one.
	 * 
	 * @param streamName
	 *            Name of stream
	 * @param shards
	 *            Number of shards to create stream with. This is ignored if the
	 *            stream already exists.
	 * @throws AmazonServiceException
	 *             Error communicating with Amazon Kinesis.
	 */
	public void createOrRenewStream(String streamName, int shards)
			throws AmazonClientException {
		try {
			if (isActive(kinesis.describeStream(streamName))) {
				LOG.info(String.format("Deleting stream %s....", streamName));
				kinesis.deleteStream(streamName);
				try {
					Thread.sleep(CREATION_WAIT_TIME_IN_SECONDS);
				} catch (InterruptedException e) {
					LOG.warn(String
							.format("Interrupted while waiting for %s stream to become active. Aborting.",
									streamName));
					return;
				}
			}
		} catch (ResourceNotFoundException ex) {
		}
		createStream(streamName, shards);
	}

	/**
	 * Does the result of a describe stream request indicate the stream is
	 * ACTIVE?
	 * 
	 * @param r
	 *            The describe stream result to check for ACTIVE status.
	 */
	public boolean isActive(DescribeStreamResult r) {
		return "ACTIVE".equals(r.getStreamDescription().getStreamStatus());
	}

	/**
	 * Checks if a stream is active and waits, if it's not active, until it
	 * becomes active
	 * 
	 * @param streamName
	 *            The name of the stream we are waiting for to become active
	 */
	public void waitForStreamToBecomeActive(String streamName) {
		while (true) {
			try {
				if (isActive(kinesis.describeStream(streamName))) {
					return;
				}
			} catch (ResourceNotFoundException ignore) {
				// The stream may be reported as not found if it was just
				// created.
			}
			try {
				Thread.sleep(DELAY_BETWEEN_STATUS_CHECKS_IN_SECONDS);
			} catch (InterruptedException e) {
				LOG.warn(String
						.format("Interrupted while waiting for %s stream to become active. Aborting.",
								streamName));
				Thread.currentThread().interrupt();
				return;
			}
		}
	}

	/**
	 * Delete an Amazon Kinesis stream.
	 * 
	 * @param streamName
	 *            The name of the stream to delete.
	 */
	public void deleteStream(String streamName) {
		LOG.info(String.format("Deleting Kinesis stream %s", streamName));
		try {
			kinesis.deleteStream(streamName);
		} catch (ResourceNotFoundException ex) {
			// The stream could not be found.
		} catch (AmazonClientException ex) {
			LOG.error(String.format("Error deleting stream %s", streamName), ex);
		}
	}

}
