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

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Random;

public class Utils {
	private static final Random RANDOM = new Random();
	private static final double RANGE_MIN = -20;
	private static final double RANGE_MAX = 40;

	/**
	 * @return A random unsigned 128-bit int converted to a decimal string.
	 */
	public static String randomExplicitHashKey() {
		return new BigInteger(128, RANDOM).toString(10);
	}

	public static double getNextTemperature(double lastTemperature) {
		float random = RANDOM.nextFloat();
		if (random < 0.33) {
			lastTemperature -= 0.1;
		} else if (random > 0.66) {
			lastTemperature += 0.1;
		}
		return (double) Math.round(lastTemperature * 10d) / 10d;
	}

	public static double getFirstTemperature() {
		return RANGE_MIN + (RANGE_MAX - RANGE_MIN) * RANDOM.nextDouble();
	}

	/**
	 * Generates a blob containing a UTF-8 string. The string begins with the
	 * sequence number in decimal notation, followed by a space, followed by
	 * padding.
	 * 
	 * @param sequenceNumber
	 *            The sequence number to place at the beginning of the record
	 *            data.
	 * @param totalLen
	 *            Total length of the data. After the sequence number, padding
	 *            is added until this length is reached.
	 * @return ByteBuffer containing the blob
	 */
	public static ByteBuffer generateData(double lasttemperature,
			String sensorName, int totalLen) {
		StringBuilder sb = new StringBuilder();
		sb.append(Double.toString(getNextTemperature(lasttemperature)));
		sb.append(";");
		sb.append(sensorName);
		sb.append(";");
		while (sb.length() < totalLen) {
			sb.append("a");
		}
		try {
			return ByteBuffer.wrap(sb.toString().getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}
}