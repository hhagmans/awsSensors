package com.innoq.hagmans.bachelor;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;

public class TemperatureServlet extends HttpServlet {

	private DynamoDBUtils dbUtils;

	private String tableName;

	public TemperatureServlet(String streamName, String db_name,
			String tableName) {
		this.tableName = tableName;
		KinesisClientLibConfiguration config = new KinesisClientLibConfiguration(
				db_name, streamName, new DefaultAWSCredentialsProviderChain(),
				"KinesisProducerLibSampleConsumer").withRegionName(
				TemperatureProducer.REGION).withInitialPositionInStream(
				InitialPositionInStream.TRIM_HORIZON);

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
	public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws IOException, ServletException {
		// Set the response message's MIME type
		response.setContentType("text/html;charset=UTF-8");
		// Allocate a output writer to write the response message into the
		// network socket
		PrintWriter out = response.getWriter();

		HashMap<String, HashMap<String, ArrayList<Object>>> allTemperatures = dbUtils
				.getAllSensorTemperatures(tableName);

		// Write the response message, in an HTML page
		try {
			out.println("<!DOCTYPE html>");
			out.println("<html><head>");
			out.println("<meta http-equiv='Content-Type' content='text/html; charset=UTF-8'>");
			out.println("<script type='text/javascript' src='https://www.gstatic.com/charts/loader.js'></script>");
			out.println("<script type='text/javascript'>");
			out.println("google.charts.load('current', {'packages':['corechart']});");
			out.println("google.charts.setOnLoadCallback(drawChart);");
			out.println("function drawChart() {");
			int sensorCount = 0;
			for (String sensor : allTemperatures.keySet()) {
				HashMap<String, ArrayList<Object>> hashMap = allTemperatures
						.get(sensor);
				for (String timestamp : hashMap.keySet()) {
					out.println("var data" + sensorCount
							+ " = google.visualization.arrayToDataTable([");
					out.println("['Time', 'Temperature'],");
					for (Object temperature : hashMap.get(timestamp)) {
						out.println("['', " + (String) temperature + "],");
					}
					out.println("]);");
					out.println("var options" + sensorCount + " = {");
					out.println("title: 'Temperatures of " + sensor
							+ " at timestamp " + timestamp + "',");
					out.println("curveType: 'function',");
					out.println("legend: { position: 'bottom' }");
					out.println("};");
					out.println("var chart"
							+ sensorCount
							+ " = new google.visualization.LineChart(document.getElementById('curve_chart"
							+ sensorCount + "'));");
					out.println("chart" + sensorCount + ".draw(data"
							+ sensorCount + ", options" + sensorCount + ");");
					sensorCount++;
				}
			}
			out.println("}");
			out.println("</script>");
			out.println("<title>Aktuelle Temperaturen</title></head>");
			out.println("<body>");
			out.println("<h1>Aktuelle Temperaturen</h1>");
			int tempSensorCount = 0;
			for (String sensor : allTemperatures.keySet()) {
				HashMap<String, ArrayList<Object>> hashMap = allTemperatures
						.get(sensor);
				out.println("<h2>Sensor: " + sensor + "</h2>");
				for (String timestamp : hashMap.keySet()) {
					out.println("<h3>Timestamp: " + timestamp + "</h3>");
					out.println("<div id='curve_chart" + tempSensorCount
							+ "' style='width: 900px; height: 500px'></div>");
					tempSensorCount++;
				}
			}
			out.println("</body>");
			out.println("</html>");
		} finally {
			out.close(); // Always close the output writer
		}
	}
}
