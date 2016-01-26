package com.innoq.hagmans.bachelor;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public class ServletStarter {

	/**
	 * Name of the table, that holds the data of the current run
	 */
	public static String db_name = TemperatureConsumer.db_name;

	/**
	 * Name of the table that holds the temperatures
	 */
	public static String tableName = TemperatureConsumer.tableName;

	/**
	 * Name of the Kinesis stream
	 */
	public static String streamName = TemperatureConsumer.streamName;

	/**
	 * Starts the Jetty Server and puts the servlet in the context
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		if (args.length == 2) {
			streamName = args[0];
			db_name = args[1];
		}

		Server server = new Server(8080);
		// Servlet context
		ServletContextHandler context = new ServletContextHandler(
				ServletContextHandler.NO_SESSIONS
						| ServletContextHandler.NO_SECURITY);
		context.setContextPath("/api");
		context.addServlet(new ServletHolder(new TemperatureServlet(streamName,
				db_name, tableName)), "/GetTemperature/*");

		HandlerList handlers = new HandlerList();
		handlers.addHandler(context);
		handlers.addHandler(new DefaultHandler());

		server.setHandler(handlers);
		server.start();
		server.join();
	}
}
