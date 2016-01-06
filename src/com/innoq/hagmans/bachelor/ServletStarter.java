package com.innoq.hagmans.bachelor;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public class ServletStarter {

	public static void main(String[] args) throws Exception {
		Server server = new Server(8080);
		// Servlet context
		ServletContextHandler context = new ServletContextHandler(
				ServletContextHandler.NO_SESSIONS
						| ServletContextHandler.NO_SECURITY);
		context.setContextPath("/api");
		context.addServlet(new ServletHolder(new TemperatureServlet()),
				"/GetTemperature/*");
		server.start();
		server.join();
	}
}
