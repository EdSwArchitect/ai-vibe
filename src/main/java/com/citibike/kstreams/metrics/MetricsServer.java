package com.citibike.kstreams.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

/**
 * HTTP server that exposes Prometheus metrics endpoint.
 */
public class MetricsServer {
    private static final Logger logger = LoggerFactory.getLogger(MetricsServer.class);
    private static final int DEFAULT_PORT = 8081;
    
    private HttpServer server;
    private final MetricsService metricsService;

    public MetricsServer(MetricsService metricsService) {
        this.metricsService = metricsService;
    }

    public void start() throws IOException {
        start(DEFAULT_PORT);
    }

    public void start(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/metrics", new MetricsHandler());
        server.setExecutor(null); // Use default executor
        server.start();
        logger.info("Metrics server started on port {}", port);
        logger.info("Metrics available at http://localhost:{}/metrics", port);
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
            logger.info("Metrics server stopped");
        }
    }

    private class MetricsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("GET".equals(exchange.getRequestMethod())) {
                String response = metricsService.scrape();
                exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4");
                exchange.sendResponseHeaders(200, response.getBytes().length);
                OutputStream os = exchange.getResponseBody();
                os.write(response.getBytes());
                os.close();
            } else {
                exchange.sendResponseHeaders(405, -1);
            }
        }
    }
}

