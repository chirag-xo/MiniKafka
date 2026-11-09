package com.minikafka.metrics;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/**
 * Zero-dependency HTTP metrics server using the built-in JDK {@link HttpServer}.
 *
 * <p>Exposes a single endpoint:
 * <pre>GET /metrics → plain-text metrics snapshot</pre>
 *
 * <p>Uses a virtual-thread executor so metric reads never block server threads.
 */
public class MetricsServer implements AutoCloseable {

    private final HttpServer server;
    private final BrokerMetrics metrics;

    public MetricsServer(BrokerMetrics metrics, int port) throws IOException {
        this.metrics = metrics;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.server.createContext("/metrics", exchange -> {
            byte[] body = metrics.formatAsText().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        // Use virtual threads for handling HTTP requests
        this.server.setExecutor(java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor());
    }

    public void start() {
        server.start();
    }

    public int getPort() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
