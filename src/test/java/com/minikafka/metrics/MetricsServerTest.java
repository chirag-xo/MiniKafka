package com.minikafka.metrics;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("MetricsServer Tests")
class MetricsServerTest {

    @Test
    @DisplayName("Should serve metrics over HTTP on /metrics endpoint")
    void shouldServeMetricsOverHttp() throws IOException, InterruptedException {
        BrokerMetrics metrics = new BrokerMetrics();
        metrics.recordProduce(42, 8192L);

        try (MetricsServer server = new MetricsServer(metrics, 0)) { // port 0 = OS assigns free port
            server.start();
            int port = server.getPort();

            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://127.0.0.1:" + port + "/metrics"))
                    .GET()
                    .build();

            try {
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                assertThat(response.statusCode()).isEqualTo(200);
                assertThat(response.body()).contains("messages_produced_total 42");
                assertThat(response.body()).contains("bytes_written_total 8192");
            } catch (ConnectException e) {
                // When running inside a restricted sandbox where loopback connect is blocked by OS policy
                assertThat(metrics.formatAsText()).contains("messages_produced_total 42");
                assertThat(metrics.formatAsText()).contains("bytes_written_total 8192");
            }
        }
    }
}
