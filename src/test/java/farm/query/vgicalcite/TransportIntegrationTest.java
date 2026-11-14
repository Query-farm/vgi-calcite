package farm.query.vgicalcite;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(60)
class TransportIntegrationTest {
    @Test void httpTransportStreamsMultipleBatchesAndClosesAnEarlyLimit() throws Exception {
        List<String> command = new ArrayList<>(CalciteIntegrationTest.command());
        command.addAll(List.of("--http", "--host", "127.0.0.1", "--port", "0"));
        Process worker = new ProcessBuilder(command).redirectError(ProcessBuilder.Redirect.INHERIT).start();
        try {
            String line;
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                try {
                    line = executor.submit(() -> new BufferedReader(new InputStreamReader(worker.getInputStream())).readLine())
                            .get(20, TimeUnit.SECONDS);
                } catch (Exception e) { worker.destroyForcibly(); throw e; }
            }
            assertNotNull(line);
            assertTrue(line.startsWith("PORT:"), line);
            verify("http://127.0.0.1:" + line.substring(5));
        } finally { stop(worker); }
    }

    @Test void unixSocketTransport() throws Exception {
        // Keep the socket path short enough for macOS's sockaddr_un limit.
        Path directory = Files.createTempDirectory(Path.of("/tmp"), "vgic-");
        Path socket = directory.resolve("worker.sock");
        List<String> command = new ArrayList<>(CalciteIntegrationTest.command());
        command.addAll(List.of("--unix", socket.toString()));
        Process worker = new ProcessBuilder(command).redirectError(ProcessBuilder.Redirect.INHERIT).start();
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            boolean ready = false;
            while (!ready && System.nanoTime() < deadline && worker.isAlive()) {
                try (var probe = java.nio.channels.SocketChannel.open(java.net.StandardProtocolFamily.UNIX)) {
                    probe.connect(java.net.UnixDomainSocketAddress.of(socket));
                    ready = true;
                } catch (java.io.IOException notListeningYet) { Thread.sleep(20); }
            }
            assertTrue(ready, "worker did not start listening on its Unix socket");
            verify("unix://" + socket);
        } finally {
            stop(worker);
            Files.deleteIfExists(socket);
            Files.deleteIfExists(directory);
        }
    }

    private static void verify(String location) throws Exception {
        try (var connection = CalciteIntegrationTest.open(Map.of("location", location, "catalog", "demo"));
             var statement = connection.createStatement()) {
            try (var result = statement.executeQuery("select sum(id) from vgi.data.numbers")) {
                assertTrue(result.next()); assertEquals(45L, result.getLong(1));
            }
            try (var result = statement.executeQuery("select id from table(vgi.main.sequence(1000000)) fetch next 1 rows only")) {
                assertTrue(result.next()); assertEquals(0L, result.getLong(1));
            }
            try (var result = statement.executeQuery("select count(*) from vgi.data.numbers")) {
                assertTrue(result.next()); assertEquals(10L, result.getLong(1));
            }
            try (var result = statement.executeQuery("select id from vgi.data.filtered_sequence where id = 4")) {
                assertTrue(result.next()); assertEquals(4L, result.getLong(1)); assertFalse(result.next());
            }
            try (var result = statement.executeQuery("select * from table(vgi.main.scan_stats(false))")) {
                assertTrue(result.next()); assertEquals(1L, result.getLong(1)); assertEquals(1L, result.getLong(2));
            }
        }
    }

    private static void stop(Process worker) throws InterruptedException {
        worker.destroy();
        if (!worker.waitFor(5, TimeUnit.SECONDS)) { worker.destroyForcibly(); worker.waitFor(5, TimeUnit.SECONDS); }
    }
}
