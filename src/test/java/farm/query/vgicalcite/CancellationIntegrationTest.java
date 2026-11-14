package farm.query.vgicalcite;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
class CancellationIntegrationTest {
    @ParameterizedTest @ValueSource(strings = {"tcp", "http", "unix", "subprocess"})
    void statementCancelInterruptsAStalledWorker(String transport) throws Exception {
        verify(transport, false);
    }

    @ParameterizedTest @ValueSource(strings = {"tcp", "http", "unix", "subprocess"})
    void queryTimeoutInterruptsAStalledWorker(String transport) throws Exception {
        verify(transport, true);
    }

    private static void verify(String transport, boolean timeout) throws Exception {
        Path directory = Files.createTempDirectory(Path.of("/tmp"), "vgic-stop-");
        Path entered = directory.resolve("scan.entered");
        Path release = directory.resolve("scan.release");
        var executor = Executors.newSingleThreadExecutor();
        try (var worker = WorkerProcess.start(transport, directory);
             var connection = CalciteIntegrationTest.open(worker.operand)) {
            String marker = directory.resolve("scan").toString().replace("'", "''");
            try (var statement = connection.prepareStatement("select count(*) from table(vgi.main.blocking('" + marker + "'))")) {
                if (timeout) statement.setQueryTimeout(1);
                var pending = executor.submit(() -> {
                    try (var result = statement.executeQuery()) {
                        while (result.next()) result.getObject(1);
                        return (Exception) null;
                    } catch (Exception failure) { return failure; }
                });
                try {
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                    while (!Files.exists(entered) && !pending.isDone() && System.nanoTime() < deadline) Thread.sleep(10);
                    assertTrue(Files.exists(entered), "Worker must enter the stalled producer before cancellation");
                    if (!timeout) {
                        // A blocked scan must not monopolize the connection or
                        // abort a different statement's independent RPC session.
                        try (var other = connection.createStatement();
                             var rows = other.executeQuery("select sum(id) from table(vgi.main.sequence(10))")) {
                            assertTrue(rows.next()); assertEquals(45, rows.getInt(1));
                        }
                        statement.cancel();
                    }
                    Exception failure = pending.get(3, TimeUnit.SECONDS);
                    assertNotNull(failure, "Cancellation must fail the query, not return a partial result");
                    StringBuilder causes = new StringBuilder();
                    for (Throwable cause = failure; cause != null; cause = cause.getCause()) causes.append(cause).append('\n');
                    assertTrue(causes.toString().toLowerCase(java.util.Locale.ROOT).contains(timeout ? "tim" : "cancel"), causes.toString());
                    assertFalse(Files.exists(release), "The query must stop before the worker is released");
                    try (var next = connection.createStatement();
                         var rows = next.executeQuery("select count(*) from table(vgi.main.sequence(3))")) {
                        assertTrue(rows.next()); assertEquals(3, rows.getInt(1));
                    }
                } finally {
                    Files.writeString(release, "release");
                    pending.get(10, TimeUnit.SECONDS);
                }
            }
        } finally {
            Files.writeString(release, "release");
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
            Files.deleteIfExists(entered);
            Files.deleteIfExists(release);
            Files.deleteIfExists(directory.resolve("worker.sock"));
            Files.deleteIfExists(directory);
        }
    }

    private static final class WorkerProcess implements AutoCloseable {
        final Process process;
        final Map<String, Object> operand;
        WorkerProcess(Process process, Map<String, Object> operand) { this.process = process; this.operand = operand; }
        static WorkerProcess start(String transport, Path directory) throws Exception {
            if (transport.equals("subprocess")) return new WorkerProcess(null,
                    Map.of("command", CalciteIntegrationTest.command(), "catalog", "demo"));
            List<String> command = new ArrayList<>(CalciteIntegrationTest.command());
            command.addAll(switch (transport) {
                case "tcp" -> List.of("--tcp", "127.0.0.1:0");
                case "http" -> List.of("--http", "--host", "127.0.0.1", "--port", "0");
                default -> List.of("--unix", directory.resolve("worker.sock").toString());
            });
            Process worker = new ProcessBuilder(command).redirectError(ProcessBuilder.Redirect.INHERIT).start();
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                String line = executor.submit(() -> new BufferedReader(new InputStreamReader(worker.getInputStream())).readLine())
                        .get(20, TimeUnit.SECONDS);
                assertNotNull(line);
                String location = switch (transport) {
                    case "tcp" -> { assertTrue(line.startsWith("TCP:"), line); yield "tcp://" + line.substring(4); }
                    case "http" -> { assertTrue(line.startsWith("PORT:"), line); yield "http://127.0.0.1:" + line.substring(5); }
                    default -> { assertTrue(line.startsWith("UNIX:"), line); yield "unix://" + line.substring(5); }
                };
                return new WorkerProcess(worker, Map.of("location", location, "catalog", "demo"));
            } catch (Throwable e) { worker.destroyForcibly(); throw e; }
        }
        @Override public void close() throws InterruptedException {
            if (process == null) return;
            process.destroy();
            if (!process.waitFor(5, TimeUnit.SECONDS)) { process.destroyForcibly(); process.waitFor(5, TimeUnit.SECONDS); }
        }
    }
}
