package farm.query.vgicalcite;

import farm.query.vgi.VgiService;
import farm.query.vgi.client.SettingsEncoder;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.CatalogAttachResult;
import farm.query.vgirpc.RpcConnection;
import farm.query.vgirpc.http.HttpRpcConnection;
import farm.query.vgirpc.transport.RpcTransport;
import farm.query.vgirpc.transport.TcpSocketTransport;
import farm.query.vgirpc.transport.UnixSocketTransport;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.time.Duration;
import java.nio.channels.SocketChannel;

/** One lockstep RPC connection; never shared by concurrent enumerators. */
final class VgiSession implements AutoCloseable {
    final VgiService service;
    final CatalogAttachResult attachment;
    final farm.query.vgi.protocol.CatalogCapabilities selectedCatalogCapabilities;
    private final AutoCloseable connection;
    private final Runnable abort;
    private boolean closed;
    private boolean detached;

    private VgiSession(AutoCloseable connection, Runnable abort, VgiService service, VgiConfig config, long catalogVersion) {
        this.connection = connection;
        this.abort = abort;
        this.service = service;
        boolean attached = false;
        try {
            attachment = service.catalog_attach(new CatalogAttachRequest(config.catalog(),
                    config.attachOptions().isEmpty() ? null : SettingsEncoder.of(config.attachOptions()),
                    config.dataVersion(), config.implementationVersion(),
                    farm.query.vgirpc.marshal.RecordCodec.serializeToBytes(
                            new farm.query.vgi.protocol.ClientCapabilities("calcite", java.util.List.of(),
                                    java.util.List.of(), true, java.util.List.of(VgiFilters.ENCODING),
                                    farm.query.vgi.CatalogProfiles.encode(VgiCatalogMetadata.OFFER)))), null);
            attached = true;
            selectedCatalogCapabilities = VgiCatalogMetadata.selected(attachment.selected_capabilities());
            if (!attachment.attach_catalogs().isEmpty()) {
                throw new UnsupportedOperationException("VGI companion catalogs are not supported by this adapter");
            }
            requireCatalogVersion(catalogVersion, attachment.catalog_version());
        } catch (RuntimeException | Error e) {
            if (attached) {
                try { detach(); } catch (RuntimeException detaching) { e.addSuppressed(detaching); }
            }
            try { close(); } catch (RuntimeException closing) { e.addSuppressed(closing); }
            throw e;
        }
    }

    static VgiSession open(VgiConfig config) {
        return open(config, 0);
    }

    static VgiSession open(VgiConfig config, long catalogVersion) {
        return open(config, catalogVersion, null);
    }

    static VgiSession open(VgiConfig config, long catalogVersion, VgiCancellation cancellation) {
        String location = config.location();
        if (location != null && (location.startsWith("http://") || location.startsWith("https://"))) {
            var builder = HttpRpcConnection.builder(location);
            if (config.bearerTokenEnv() != null) {
                String token = System.getenv(config.bearerTokenEnv());
                if (token == null || token.isBlank()) {
                    throw new IllegalArgumentException("Bearer token environment variable is unset: " + config.bearerTokenEnv());
                }
                builder.bearerToken(token);
            }
            // Own the HTTP client so cancellation can abort in-flight requests.
            // HttpRpcConnection.close() otherwise waits for them to finish.
            var http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
            Runnable abort = http::shutdownNow;
            if (cancellation != null) cancellation.onAbort(abort);
            try {
                var connection = builder.httpClient(http).build();
                return new VgiSession(connection, abort, connection.proxy(VgiService.class), config, catalogVersion);
            } catch (RuntimeException | Error e) { abort.run(); throw e; }
        }
        Transport transport = transport(config, cancellation);
        var connection = new RpcConnection(transport.rpc());
        return new VgiSession(connection, transport.abort(), connection.proxy(VgiService.class), config, catalogVersion);
    }

    private record Transport(RpcTransport rpc, Runnable abort) {}

    private static Transport transport(VgiConfig config, VgiCancellation cancellation) {
        if (!config.command().isEmpty()) {
            try {
                Process process = new ProcessBuilder(config.command()).redirectError(ProcessBuilder.Redirect.INHERIT).start();
                Runnable abort = () -> process.destroyForcibly();
                if (cancellation != null) cancellation.onAbort(abort);
                RpcTransport rpc = new RpcTransport() {
                    @Override public java.io.InputStream reader() { return process.getInputStream(); }
                    @Override public java.io.OutputStream writer() { return process.getOutputStream(); }
                    @Override public void close() {
                        process.destroyForcibly();
                        try { process.getOutputStream().close(); } catch (IOException ignored) {}
                        try { process.getInputStream().close(); } catch (IOException ignored) {}
                    }
                };
                return new Transport(rpc, abort);
            } catch (IOException e) { throw new UncheckedIOException("Unable to start VGI worker", e); }
        }
        URI uri = URI.create(config.location());
        try {
            if (uri.getScheme().equals("tcp")) {
                if (uri.getHost() == null || uri.getPort() < 1) {
                    throw new IllegalArgumentException("TCP location requires a host and port");
                }
                Socket socket = new Socket();
                Runnable abort = () -> closeTransport(socket);
                if (cancellation != null) cancellation.onAbort(abort);
                try {
                    socket.connect(new InetSocketAddress(uri.getHost(), uri.getPort()), 10_000);
                    socket.setSoTimeout(60_000);
                    return new Transport(new TcpSocketTransport(socket), abort);
                } catch (IOException | RuntimeException e) {
                    socket.close();
                    throw e;
                }
            }
            if (uri.getAuthority() != null || uri.getPath() == null || uri.getPath().isBlank()) {
                throw new IllegalArgumentException("Unix location must be unix:///absolute/path");
            }
            SocketChannel channel = connectUnix(Path.of(uri.getPath()), cancellation);
            Runnable abort = () -> closeTransport(channel);
            if (cancellation != null) cancellation.onAbort(abort);
            try {
                return new Transport(new UnixSocketTransport(channel), abort);
            } catch (IOException | RuntimeException e) {
                channel.close();
                throw e;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to connect to VGI worker", e);
        }
    }

    private static void closeTransport(AutoCloseable transport) {
        try { transport.close(); } catch (Exception e) { throw new IllegalStateException("Unable to abort VGI transport", e); }
    }

    private static SocketChannel connectUnix(Path path, VgiCancellation cancellation) throws IOException {
        if (cancellation == null) return UnixSocketTransport.connectChannel(path, Duration.ofSeconds(10));
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
        while (true) {
            cancellation.check();
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) throw new java.net.SocketTimeoutException("Timed out connecting to VGI Unix socket");
            try {
                return UnixSocketTransport.connectChannel(path,
                        Duration.ofNanos(Math.min(remaining, java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(250))));
            } catch (java.net.SocketTimeoutException busy) {
                // A full Unix accept queue must also observe JDBC cancellation.
            }
        }
    }

    /** Close the raw transport first, without flushing or acquiring stream locks. */
    void abort() { abort.run(); }

    byte[] handle() { return attachment.attach_opaque_data(); }

    /** Recheck mutable metadata after discovery or binding, while no stream is active. */
    void verifyCatalogVersion() {
        long expected = attachment.catalog_version();
        // Protocol version 0 is unknown, not an immutable version identifier.
        if (expected == 0 || attachment.catalog_version_frozen()) return;
        long actual = service.catalog_version(handle(), null, null).version();
        try {
            requireCatalogVersion(expected, actual);
        } catch (IllegalStateException changed) {
            try { detach(); } catch (RuntimeException detaching) { changed.addSuppressed(detaching); }
            throw changed;
        }
    }

    private static void requireCatalogVersion(long expected, long actual) {
        if (expected != 0 && actual != expected) {
            throw new IllegalStateException("VGI catalog metadata changed from version " + expected
                    + " to " + actual + "; reconnect to refresh metadata");
        }
    }

    /** Only on a completed/metadata connection, never while a stream is active. */
    void detach() {
        if (!closed && !detached) {
            service.catalog_detach(handle(), null);
            detached = true;
        }
    }

    @Override public void close() {
        if (closed) return;
        closed = true;
        try {
            try { abort(); } finally { connection.close(); }
        } catch (Exception e) {
            throw new IllegalStateException("Unable to close VGI connection", e);
        }
    }
}
