package farm.query.vgicalcite;

import farm.query.vgi.VgiService;
import farm.query.vgi.client.SettingsEncoder;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.CatalogAttachResult;
import farm.query.vgirpc.RpcConnection;
import farm.query.vgirpc.http.HttpRpcConnection;
import farm.query.vgirpc.transport.RpcTransport;
import farm.query.vgirpc.transport.SubprocessTransport;
import farm.query.vgirpc.transport.TcpSocketTransport;
import farm.query.vgirpc.transport.UnixSocketTransport;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.StandardProtocolFamily;
import java.net.URI;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.SocketChannel;

/** One lockstep RPC connection; never shared by concurrent enumerators. */
final class VgiSession implements AutoCloseable {
    final VgiService service;
    final CatalogAttachResult attachment;
    final farm.query.vgi.protocol.CatalogCapabilities selectedCatalogCapabilities;
    private final AutoCloseable connection;
    private boolean closed;
    private boolean detached;

    private VgiSession(AutoCloseable connection, VgiService service, VgiConfig config, long catalogVersion) {
        this.connection = connection;
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
            var connection = builder.build();
            return new VgiSession(connection, connection.proxy(VgiService.class), config, catalogVersion);
        }
        var connection = new RpcConnection(transport(config));
        return new VgiSession(connection, connection.proxy(VgiService.class), config, catalogVersion);
    }

    private static RpcTransport transport(VgiConfig config) {
        if (!config.command().isEmpty()) return new SubprocessTransport(config.command());
        URI uri = URI.create(config.location());
        try {
            if (uri.getScheme().equals("tcp")) {
                if (uri.getHost() == null || uri.getPort() < 1) {
                    throw new IllegalArgumentException("TCP location requires a host and port");
                }
                Socket socket = new Socket();
                try {
                    socket.connect(new InetSocketAddress(uri.getHost(), uri.getPort()), 10_000);
                    socket.setSoTimeout(60_000);
                    return new TcpSocketTransport(socket);
                } catch (IOException | RuntimeException e) {
                    socket.close();
                    throw e;
                }
            }
            if (uri.getAuthority() != null || uri.getPath() == null || uri.getPath().isBlank()) {
                throw new IllegalArgumentException("Unix location must be unix:///absolute/path");
            }
            SocketChannel channel = SocketChannel.open(StandardProtocolFamily.UNIX);
            try {
                channel.connect(UnixDomainSocketAddress.of(uri.getPath()));
                return new UnixSocketTransport(channel);
            } catch (IOException | RuntimeException e) {
                channel.close();
                throw e;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to connect to VGI worker", e);
        }
    }

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
            connection.close();
        } catch (Exception e) {
            throw new IllegalStateException("Unable to close VGI connection", e);
        }
    }
}
