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
    private final AutoCloseable connection;
    private boolean closed;
    private boolean detached;

    private VgiSession(AutoCloseable connection, VgiService service, VgiConfig config) {
        this.connection = connection;
        this.service = service;
        try {
            attachment = service.catalog_attach(CatalogAttachRequest.of(config.catalog(),
                    config.attachOptions().isEmpty() ? null : SettingsEncoder.of(config.attachOptions()),
                    config.dataVersion(), config.implementationVersion()), null);
            if (!attachment.attach_catalogs().isEmpty()) {
                throw new UnsupportedOperationException("VGI companion catalogs are not supported by this adapter");
            }
        } catch (RuntimeException | Error e) {
            try { close(); } catch (RuntimeException closing) { e.addSuppressed(closing); }
            throw e;
        }
    }

    static VgiSession open(VgiConfig config) {
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
            return new VgiSession(connection, connection.proxy(VgiService.class), config);
        }
        var connection = new RpcConnection(transport(config));
        return new VgiSession(connection, connection.proxy(VgiService.class), config);
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
