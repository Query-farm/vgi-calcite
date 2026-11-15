package farm.query.vgicalcite;

import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.jdbc.CalcitePrepare;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.function.Supplier;

/** Calcite JDBC driver with VGI's numeric validation and type policy. */
public final class VgiDriver extends Driver {
    public static final String CONNECT_STRING_PREFIX = "jdbc:vgi-calcite:";

    static {
        new VgiDriver().register();
    }

    public VgiDriver() {
        this(VgiPrepare::new);
    }

    private VgiDriver(Supplier<CalcitePrepare> prepareFactory) {
        super(prepareFactory);
    }

    @Override public VgiDriver withPrepareFactory(Supplier<CalcitePrepare> prepareFactory) {
        return new VgiDriver(prepareFactory);
    }

    @Override protected String getConnectStringPrefix() {
        return CONNECT_STRING_PREFIX;
    }

    @Override public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) return null;
        Properties properties = new Properties();
        properties.putAll(info);
        for (String name : info.stringPropertyNames()) properties.setProperty(name, info.getProperty(name));
        properties.putIfAbsent("typeSystem", VgiTypeSystem.class.getName());
        return super.connect(url, properties);
    }
}
