package farm.query.vgicalcite;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.Properties;

/** Small JDBC runner for trying a model without installing a SQL shell. */
public final class Main {
    private Main() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: java [Arrow JVM flags] -jar vgi-calcite.jar model.json 'SELECT ...'");
            System.exit(2);
        }
        Properties properties = new Properties();
        properties.setProperty("model", Path.of(args[0]).toAbsolutePath().toString());
        properties.setProperty("caseSensitive", "false");
        properties.setProperty("timeZone", "UTC");
        properties.setProperty("typeSystem", VgiTypeSystem.class.getName());
        try (var connection = DriverManager.getConnection("jdbc:calcite:", properties);
             var statement = connection.createStatement();
             var result = statement.executeQuery(args[1])) {
            int count = result.getMetaData().getColumnCount();
            for (int i = 1; i <= count; i++) {
                if (i > 1) System.out.print('\t');
                System.out.print(result.getMetaData().getColumnLabel(i));
            }
            System.out.println();
            while (result.next()) {
                for (int i = 1; i <= count; i++) {
                    if (i > 1) System.out.print('\t');
                    String value = result.getString(i);
                    System.out.print(value == null ? "NULL" : value);
                }
                System.out.println();
            }
        }
    }
}
