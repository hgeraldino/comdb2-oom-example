package comdb2.test.oom;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.bloomberg.comdb2.jdbc.Comdb2Connection;
import com.bloomberg.comdb2.jdbc.Comdb2Handle;
import com.bloomberg.comdb2.jdbc.Comdb2Statement;
import com.bloomberg.comdb2.jdbc.Driver;
import com.bloomberg.comdb2.jdbc.UnpooledDataSource;
import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.platform.commons.util.ReflectionUtils;
import org.junit.platform.commons.util.ReflectionUtils.HierarchyTraversalMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@SuppressWarnings({"SqlResolve", "SqlNoDataSourceInspection", "StatementWithEmptyBody",
    "unchecked"})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Testcontainers
class Comdb2StatementIT {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String COMDB2_DBNAME = "oomtest";
  private static final String IMAGE_NAME = "artprod.dev.bloomberg.com/comdb2:latest";
  private static GenericContainer<?> SERVER;

  @BeforeAll
  static void init() {
    SERVER = DatabaseUtils.startContainer();
  }

  @AfterAll
  static void destroy() {
    DatabaseUtils.stopContainer(SERVER);
  }

  @Test
  @DisplayName("Create new statement per execution")
  void newStatementPerExec() throws Exception {
    try (var connection = (Comdb2Connection) DatabaseUtils.getConnection(SERVER)) {
      connection.setVerifyRetry(false);
      connection.setStatementQueryEffects(false);
      connection.setQueryTimeout(10);
      connection.setTimeout(30);

      var statementCount = 6;
      for (int i = 1; i <= 100; i++) {
        var statement = connection.createStatement();
        statement.execute(
            String.format("INSERT INTO tickers VALUES ('%s', '2021-10-12', '100.34')", UUID.randomUUID()));

        var rs = statement.executeQuery("SELECT * FROM tickers");
        while (rs.next()) {
        }

        assertEquals(statementCount, setsSize(statement));
        statementCount += 4;
      }
    }
  }

  @Test
  @DisplayName("Create the Statement just once")
  void reuseStatement() throws Exception {
    try (var connection = (Comdb2Connection) DatabaseUtils.getConnection(SERVER);
        var statement = (Comdb2Statement) connection.createStatement()) {
      statement.setQueryTimeout(10);

      // Insert rows
      for (int i = 1; i <= 100; i++) {
        statement.execute(
            String.format("INSERT INTO tickers VALUES ('%s', '2021-10-12', '100.34')",
                UUID.randomUUID()));

        var rs = statement.executeQuery("SELECT * FROM tickers");
        while (rs.next()) {
          // noop
        }
      }

      assertEquals(200, setsSize(statement));
    }
  }

  private int setsSize(Statement statement) throws Exception {
    var connection = (Comdb2Connection) statement.getConnection();

    var setsField = ReflectionUtils.findFields(Comdb2Handle.class,
            field -> field.getName().equals("sets"),
            HierarchyTraversalMode.TOP_DOWN)
        .iterator().next();

    var setsInstance = (List<String>) ReflectionUtils.tryToReadFieldValue(setsField,
            connection.dbHandle())
        .get();

    log.info("Executed {} set commands [{}]", setsInstance.size(), setsInstance);
    return setsInstance.size();
  }

  static class DatabaseUtils {

    static GenericContainer<?> startContainer() {
      var databaseTablePath = String.format("/opt/bb/share/schemas/%s", COMDB2_DBNAME);

      var container = new GenericContainer<>(DockerImageName.parse(IMAGE_NAME))
          .withExposedPorts(5105)
          .withEnv("DATABASE_TABLE_PATH", databaseTablePath)
          .withEnv("COMDB2_DBNAME", COMDB2_DBNAME)
          .withClasspathResourceMapping("csc2", databaseTablePath, BindMode.READ_ONLY)
          .waitingFor(Wait.forHealthcheck());
      container.start();
      container.followOutput(new Slf4jLogConsumer(log).withSeparateOutputStreams());

      return container;
    }

    static void stopContainer(GenericContainer<?> container) {
      if (container != null) {
        container.stop();
      }
    }

    static Connection getConnection(GenericContainer<?> container) throws SQLException {

      var dataSource = getDataSource(container);
      return dataSource.getConnection();
    }

    static DataSource getDataSource(GenericContainer<?> container) {
      var props = new Properties();
      final String connectionString = String
          .format("jdbc:comdb2://%s:%d/%s?portmuxport=%2$s&allow_pmux_route=true",
              container.getHost(), container.getMappedPort(5105), COMDB2_DBNAME);

      return new UnpooledDataSource(Driver.class.getCanonicalName(),
          connectionString, props);
    }
  }
}
