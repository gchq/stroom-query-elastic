package stroom.tracking;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import java.util.concurrent.locks.ReentrantLock;

import static org.junit.Assert.fail;

public final class TestDSLContext {

    private static final ReentrantLock lock = new ReentrantLock();
    private static DSLContext dslContext = null;

    private static final String DB_URL = "jdbc:mariadb://localhost:7451/tracking";
    private static final String DB_USER = "stroomuser";
    private static final String DB_PASSWORD = "stroompassword1";

    static DSLContext createTestJooq() {

        lock.lock();
        try {
            if (null == dslContext) {
                // Create the Flyway instance
                Flyway flyway = new Flyway();

                // Point it to the database
                flyway.setDataSource(DB_URL, DB_USER, DB_PASSWORD);
                flyway.setLocations("classpath:/tracking/migration");

                // Start the migration
                flyway.migrate();

                dslContext = DSL.using(DB_URL, DB_USER, DB_PASSWORD);
            }
        } catch (final Exception e) {
            fail(e.getLocalizedMessage());
        } finally {
            lock.unlock();
        }

        return dslContext;
    }

    private TestDSLContext() {

    }
}
