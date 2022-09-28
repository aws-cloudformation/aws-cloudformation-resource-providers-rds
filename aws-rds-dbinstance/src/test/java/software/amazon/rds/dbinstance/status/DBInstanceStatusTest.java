package software.amazon.rds.dbinstance.status;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class DBInstanceStatusTest {

    @Test
    public void test_fromString() {
        for (final DBInstanceStatus status : DBInstanceStatus.values()) {
            Assertions.assertThat(DBInstanceStatus.fromString(status.toString())).isEqualTo(status);
        }
    }

    @Test
    public void test_fromString_Unknown() {
        Assertions.assertThat(DBInstanceStatus.fromString("unknown-status")).isNull();
    }
}
