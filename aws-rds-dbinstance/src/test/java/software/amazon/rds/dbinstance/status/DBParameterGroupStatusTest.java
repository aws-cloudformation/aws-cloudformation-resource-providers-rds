package software.amazon.rds.dbinstance.status;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class DBParameterGroupStatusTest {

    @Test
    public void test_fromString() {
        for (final DBParameterGroupStatus status : DBParameterGroupStatus.values()) {
            Assertions.assertThat(DBParameterGroupStatus.fromString(status.toString())).isEqualTo(status);
        }
    }

    @Test
    public void test_fromString_Unknown() {
        Assertions.assertThat(DBParameterGroupStatus.fromString("unknown-status")).isNull();
    }
}
