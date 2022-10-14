package software.amazon.rds.dbcluster;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DBClusterStatusTest {

    @Test
    public void fromString_unknown() {
        final String source = "unknown";
        assertThat(DBClusterStatus.fromString(source)).isNull();
    }

    @Test
    public void test_fromString() {
        for (final DBClusterStatus status : DBClusterStatus.values()) {
            Assertions.assertThat(DBClusterStatus.fromString(status.toString())).isEqualTo(status);
        }
    }
}
