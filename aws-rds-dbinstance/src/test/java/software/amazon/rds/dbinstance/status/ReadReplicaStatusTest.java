package software.amazon.rds.dbinstance.status;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class ReadReplicaStatusTest {

    @Test
    public void test_fromString() {
        for (final ReadReplicaStatus status : ReadReplicaStatus.values()) {
            Assertions.assertThat(ReadReplicaStatus.fromString(status.toString())).isEqualTo(status);
        }
    }

    @Test
    public void test_fromString_Unknown() {
        Assertions.assertThat(ReadReplicaStatus.fromString("unknown-status")).isNull();
    }
}
