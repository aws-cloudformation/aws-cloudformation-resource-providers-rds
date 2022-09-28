package software.amazon.rds.dbinstance.status;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class VPCSecurityGroupStatusTest {

    @Test
    public void test_fromString() {
        for (final VPCSecurityGroupStatus status : VPCSecurityGroupStatus.values()) {
            Assertions.assertThat(VPCSecurityGroupStatus.fromString(status.toString())).isEqualTo(status);
        }
    }

    @Test
    public void test_fromString_Unknown() {
        Assertions.assertThat(VPCSecurityGroupStatus.fromString("unknown-status")).isNull();
    }
}
