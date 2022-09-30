package software.amazon.rds.dbinstance.status;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class DomainMembershipStatusTest {

    @Test
    public void test_fromString() {
        for (final DomainMembershipStatus status : DomainMembershipStatus.values()) {
            Assertions.assertThat(DomainMembershipStatus.fromString(status.toString())).isEqualTo(status);
        }
    }

    @Test
    public void test_fromString_Unknown() {
        Assertions.assertThat(DomainMembershipStatus.fromString("unknown-status")).isNull();
    }
}
