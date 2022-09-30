package software.amazon.rds.dbinstance.status;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class OptionGroupStatusTest {

    @Test
    public void test_fromString() {
        for (final OptionGroupStatus status : OptionGroupStatus.values()) {
            Assertions.assertThat(OptionGroupStatus.fromString(status.toString())).isEqualTo(status);
        }
    }

    @Test
    public void test_fromString_Unknown() {
        Assertions.assertThat(OptionGroupStatus.fromString("unknown-status")).isNull();
    }
}
