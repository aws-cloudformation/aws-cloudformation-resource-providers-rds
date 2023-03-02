package software.amazon.rds.test.common.verification;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import software.amazon.rds.test.common.core.ServiceProvider;

class AccessPermissionTest {

    @Test
    public void test_toString() {
        final AccessPermission permission = new AccessPermission(ServiceProvider.RDS, "TestMethod");
        Assertions.assertThat(permission.toString()).isEqualTo("rds:TestMethod");
    }
}
