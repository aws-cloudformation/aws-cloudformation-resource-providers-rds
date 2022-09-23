package software.amazon.rds.test.common.core;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class ServiceProviderTest {

    @Test
    public void test_fromString_Success() {
        for (final ServiceProvider serviceProvider: ServiceProvider.values()) {
            Assertions.assertThat(ServiceProvider.fromString(serviceProvider.toString())).isEqualTo(serviceProvider);
        }
    }

    @Test
    public void test_fromString_NonExistingThrowsRuntimeError() {
        Assertions.assertThatThrownBy(() -> {
            ServiceProvider.fromString("non-existing-provider");
        }).isInstanceOf(RuntimeException.class);
    }

    @Test
    public void test_fromClientClass_Success() {
        class RdsClient {}
        Assertions.assertThat(ServiceProvider.fromClientClass(RdsClient.class)).isEqualTo(ServiceProvider.RDS);
    }

    @Test
    public void test_fromClientClass_NonExistingThrowsRuntimeException() {
        class NonExistingClient {}
        Assertions.assertThatThrownBy(() -> {
            ServiceProvider.fromClientClass(NonExistingClient.class);
        }).isInstanceOf(RuntimeException.class);
    }
}
