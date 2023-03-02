package software.amazon.rds.test.common.verification;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.exceptions.base.MockitoAssertionError;
import org.mockito.internal.util.MockUtil;
import org.mockito.internal.verification.VerificationDataImpl;

import software.amazon.rds.test.common.core.ServiceProvider;

class AccessPermissionVerificationModeTest {

    // A hack to pretend we're passing a known ServiceProvider to the FASPermission constructor.
    static class RdsClient {
        public void testMethod() {
        }

        public void testMethodAlias() {
        }
    }

    @Test
    public void test_unknownPermissionThrowsMockitoAssertionError() {
        final AccessPermissionVerificationMode verificationMode = new AccessPermissionVerificationMode();

        RdsClient mock = Mockito.mock(RdsClient.class);
        mock.testMethod();

        Assertions.assertThatThrownBy(() -> {
            verificationMode.verify(new VerificationDataImpl(MockUtil.getInvocationContainer(mock), null));
        }).isInstanceOf(MockitoAssertionError.class);
    }

    @Test
    public void test_enabledPermissionVerifiedSuccessfully() {
        final AccessPermissionVerificationMode verificationMode = new AccessPermissionVerificationMode();
        verificationMode.enablePermission(new AccessPermission(ServiceProvider.RDS, "TestMethod"));

        RdsClient mock = Mockito.mock(RdsClient.class);
        mock.testMethod();

        Assertions.assertThatCode(() -> {
            verificationMode.verify(new VerificationDataImpl(MockUtil.getInvocationContainer(mock), null));
        }).doesNotThrowAnyException();
    }

    @Test
    public void test_enabledPermissionAliasVerifiedSuccessfully() {
        final AccessPermission origin = new AccessPermission(ServiceProvider.RDS, "TestMethodAlias");
        final AccessPermission equivalent = new AccessPermission(ServiceProvider.RDS, "TestMethod");

        final AccessPermissionVerificationMode verificationMode = new AccessPermissionVerificationMode()
                .enablePermission(equivalent)
                .withAliases(new AccessPermissionAlias(origin, equivalent));

        RdsClient mock = Mockito.mock(RdsClient.class);
        mock.testMethodAlias();

        Assertions.assertThatCode(() -> {
            verificationMode.verify(new VerificationDataImpl(MockUtil.getInvocationContainer(mock), null));
        }).doesNotThrowAnyException();
    }
}
