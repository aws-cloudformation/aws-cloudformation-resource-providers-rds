package software.amazon.rds.dbinstance.client;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class ApiVersionDispatcherTest {

    @Test
    public void test_dispatch_noTesters() {
        final ApiVersionDispatcher<Void, Void> dispatcher = new ApiVersionDispatcher<>();
        Assertions.assertThat(dispatcher.dispatch(null, null)).isEqualTo(ApiVersion.DEFAULT);
    }

    @Test
    public void test_dispatch_allTestersMatchReturnOldestVersion() {
        final ApiVersionDispatcher<Void, Void> dispatcher = new ApiVersionDispatcher<Void, Void>()
                .register(ApiVersion.V12, (m, c) -> true)
                .register(ApiVersion.DEFAULT, (m, c) -> true);
        // Default version is defined the latest in order to "win" these cases.
        Assertions.assertThat(dispatcher.dispatch(null, null)).isEqualTo(ApiVersion.DEFAULT);
    }

    @Test
    public void test_dispatch_testerMatchSingleVersion() {
        final ApiVersionDispatcher<Void, Void> dispatcher = new ApiVersionDispatcher<Void, Void>()
                .register(ApiVersion.V12, (m, c) -> true);
        Assertions.assertThat(dispatcher.dispatch(null, null)).isEqualTo(ApiVersion.V12);
    }

    @Test
    public void test_dispatch_testerMatchExactVersionMismatchAnother() {
        final ApiVersionDispatcher<Void, Void> dispatcher = new ApiVersionDispatcher<Void, Void>()
                .register(ApiVersion.V12, (m, c) -> true)
                .register(ApiVersion.DEFAULT, (m, c) -> false);
        Assertions.assertThat(dispatcher.dispatch(null, null)).isEqualTo(ApiVersion.V12);
    }
}
