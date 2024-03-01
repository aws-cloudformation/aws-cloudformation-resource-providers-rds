package software.amazon.rds.integration;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.cloudformation.proxy.delay.Constant;

import java.time.Duration;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TimeoutVerificationTest {

    @ParameterizedTest
    @MethodSource("timeoutsAndHandlersProvider")
    public void verifyDefaultBackoffConfig(final Duration timeout, final BaseHandlerStd handler) {
        final Constant backoffConfig = handler.config.getBackoff();
        Duration totalWaitTime = Duration.ZERO;
        int attempt = 1;
        Duration delay = backoffConfig.nextDelay(attempt);
        while (delay != Duration.ZERO) {
            totalWaitTime = totalWaitTime.plus(delay);
            attempt += 1;
            delay = backoffConfig.nextDelay(attempt);
        }
        assertTrue(totalWaitTime.compareTo(timeout) >= 0);
    }

    static Stream<Arguments> timeoutsAndHandlersProvider() {
        return Stream.of(
                Arguments.arguments(Duration.ofHours(8), new CreateHandler()),
                Arguments.arguments(Duration.ofHours(8), new UpdateHandler()),
                Arguments.arguments(Duration.ofMinutes(30), new DeleteHandler())
        );
    }
}
