package software.amazon.rds.common.handler;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import software.amazon.cloudformation.proxy.delay.Constant;

public class HandlerConfigTest {

    @Test
    public void test_HandlerConfigBuilder() {
        final Duration delay = Duration.ofSeconds(5);
        final Duration timeout = Duration.ofMinutes(7);
        final Constant backoff = Constant.of()
                .delay(delay)
                .timeout(timeout)
                .build();
        final boolean probingEnabled = false;
        final HandlerConfig config = HandlerConfig.builder()
                .backoff(backoff)
                .probingEnabled(probingEnabled)
                .build();
        assertThat(config.isProbingEnabled()).isEqualTo(probingEnabled);
        assertThat(config.getBackoff()).isEqualTo(backoff);
    }

    @Test
    public void test_HandlerConfigBuilder_Defaults() {
        final HandlerConfig config = HandlerConfig.builder().build();
        assertThat(config.isProbingEnabled()).isNotNull();
        assertThat(config.getBackoff()).isNotNull();
    }

}
