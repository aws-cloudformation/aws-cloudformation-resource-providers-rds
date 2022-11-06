package software.amazon.rds.common.util;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import software.amazon.cloudformation.proxy.Delay;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.common.annotations.ExcludeFromJacocoGeneratedReport;
import software.amazon.rds.common.config.RuntimeConfig;

public class ConfigHelper {

    public static final String HANDLER_BACKOFF_DELAY = "handler.backoff.delay";
    public static final String HANDLER_BACKOFF_TIMEOUT = "handler.backoff.timeout";

    @ExcludeFromJacocoGeneratedReport
    public static Delay getBackoff(final RuntimeConfig config) {
        return Constant.of()
                .delay(Duration.ofMillis(config.getLong(HANDLER_BACKOFF_DELAY, TimeUnit.SECONDS.toMillis(30))))
                .timeout(Duration.ofMillis(config.getLong(HANDLER_BACKOFF_TIMEOUT, TimeUnit.MINUTES.toMillis(180))))
                .build();
    }
}
