package software.amazon.rds.common.handler;

import java.time.Duration;

import lombok.Builder;
import lombok.Getter;
import lombok.experimental.SuperBuilder;
import software.amazon.cloudformation.proxy.delay.Constant;

@SuperBuilder
public class HandlerConfig {

    @Getter
    @Builder.Default
    final private boolean probingEnabled = false;

    @Getter
    @Builder.Default
    final private Constant backoff = Constant.of()
            .delay(Duration.ofSeconds(30))
            .timeout(Duration.ofMinutes(90))
            .build();
}
