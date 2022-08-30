package software.amazon.rds.dbclusterparametergroup;

import java.time.Duration;

import lombok.Builder;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

@SuperBuilder
public class LocalHandlerConfig extends software.amazon.rds.common.handler.HandlerConfig {

    @Getter
    @Builder.Default
    final private Duration stabilizationDelay = Duration.ZERO;

}
