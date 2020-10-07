package software.amazon.rds.dbinstance;

import java.time.Duration;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Setter;
import software.amazon.cloudformation.proxy.delay.Constant;

@Data
public class HandlerConfig {

    public static class HandlerConfigBuilder {
        private Boolean probingEnabled;
        private Constant backoff;

        public HandlerConfigBuilder probingEnabled(final Boolean samplingEnabled) {
            this.probingEnabled = samplingEnabled;
            return this;
        }

        public HandlerConfigBuilder backoff(final Constant backoff) {
            this.backoff = backoff;
            return this;
        }

        public HandlerConfig build() {
            final HandlerConfig handlerConfig = new HandlerConfig();
            if (this.probingEnabled != null) {
                handlerConfig.probingEnabled = this.probingEnabled;
            }
            if (this.backoff != null) {
                handlerConfig.backoff = this.backoff;
            }
            return handlerConfig;
        }
    }

    public static HandlerConfigBuilder builder() {
        return new HandlerConfigBuilder();
    }

    @Setter(AccessLevel.NONE)
    private boolean probingEnabled = true;

    @Setter(AccessLevel.NONE)
    private Constant backoff = Constant.of()
            .delay(Duration.ofSeconds(30))
            .timeout(Duration.ofMinutes(60))
            .build();
}
