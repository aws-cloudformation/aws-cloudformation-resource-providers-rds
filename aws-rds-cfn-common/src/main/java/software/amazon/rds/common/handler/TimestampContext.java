package software.amazon.rds.common.handler;

import java.time.Instant;

@lombok.Getter
@lombok.Setter
@lombok.ToString
@lombok.EqualsAndHashCode
public class TimestampContext {

    public interface Provider {

        void timestamp(final String label, final Instant instant);

        void timestampOnce(final String label, final Instant instant);

        Instant getTimestamp(final String label);
    }
}
