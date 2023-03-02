package software.amazon.rds.common.handler;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@lombok.Getter
@lombok.Setter
@lombok.ToString
@lombok.EqualsAndHashCode
public class Timestamp {

    public interface Provider {
        Map<String, Long> timestamps = new HashMap<>();

        default void timestamp(final String label, final Instant instant) {
            timestamps.put(label, instant.getEpochSecond());
        }

        default void timestampOnce(final String label, final Instant instant) {
            timestamps.computeIfAbsent(label, s -> instant.getEpochSecond());
        }

        default Instant getTimestamp(final String label) {
            if (timestamps.containsKey(label)) {
                return Instant.ofEpochSecond(timestamps.get(label));
            }
            return null;
        }
    }
}
