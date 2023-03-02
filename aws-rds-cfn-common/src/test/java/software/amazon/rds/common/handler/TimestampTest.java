package software.amazon.rds.common.handler;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TimestampTest {


    private Timestamp.Provider timestampProvider;

    @BeforeEach
    public void setup() {
        timestampProvider = new Timestamp.Provider() {
        };
    }

    @Test
    public void test_timestamp() {
        String label = "test_stamp";
        Instant first = Instant.parse("2023-02-15T19:34:50Z");
        Instant second = Instant.ofEpochSecond(first.getEpochSecond()).plus(10, ChronoUnit.MINUTES);
        timestampProvider.timestamp(label, first);
        timestampProvider.timestamp(label, second);
        assertThat(timestampProvider.getTimestamp(label)).isEqualTo(second);
    }

    @Test
    public void test_timestamp_once() {
        String label = "test_stamp_once";
        Instant first = Instant.parse("2023-02-15T19:34:50Z");
        Instant second = Instant.ofEpochSecond(first.getEpochSecond()).plus(10, ChronoUnit.MINUTES);
        timestampProvider.timestampOnce(label, first);
        timestampProvider.timestampOnce(label, second);
        assertThat(timestampProvider.getTimestamp(label)).isEqualTo(first);
    }

}
