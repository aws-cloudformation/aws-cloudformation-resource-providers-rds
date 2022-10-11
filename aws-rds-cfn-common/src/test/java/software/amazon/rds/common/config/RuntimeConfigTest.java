package software.amazon.rds.common.config;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class RuntimeConfigTest {

    private final static String propertiesFile = "string.key=TestString\n" +
            "integer.key=42\n" +
            "long.key=1234567890\n" +
            "boolean.key=true";

    @Test
    public void test_getString() {
        final InputStream input = new ByteArrayInputStream(propertiesFile.getBytes(StandardCharsets.UTF_8));
        final RuntimeConfig config = RuntimeConfig.loadFrom(input);
        Assertions.assertThat(config.isSet("string.key")).isTrue();
        Assertions.assertThat(config.getString("string.key")).isEqualTo("TestString");
    }

    @Test
    public void test_getStringDefault() {
        final InputStream input = new ByteArrayInputStream(propertiesFile.getBytes(StandardCharsets.UTF_8));
        final RuntimeConfig config = RuntimeConfig.loadFrom(input);
        Assertions.assertThat(config.isSet("string.key.non.existing")).isFalse();
        Assertions.assertThat(config.getString("string.key.non.existing", "TestString")).isEqualTo("TestString");
    }

    @Test
    public void test_getInteger() {
        final InputStream input = new ByteArrayInputStream(propertiesFile.getBytes(StandardCharsets.UTF_8));
        final RuntimeConfig config = RuntimeConfig.loadFrom(input);
        Assertions.assertThat(config.isSet("integer.key")).isTrue();
        Assertions.assertThat(config.getInteger("integer.key")).isEqualTo(42);
    }

    @Test
    public void test_getIntegerDefault() {
        final InputStream input = new ByteArrayInputStream(propertiesFile.getBytes(StandardCharsets.UTF_8));
        final RuntimeConfig config = RuntimeConfig.loadFrom(input);
        Assertions.assertThat(config.isSet("integer.key.non.existing")).isFalse();
        Assertions.assertThat(config.getInteger("integer.key.non.existing", 42)).isEqualTo(42);
    }

    @Test
    public void test_getLong() {
        final InputStream input = new ByteArrayInputStream(propertiesFile.getBytes(StandardCharsets.UTF_8));
        final RuntimeConfig config = RuntimeConfig.loadFrom(input);
        Assertions.assertThat(config.isSet("long.key")).isTrue();
        Assertions.assertThat(config.getLong("long.key")).isEqualTo(1234567890L);
    }

    @Test
    public void test_getLongDefault() {
        final InputStream input = new ByteArrayInputStream(propertiesFile.getBytes(StandardCharsets.UTF_8));
        final RuntimeConfig config = RuntimeConfig.loadFrom(input);
        Assertions.assertThat(config.isSet("long.key.non.existing")).isFalse();
        Assertions.assertThat(config.getLong("long.key.non.existing", 1234567890L)).isEqualTo(1234567890L);
    }

    @Test
    public void test_getBoolean() {
        final InputStream input = new ByteArrayInputStream(propertiesFile.getBytes(StandardCharsets.UTF_8));
        final RuntimeConfig config = RuntimeConfig.loadFrom(input);
        Assertions.assertThat(config.isSet("boolean.key")).isTrue();
        Assertions.assertThat(config.getBoolean("boolean.key")).isEqualTo(true);
    }

    @Test
    public void test_getBooleanDefault() {
        final InputStream input = new ByteArrayInputStream(propertiesFile.getBytes(StandardCharsets.UTF_8));
        final RuntimeConfig config = RuntimeConfig.loadFrom(input);
        Assertions.assertThat(config.isSet("boolean.key.non.existing")).isFalse();
        Assertions.assertThat(config.getBoolean("boolean.key.non.existing", true)).isEqualTo(true);
    }
}
