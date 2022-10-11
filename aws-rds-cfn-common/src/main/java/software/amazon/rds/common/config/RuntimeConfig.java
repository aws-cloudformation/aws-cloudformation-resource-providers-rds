package software.amazon.rds.common.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.amazonaws.util.StringUtils;
import software.amazon.rds.common.annotations.ExcludeFromJacocoGeneratedReport;

public class RuntimeConfig {

    public static final String RUNTIME_PROPERTIES = "config/runtime.properties";
    public static final String TEST_PROPERTIES = "config/test.properties";

    protected RuntimeConfig() {
        this.rawProperties = new Properties();
    }

    private final Properties rawProperties;

    @ExcludeFromJacocoGeneratedReport
    public static RuntimeConfig loadFrom(final InputStream input) {
        final RuntimeConfig config = new RuntimeConfig();
        try {
            config.rawProperties.load(input);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return config;
    }

    public Integer getInteger(final String key) {
        return getInteger(key, null);
    }

    public Integer getInteger(final String key, final Integer defaultValue) {
        final String stringVal = rawProperties.getProperty(key);
        if (StringUtils.isNullOrEmpty(stringVal)) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(stringVal, 10);
        } catch (NumberFormatException e) {
            throw new RuntimeException(e);
        }
    }

    public Long getLong(final String key) {
        return getLong(key, null);
    }

    public Long getLong(final String key, final Long defaultValue) {
        final String stringVal = rawProperties.getProperty(key);
        if (StringUtils.isNullOrEmpty(stringVal)) {
            return defaultValue;
        }
        try {
            return Long.parseLong(stringVal, 10);
        } catch (NumberFormatException e) {
            throw new RuntimeException(e);
        }
    }

    public String getString(final String key) {
        return getString(key, null);
    }

    public String getString(final String key, final String defaultValue) {
        return rawProperties.getProperty(key, defaultValue);
    }

    public Boolean getBoolean(final String key) {
        return getBoolean(key, null);
    }

    public Boolean getBoolean(final String key, final Boolean defaultValue) {
        final String stringVal = rawProperties.getProperty(key);
        if (StringUtils.isNullOrEmpty(stringVal)) {
            return defaultValue;
        }
        return Boolean.parseBoolean(stringVal);
    }

    public boolean isSet(final String key) {
        return rawProperties.containsKey(key);
    }
}
