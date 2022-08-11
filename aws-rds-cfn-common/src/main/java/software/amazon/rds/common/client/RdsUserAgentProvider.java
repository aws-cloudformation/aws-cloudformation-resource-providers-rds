package software.amazon.rds.common.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.amazonaws.util.StringUtils;
import software.amazon.rds.common.test.ExcludeFromJacocoGeneratedReport;

public final class RdsUserAgentProvider {

    public static final String SDK_CLIENT_USER_AGENT_PREFIX = "AWS/CloudFormation/RDS";

    static final String VERSION_PROPERTIES_FILE_PATH = "version.properties";
    static final String GIT_COMMIT_ID_ABBREV_PROPERTY = "git.commit.id.abbrev";
    static final String NO_VERSION = "<NO_VERSION>";

    private static final RdsUserAgentProvider instance = new RdsUserAgentProvider();

    private final String scmVersion;

    private RdsUserAgentProvider() {
        this.scmVersion = readProperty(VERSION_PROPERTIES_FILE_PATH, GIT_COMMIT_ID_ABBREV_PROPERTY, NO_VERSION);
    }

    public static String getUserAgentPrefix() {
        return SDK_CLIENT_USER_AGENT_PREFIX;
    }

    public static String getUserAgentSuffix() {
        return instance.scmVersion;
    }

    @ExcludeFromJacocoGeneratedReport
    private String readProperty(final String propertiesFile, final String propertyName, final String ifEmpty) {
        final InputStream versionsInputStream = getClass().getClassLoader().getResourceAsStream(propertiesFile);
        final Properties versionProperties = new Properties();
        try {
            versionProperties.load(versionsInputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        final String propertyValue = versionProperties.getProperty(propertyName);

        return StringUtils.isNullOrEmpty(propertyValue) ? ifEmpty : propertyValue;
    }
}
