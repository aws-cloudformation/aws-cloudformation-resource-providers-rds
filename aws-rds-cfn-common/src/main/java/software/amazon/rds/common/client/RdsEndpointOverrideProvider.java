package software.amazon.rds.common.client;

import com.amazonaws.util.StringUtils;
import software.amazon.rds.common.annotations.ExcludeFromJacocoGeneratedReport;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.Properties;

public final class RdsEndpointOverrideProvider {

    private static final String ENDPOINT_PROPERTIES_FILE_PATH = "endpoint.properties";
    private static final String RDS_ENDPOINT_OVERRIDE = "rds.endpoint.override";

    private static final RdsEndpointOverrideProvider instance = new RdsEndpointOverrideProvider();

    private final Optional<URI> endpointOverride;

    public static Optional<URI> getEndpointOverride() {
        return instance.endpointOverride;
    }

    private RdsEndpointOverrideProvider() {
        this.endpointOverride = readProperty(ENDPOINT_PROPERTIES_FILE_PATH, RDS_ENDPOINT_OVERRIDE);
    }

    @ExcludeFromJacocoGeneratedReport
    private Optional<URI> readProperty(final String propertiesFile, final String propertyName) {
        final InputStream versionsInputStream = getClass().getClassLoader().getResourceAsStream(propertiesFile);
        final Properties versionProperties = new Properties();
        try {
            versionProperties.load(versionsInputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        final String propertyValue = versionProperties.getProperty(propertyName);

        if (StringUtils.isNullOrEmpty(propertyValue)) {
            return Optional.empty();
        }

        try {
            return Optional.of(new URI(propertyValue));
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
