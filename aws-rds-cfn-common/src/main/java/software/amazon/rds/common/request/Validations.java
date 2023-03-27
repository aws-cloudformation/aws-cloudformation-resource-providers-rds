package software.amazon.rds.common.request;

import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.utils.StringUtils;

public class Validations {
    protected static final String UNKNOWN_SOURCE_REGION_ERROR = "Unknown source region";
    protected static final String INVALID_TIMESTAMP_ERROR = "timestamp must follow ISO8601";

    public static void validateSourceRegion(String sourceRegion) throws RequestValidationException {
        if (StringUtils.isNotBlank(sourceRegion)) {
            final Set<String> regionNames = Region.regions().stream().map(Region::toString).collect(Collectors.toSet());
            if (!regionNames.contains(sourceRegion.toLowerCase(Locale.getDefault()))) {
                throw new RequestValidationException(UNKNOWN_SOURCE_REGION_ERROR);
            }
        }
    }

    public static void validateTimestamp(String timestamp) throws RequestValidationException {
        if (StringUtils.isNotBlank(timestamp)) {
            try {
                ZonedDateTime.parse(timestamp).toInstant();
            } catch (DateTimeParseException exception) {
                throw new RequestValidationException(timestamp);
            }
        }
    }
}
