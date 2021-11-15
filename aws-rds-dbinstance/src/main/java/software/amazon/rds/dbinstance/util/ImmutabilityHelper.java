package software.amazon.rds.dbinstance.util;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;

import com.google.common.base.Objects;
import software.amazon.rds.dbinstance.ResourceModel;

public final class ImmutabilityHelper {

    private static final String ORACLE_SE = "oracle-se";
    private static final String ORACLE_SE1 = "oracle-se1";
    private static final String ORACLE_SE2 = "oracle-se2";

    private static final String AURORA = "aurora";
    private static final String AURORA_MYSQL = "aurora-mysql";

    protected static final List<String> DEPRECATED_ORACLE_ENGINES = Arrays.asList(ORACLE_SE, ORACLE_SE1);

    private ImmutabilityHelper() {
    }

    static boolean isUpgradeToOracleSE2(final ResourceModel previous, final ResourceModel desired) {
        return previous.getEngine() != null &&
                DEPRECATED_ORACLE_ENGINES.contains(previous.getEngine().toLowerCase()) &&
                ORACLE_SE2.equalsIgnoreCase(desired.getEngine());
    }

    static boolean isUpgradeToAuroraMySQL(final ResourceModel previous, final ResourceModel desired) {
        return previous.getEngine() != null &&
                desired.getEngine() != null &&
                previous.getEngine().equals(AURORA) &&
                desired.getEngine().equals(AURORA_MYSQL);
    }

    static boolean isAZMutable(final ResourceModel previous, final ResourceModel desired) {
        return Objects.equal(desired.getAvailabilityZone(), previous.getAvailabilityZone()) ||
                desired.getAvailabilityZone() == null &&
                        Boolean.TRUE.equals(desired.getMultiAZ());
    }

    static boolean isPerformanceInsightsMutable(final ResourceModel previous, final ResourceModel desired) {
        return previous.getEnablePerformanceInsights() == null ||
                !previous.getEnablePerformanceInsights() ||
                !desired.getEnablePerformanceInsights() ||
                Objects.equal(previous.getPerformanceInsightsKMSKeyId(), desired.getPerformanceInsightsKMSKeyId());
    }

    public static boolean isChangeImmutable(
            final ResourceModel previous,
            final ResourceModel desired
    ) {
        final boolean isEngineMutable = Objects.equal(previous.getEngine(), desired.getEngine()) ||
                isUpgradeToAuroraMySQL(previous, desired) ||
                isUpgradeToOracleSE2(previous, desired);
        final boolean isPerformanceInsightsKMSKeyIdMutable = isPerformanceInsightsMutable(previous, desired);
        final boolean isAZMutable = isAZMutable(previous, desired);

        final boolean isMutable = isAZMutable && isEngineMutable && isPerformanceInsightsKMSKeyIdMutable;

        return !isMutable;
    }
}
