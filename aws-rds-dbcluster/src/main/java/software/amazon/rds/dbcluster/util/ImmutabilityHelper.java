package software.amazon.rds.dbcluster.util;

import com.amazonaws.util.StringUtils;
import com.google.common.base.Objects;
import software.amazon.rds.dbcluster.ResourceModel;

public final class ImmutabilityHelper {

    private static final String AURORA = "aurora";
    private static final String AURORA_MYSQL = "aurora-mysql";

    private ImmutabilityHelper() {
    }

    static boolean isGlobalClusterMutable(final ResourceModel previous, final ResourceModel desired) {
        if (StringUtils.isNullOrEmpty(desired.getGlobalClusterIdentifier())) {
            return true;
        }
        return desired.getGlobalClusterIdentifier().equals(previous.getGlobalClusterIdentifier());
    }

    static boolean isEngineMutable(final ResourceModel previous, final ResourceModel desired) {
        return (AURORA.equals(previous.getEngine()) && AURORA_MYSQL.equals(desired.getEngine())) ||
                Objects.equal(previous.getEngine(), desired.getEngine());
    }

    public static boolean isChangeMutable(final ResourceModel previous, final ResourceModel desired) {
        return isGlobalClusterMutable(previous, desired) && isEngineMutable(previous, desired);
    }
}
