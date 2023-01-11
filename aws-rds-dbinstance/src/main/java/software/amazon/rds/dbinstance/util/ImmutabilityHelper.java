package software.amazon.rds.dbinstance.util;

import java.util.List;

import org.apache.commons.lang3.BooleanUtils;

import com.amazonaws.util.StringUtils;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import software.amazon.rds.dbinstance.ResourceModel;


public final class ImmutabilityHelper {

    private static final String ORACLE_SE = "oracle-se";
    private static final String ORACLE_SE1 = "oracle-se1";
    private static final String ORACLE_SE2 = "oracle-se2";

    private static final String AURORA = "aurora";
    private static final String AURORA_MYSQL = "aurora-mysql";

    private static final List<String> DEPRECATED_ORACLE_ENGINES = ImmutableList.of(ORACLE_SE, ORACLE_SE1);

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

    static boolean isEngineMutable(final ResourceModel previous, final ResourceModel desired) {
        return Objects.equal(previous.getEngine(), desired.getEngine()) ||
                isUpgradeToAuroraMySQL(previous, desired) ||
                isUpgradeToOracleSE2(previous, desired);
    }

    static boolean isPerformanceInsightsKMSKeyIdMutable(final ResourceModel previous, final ResourceModel desired) {
        return StringUtils.isNullOrEmpty(previous.getPerformanceInsightsKMSKeyId()) ||
                Objects.equal(previous.getPerformanceInsightsKMSKeyId(), desired.getPerformanceInsightsKMSKeyId());
    }

    static boolean isAvailabilityZoneChangeMutable(final ResourceModel previous, final ResourceModel desired) {
        return Objects.equal(previous.getAvailabilityZone(), desired.getAvailabilityZone()) ||
                (StringUtils.isNullOrEmpty(desired.getAvailabilityZone()) && BooleanUtils.isTrue(desired.getMultiAZ()));
    }

    private static boolean isReadReplicaPromotion(final ResourceModel previous, final ResourceModel desired) {
        return !StringUtils.isNullOrEmpty(previous.getSourceDBInstanceIdentifier()) &&
                StringUtils.isNullOrEmpty(desired.getSourceDBInstanceIdentifier());
    }

    public static boolean isSourceDBInstanceIdentifierMutable(final ResourceModel previous, final ResourceModel desired) {
        return Objects.equal(previous.getSourceDBInstanceIdentifier(), desired.getSourceDBInstanceIdentifier()) ||
                isReadReplicaPromotion(previous, desired);
    }

    public static boolean isDBSnapshotIdentifierMutable(final ResourceModel previous, final ResourceModel desired) {
        return Objects.equal(previous.getDBSnapshotIdentifier(), desired.getDBSnapshotIdentifier()) ||
                StringUtils.isNullOrEmpty(desired.getDBSnapshotIdentifier());
    }

    public static boolean isDBClusterSnapshotIdentifierMutable(final ResourceModel previous, final ResourceModel desired) {
        return Objects.equal(previous.getDBClusterSnapshotIdentifier(), desired.getDBClusterSnapshotIdentifier()) ||
                StringUtils.isNullOrEmpty(desired.getDBClusterSnapshotIdentifier());
    }

    public static boolean isUseLatestRestorableTimeMutable(final ResourceModel previous, final ResourceModel desired) {
        return Objects.equal(previous.getUseLatestRestorableTime(), desired.getUseLatestRestorableTime()) ||
                !BooleanUtils.isTrue(desired.getUseLatestRestorableTime());
    }

    public static boolean isRestoreTimeMutable(final ResourceModel previous, final ResourceModel desired) {
        return Objects.equal(previous.getRestoreTime(), desired.getRestoreTime()) ||
                StringUtils.isNullOrEmpty(desired.getRestoreTime());
    }

    public static boolean isSourceDBInstanceAutomatedBackupsArnMutable(final ResourceModel previous, final ResourceModel desired) {
        return Objects.equal(previous.getSourceDBInstanceAutomatedBackupsArn(), desired.getSourceDBInstanceAutomatedBackupsArn()) ||
                StringUtils.isNullOrEmpty(desired.getSourceDBInstanceAutomatedBackupsArn());
    }

    public static boolean isSourceDbiResourceIdMutable(final ResourceModel previous, final ResourceModel desired) {
        return Objects.equal(previous.getSourceDbiResourceId(), desired.getSourceDbiResourceId()) ||
                StringUtils.isNullOrEmpty(desired.getSourceDbiResourceId());
    }

    public static boolean isChangeMutable(final ResourceModel previous, final ResourceModel desired) {
        return isEngineMutable(previous, desired) &&
                isPerformanceInsightsKMSKeyIdMutable(previous, desired) &&
                isAvailabilityZoneChangeMutable(previous, desired) &&
                isSourceDBInstanceIdentifierMutable(previous, desired) &&
                isDBSnapshotIdentifierMutable(previous, desired) &&
                isDBClusterSnapshotIdentifierMutable(previous, desired) &&
                isUseLatestRestorableTimeMutable(previous, desired) &&
                isRestoreTimeMutable(previous, desired) &&
                isSourceDBInstanceAutomatedBackupsArnMutable(previous, desired) &&
                isSourceDbiResourceIdMutable(previous, desired);
    }
}
