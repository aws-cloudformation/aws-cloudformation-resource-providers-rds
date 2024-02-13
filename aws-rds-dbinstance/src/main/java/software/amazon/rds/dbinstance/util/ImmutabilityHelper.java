package software.amazon.rds.dbinstance.util;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.BooleanUtils;

import com.amazonaws.util.StringUtils;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.utils.MapUtils;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.dbinstance.ResourceModel;

public final class ImmutabilityHelper {

    private static final String ORACLE_SE = "oracle-se";
    private static final String ORACLE_SE1 = "oracle-se1";
    private static final String ORACLE_SE2 = "oracle-se2";
    private static final String ORACLE_SE2_CDB = "oracle-se2-cdb";
    private static final String ORACLE_EE = "oracle-ee";
    private static final String ORACLE_EE_CDB = "oracle-ee-cdb";

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

    static boolean isOracleConvertToCDB(final String previous, final ResourceModel desired) {
        return previous != null &&
            (
                (ORACLE_EE.equalsIgnoreCase(previous) && ORACLE_EE_CDB.equalsIgnoreCase(desired.getEngine())) ||
                (ORACLE_SE2.equalsIgnoreCase(previous) && ORACLE_SE2_CDB.equalsIgnoreCase(desired.getEngine()))
            );
    }

    static boolean isOracleCDBMutable(final String previous, final ResourceModel desired) {
        return org.apache.commons.lang3.StringUtils.equalsIgnoreCase(previous, desired.getEngine()) &&
                (
                        (ORACLE_EE_CDB.equalsIgnoreCase(desired.getEngine())) || (ORACLE_SE2_CDB.equalsIgnoreCase(desired.getEngine()))
                );

    }

    static boolean isUpgradeToAuroraMySQL(final ResourceModel previous, final ResourceModel desired) {
        return previous.getEngine() != null &&
                desired.getEngine() != null &&
                previous.getEngine().equals(AURORA) &&
                desired.getEngine().equals(AURORA_MYSQL);
    }

    static boolean isEngineMutable(final ResourceModel previous, final ResourceModel desired, final String currentEngine) {
        return Objects.equal(previous.getEngine(), desired.getEngine()) ||
                isUpgradeToAuroraMySQL(previous, desired) ||
                isUpgradeToOracleSE2(previous, desired) ||
                isOracleConvertToCDB(currentEngine, desired) ||
                isOracleCDBMutable(currentEngine, desired);
    }

    static boolean isPerformanceInsightsKMSKeyIdMutable(final ResourceModel previous, final ResourceModel desired) {
        return StringUtils.isNullOrEmpty(previous.getPerformanceInsightsKMSKeyId()) ||
                Objects.equal(previous.getPerformanceInsightsKMSKeyId(), desired.getPerformanceInsightsKMSKeyId());
    }

    static boolean isAvailabilityZoneChangeMutable(final ResourceModel previous, final ResourceModel desired) {
        return Objects.equal(previous.getAvailabilityZone(), desired.getAvailabilityZone()) ||
                (StringUtils.isNullOrEmpty(desired.getAvailabilityZone()) && BooleanUtils.isTrue(desired.getMultiAZ()));
    }

    public static boolean isSourceDBInstanceIdentifierMutable(final ResourceModel previous, final ResourceModel desired) {
        return Objects.equal(previous.getSourceDBInstanceIdentifier(), desired.getSourceDBInstanceIdentifier()) ||
                ResourceModelHelper.isDBInstanceReadReplicaPromotion(previous, desired);
    }

    public static boolean isSourceDBClusterIdentifierMutable(final ResourceModel previous, final ResourceModel desired) {
        return Objects.equal(previous.getSourceDBClusterIdentifier(), desired.getSourceDBClusterIdentifier()) ||
                ResourceModelHelper.isDBClusterReadReplicaPromotion(previous, desired);
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

    public static boolean isChangeMutable(final ResourceModel previous, final ResourceModel desired, final DBInstance instance, RequestLogger requestLogger) {
        final String currentEngine = (!StringUtils.isNullOrEmpty(previous.getEngine()) || instance == null) ? previous.getEngine() : instance.engine();
        final boolean isChangeMutable = isEngineMutable(previous, desired, currentEngine) &&
                isPerformanceInsightsKMSKeyIdMutable(previous, desired) &&
                isAvailabilityZoneChangeMutable(previous, desired) &&
                isSourceDBInstanceIdentifierMutable(previous, desired) &&
                isDBSnapshotIdentifierMutable(previous, desired) &&
                isDBClusterSnapshotIdentifierMutable(previous, desired) &&
                isUseLatestRestorableTimeMutable(previous, desired) &&
                isRestoreTimeMutable(previous, desired) &&
                isSourceDBInstanceAutomatedBackupsArnMutable(previous, desired) &&
                isSourceDbiResourceIdMutable(previous, desired) &&
                isSourceDBClusterIdentifierMutable(previous, desired);

        if (!isChangeMutable) {
            final Map<String, Boolean> checksMap = MapUtils.of("isEngineMutable", isEngineMutable(previous, desired, currentEngine),
                    "isPerformanceInsightsKMSKeyIdMutable", isPerformanceInsightsKMSKeyIdMutable(previous, desired),
                    "isAvailabilityZoneChangeMutable", isAvailabilityZoneChangeMutable(previous, desired),
                    "isSourceDBInstanceIdentifierMutable", isSourceDBInstanceIdentifierMutable(previous, desired),
                    "isDBSnapshotIdentifierMutable", isDBSnapshotIdentifierMutable(previous, desired),
                    "isDBClusterSnapshotIdentifierMutable", isDBClusterSnapshotIdentifierMutable(previous, desired));

            checksMap.put("isUseLatestRestorableTimeMutable", isUseLatestRestorableTimeMutable(previous, desired));
            checksMap.put("isRestoreTimeMutable", isRestoreTimeMutable(previous, desired));
            checksMap.put("isSourceDBInstanceAutomatedBackupsArnMutable", isSourceDBInstanceAutomatedBackupsArnMutable(previous, desired));
            checksMap.put("isSourceDbiResourceIdMutable", isSourceDbiResourceIdMutable(previous, desired));
            checksMap.put("isSourceDBClusterIdentifierMutable", isSourceDBClusterIdentifierMutable(previous, desired));
            requestLogger.log(String.format("isChangeMutable: %b", isChangeMutable), checksMap);
        }

        return isChangeMutable;
    }
}
