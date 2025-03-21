package software.amazon.rds.dbinstance.util;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang3.BooleanUtils;

import com.amazonaws.arn.Arn;
import com.amazonaws.util.StringUtils;
import com.google.common.collect.ImmutableSet;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.rds.dbinstance.ResourceModel;

public final class ResourceModelHelper {
    private static final Set<String> SQLSERVER_ENGINES_WITH_MIRRORING = ImmutableSet.of(
            "sqlserver-ee",
            "sqlserver-se"
    );
    private static final String SQLSERVER_ENGINE = "sqlserver";
    private static final String MYSQL_ENGINE_PREFIX = "mysql";
    private static final String ORACLE_ENGINE_PREFIX = "oracle";
    private static final String ORACLE_SE2_CDB = "oracle-se2-cdb";
    private static final String ORACLE_EE_CDB = "oracle-ee-cdb";

    public static boolean shouldUpdateAfterCreate(final ResourceModel model,
                                                  final String dbInstanceEngine) {
        return (isReadReplica(model) ||
                isRestoreFromSnapshot(model) ||
                isRestoreFromClusterSnapshot(model) ||
                isRestoreToPointInTime(model)) &&
                (
                        !CollectionUtils.isNullOrEmpty(model.getDBSecurityGroups()) ||
                                StringUtils.hasValue(model.getCACertificateIdentifier()) ||
                                StringUtils.hasValue(model.getDBParameterGroupName()) ||
                                StringUtils.hasValue(model.getEngineVersion()) ||
                                StringUtils.hasValue(model.getMasterUserPassword()) ||
                                StringUtils.hasValue(model.getPreferredBackupWindow()) ||
                                StringUtils.hasValue(model.getPreferredMaintenanceWindow()) ||
                                StringUtils.hasValue(model.getMonitoringRoleArn()) ||
                                Optional.ofNullable(model.getBackupRetentionPeriod()).orElse(0) > 0 ||
                                Optional.ofNullable(model.getMonitoringInterval()).orElse(0) > 0 ||
                                (isSqlServer(model) && isStorageParametersModified(model)) ||
                                BooleanUtils.isTrue(model.getManageMasterUserPassword()) ||
                                BooleanUtils.isTrue(model.getDeletionProtection()) ||
                                BooleanUtils.isTrue(model.getEnablePerformanceInsights()) ||
                                ImmutabilityHelper.isOracleConvertToCDB(dbInstanceEngine, model)
                );
    }

    public static boolean isSqlServer(final ResourceModel model) {
        final String engine = model.getEngine();
        // treat unknown engines as SQLServer
        return engine == null || engine.contains(SQLSERVER_ENGINE);
    }

    public static boolean isMySQL(final ResourceModel model) {
        final String engine = model.getEngine();
        return engine != null && engine.toLowerCase().startsWith(MYSQL_ENGINE_PREFIX);
    }

    public static boolean isStorageParametersModified(final ResourceModel model) {
        return StringUtils.hasValue(model.getAllocatedStorage()) ||
                Optional.ofNullable(model.getIops()).orElse(0) > 0 ||
                Optional.ofNullable(model.getMaxAllocatedStorage()).orElse(0) > 0 ||
                Optional.ofNullable(model.getStorageThroughput()).orElse(0) > 0 ||
                StringUtils.hasValue(model.getStorageType());
    }

    public static boolean isRestoreToPointInTime(final ResourceModel model) {
        // Parameters to rely on are UseLatestRestorableTime and RestoreTime to tell if this is a RestoreToPointInTime
        // But SourceDBInstanceAutomatedBackupsArn and DbiResourceId are also unique identifying parameters of RestoreToPointInTime
        return BooleanUtils.isTrue(model.getUseLatestRestorableTime()) || StringUtils.hasValue(model.getRestoreTime()) ||
                StringUtils.hasValue(model.getSourceDBInstanceAutomatedBackupsArn()) || StringUtils.hasValue(model.getSourceDbiResourceId());
    }

    public static boolean isReadReplica(final ResourceModel model) {
        return isDBInstanceReadReplica(model) || isDBClusterReadReplica(model);
    }

    public static boolean isDBInstanceReadReplica(final ResourceModel model) {
        return StringUtils.hasValue(model.getSourceDBInstanceIdentifier());
    }

    public static boolean isCrossRegionDBInstanceReadReplica(final ResourceModel model, final String currentRegion) {
        final String sourceDBInstanceIdentifier = model.getSourceDBInstanceIdentifier();
        return isDBInstanceReadReplica(model) &&
                isValidArn(sourceDBInstanceIdentifier) &&
                !getRegionFromArn(sourceDBInstanceIdentifier).equals(currentRegion);
    }
    public static boolean isDBClusterReadReplica(final ResourceModel model) {
        return StringUtils.hasValue(model.getSourceDBClusterIdentifier());
    }

    public static boolean isCrossRegionDBClusterReadReplica(final ResourceModel model, final String currentRegion) {
        final String sourceDBClusterIdentifier = model.getSourceDBClusterIdentifier();
        return isDBClusterReadReplica(model) &&
                isValidArn(sourceDBClusterIdentifier) &&
                !getRegionFromArn(sourceDBClusterIdentifier).equals(currentRegion);
    }

    public static boolean isValidArn(final String arn) {
        try {
            Arn.fromString(arn);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }

    }
    public static String getRegionFromArn(final String arn) {
        return Arn.fromString(arn).getRegion();
    }

    public static String getResourceNameFromArn(final String arn) {
        return Arn.fromString(arn).getResource().getResource();
    }

    public static boolean isRestoreFromSnapshot(final ResourceModel model) {
        return StringUtils.hasValue(model.getDBSnapshotIdentifier());
    }

    public static boolean isRestoreFromClusterSnapshot(final ResourceModel model) {
        return StringUtils.hasValue(model.getDBClusterSnapshotIdentifier());
    }

    public static boolean shouldReboot(final ResourceModel model) {
        return StringUtils.hasValue(model.getDBParameterGroupName());
    }

    public static boolean shouldApplyImmediately(final ResourceModel model) {
        Boolean applyImmediately = model.getApplyImmediately();
        // default to true
        return applyImmediately == null || applyImmediately;
    }

    public static Boolean getDefaultMultiAzForEngine(final String engine) {
        if (SQLSERVER_ENGINES_WITH_MIRRORING.contains(engine)) {
            return null;
        }
        return false;
    }

    public static boolean isDBInstanceReadReplicaPromotion(final ResourceModel previous, final ResourceModel desired) {
        return !StringUtils.isNullOrEmpty(previous.getSourceDBInstanceIdentifier()) &&
                StringUtils.isNullOrEmpty(desired.getSourceDBInstanceIdentifier());
    }

    public static boolean isDBClusterReadReplicaPromotion(final ResourceModel previous, final ResourceModel desired) {
        return !StringUtils.isNullOrEmpty(previous.getSourceDBClusterIdentifier()) &&
                StringUtils.isNullOrEmpty(desired.getSourceDBClusterIdentifier());
    }

    public static boolean isReadReplicaPromotion(final ResourceModel previous, final ResourceModel desired) {
        return isDBClusterReadReplicaPromotion(previous, desired) ||
                isDBInstanceReadReplicaPromotion(previous, desired);
    }

    public static boolean shouldStartAutomaticBackupReplication(final ResourceModel previous, final ResourceModel desired) {
        final String previousRegion = getAutomaticBackupReplicationRegion(previous);
        final String desiredRegion = getAutomaticBackupReplicationRegion(desired);

        if (StringUtils.isNullOrEmpty(desiredRegion)) {
            return false;
        }

        if (StringUtils.isNullOrEmpty(previousRegion)) {
            return true;
        }

        return hasAutomaticBackupReplicationChanged(previous, desired);
    }

    public static boolean shouldStopAutomaticBackupReplication(final ResourceModel previous, final ResourceModel desired) {
        final String previousRegion = getAutomaticBackupReplicationRegion(previous);
        final String desiredRegion = getAutomaticBackupReplicationRegion(desired);

        // if region not provided and previous region was, then we stop replication
        if (StringUtils.isNullOrEmpty(previousRegion)) {
            return false;
        }

        if (StringUtils.isNullOrEmpty(desiredRegion)) {
            return true;
        }

        return hasAutomaticBackupReplicationChanged(previous, desired);
    }

    private static boolean hasBackupRetentionPeriodChangedWithNoOverride(final ResourceModel previous, final ResourceModel desired) {
        return (!Objects.equals(getBackupRetentionPeriod(previous), getBackupRetentionPeriod(desired))) && (getBackupRetentionPeriod(desired) != null && getAutomaticBackupReplicationRetentionPeriod(desired) == null);
    }

    private static boolean hasAutomaticBackupReplicationChanged(final ResourceModel previous, final ResourceModel desired) {
        // we only want to use change status of BackupRetentionPeriod if AutomaticBackupReplicationRetentionPeriod is unset / null
        if (hasBackupRetentionPeriodChangedWithNoOverride(previous, desired)) {
            return true;
        }

        // check replication parameters for changes
        boolean crossRegionRetentionChanged = !Objects.equals(getAutomaticBackupReplicationRetentionPeriod(previous), getAutomaticBackupReplicationRetentionPeriod(desired));
        boolean regionChanged = !Objects.equals(getAutomaticBackupReplicationRegion(previous), getAutomaticBackupReplicationRegion(desired));
        boolean kmsKeyIdChanged = !Objects.equals(getAutomaticBackupReplicationKmsKeyId(previous), getAutomaticBackupReplicationKmsKeyId(desired));

        // if any replication parameters have changed
        return crossRegionRetentionChanged || regionChanged || kmsKeyIdChanged;
    }


    public static Integer getBackupRetentionPeriod(final ResourceModel model) {
        if (model == null) {
            return null;
        }

        return model.getBackupRetentionPeriod();
    }

    public static String getAutomaticBackupReplicationRegion(final ResourceModel model) {
        if (model == null) {
            return null;
        }

        return model.getAutomaticBackupReplicationRegion();
    }

    public static Integer getAutomaticBackupReplicationRetentionPeriod(final ResourceModel model) {
        if (model == null) {
            return null;
        }

        return model.getAutomaticBackupReplicationRetentionPeriod();
    }

    public static String getAutomaticBackupReplicationKmsKeyId(final ResourceModel model) {
        if (model == null) {
            return null;
        }
        return model.getAutomaticBackupReplicationKmsKeyId();
    }

    public static boolean isOracleCDBEngine(final String engine) {
        return ORACLE_EE_CDB.equalsIgnoreCase(engine) || ORACLE_SE2_CDB.equalsIgnoreCase(engine);
    }
}
