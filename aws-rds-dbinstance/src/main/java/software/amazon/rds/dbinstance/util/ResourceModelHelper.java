package software.amazon.rds.dbinstance.util;

import com.amazonaws.util.StringUtils;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.BooleanUtils;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.rds.dbinstance.ResourceModel;

import java.util.Optional;
import java.util.Set;

public final class ResourceModelHelper {
    private static final Set<String> SQLSERVER_ENGINES_WITH_MIRRORING = ImmutableSet.of(
            "sqlserver-ee",
            "sqlserver-se"
    );
    private static final String SQLSERVER_ENGINE = "sqlserver";

    public static boolean shouldUpdateAfterCreate(final ResourceModel model) {
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
                                BooleanUtils.isTrue(model.getEnablePerformanceInsights())
                );
    }

    public static boolean isSqlServer(final ResourceModel model) {
        final String engine = model.getEngine();
        // treat unknown engines as SQLServer
        return engine == null || engine.contains(SQLSERVER_ENGINE);
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

    public static boolean isDBClusterReadReplica(final ResourceModel model) {
        return StringUtils.hasValue(model.getSourceDBClusterIdentifier());
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
}
