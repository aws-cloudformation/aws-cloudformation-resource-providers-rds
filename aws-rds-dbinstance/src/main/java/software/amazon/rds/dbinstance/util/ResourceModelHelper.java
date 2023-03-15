package software.amazon.rds.dbinstance.util;

import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang3.BooleanUtils;

import com.amazonaws.util.StringUtils;
import com.google.common.collect.ImmutableSet;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.rds.dbinstance.ResourceModel;

public final class ResourceModelHelper {
    private static final String SQLSERVER_ENGINE_PREFIX = "sqlserver";

    private static final Set<String> SQLSERVER_ENGINES_WITH_MIRRORING = ImmutableSet.of(
            "sqlserver-ee",
            "sqlserver-se"
    );

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
                                Optional.ofNullable(model.getBackupRetentionPeriod()).orElse(0) > 0 ||
                                Optional.ofNullable(model.getIops()).orElse(0) > 0 ||
                                Optional.ofNullable(model.getMaxAllocatedStorage()).orElse(0) > 0 ||
                                Optional.ofNullable(model.getStorageThroughput()).orElse(0) > 0 ||
                                (isSqlServer(model) && StringUtils.hasValue(model.getAllocatedStorage())) ||
                                BooleanUtils.isTrue(model.getManageMasterUserPassword()) ||
                                BooleanUtils.isTrue(model.getDeletionProtection())
                );
    }

    public static boolean isSqlServer(final ResourceModel model) {
        final String engine = model.getEngine();
        // treat unknown engines as SQLServer
        return engine == null || engine.startsWith(SQLSERVER_ENGINE_PREFIX);
    }

    public static boolean isRestoreToPointInTime(final ResourceModel model) {
        // Parameters to rely on are UseLatestRestorableTime and RestoreTime to tell if this is a RestoreToPointInTime
        // But SourceDBInstanceAutomatedBackupsArn and DbiResourceId are also unique identifying parameters of RestoreToPointInTime
        return BooleanUtils.isTrue(model.getUseLatestRestorableTime()) || StringUtils.hasValue(model.getRestoreTime()) ||
                StringUtils.hasValue(model.getSourceDBInstanceAutomatedBackupsArn()) || StringUtils.hasValue(model.getSourceDbiResourceId());
    }

    public static boolean isReadReplica(final ResourceModel model) {
        return StringUtils.hasValue(model.getSourceDBInstanceIdentifier());
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
}
