package software.amazon.rds.dbinstance.util;

import com.amazonaws.util.StringUtils;
import org.apache.commons.lang3.BooleanUtils;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.rds.dbinstance.ResourceModel;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class ResourceModelHelper {
    private static final List<String> STORAGE_TYPES_SET_ON_MODIFY_AFTER_CREATE = Arrays.asList(
            "io1",
            "gp3"
    );

    private static final List<String> SQLSERVER_ENGINES_WITH_MIRRORING = Arrays.asList(
            "sqlserver-ee",
            "sqlserver-se"
    );

    public static boolean shouldUpdateAfterCreate(final ResourceModel model) {
        return (isReadReplica(model) ||
                isRestoreFromSnapshot(model) ||
                isRestoreFromClusterSnapshot(model) ||
                isCertificateAuthorityApplied(model) ||
                isRestoreToPointInTime(model)) &&
                (
                        !CollectionUtils.isNullOrEmpty(model.getDBSecurityGroups()) ||
                                StringUtils.hasValue(model.getAllocatedStorage()) ||
                                StringUtils.hasValue(model.getCACertificateIdentifier()) ||
                                StringUtils.hasValue(model.getDBParameterGroupName()) ||
                                StringUtils.hasValue(model.getEngineVersion()) ||
                                StringUtils.hasValue(model.getMasterUserPassword()) ||
                                StringUtils.hasValue(model.getPreferredBackupWindow()) ||
                                StringUtils.hasValue(model.getPreferredMaintenanceWindow()) ||
                                Optional.ofNullable(model.getBackupRetentionPeriod()).orElse(0) > 0 ||
                                Optional.ofNullable(model.getIops()).orElse(0) > 0 ||
                                Optional.ofNullable(model.getMaxAllocatedStorage()).orElse(0) > 0 ||
                                Optional.ofNullable(model.getStorageThroughput()).orElse(0) > 0
                );
    }

    public static boolean isRestoreToPointInTime(final ResourceModel model) {
        // Parameters to rely on are UseLatestRestorableTime and RestoreTime to tell if this is a RestoreToPointInTime
        // But SourceDBInstanceAutomatedBackupsArn and DbiResourceId are also unique identifying parameters of RestoreToPointInTime
        return  BooleanUtils.isTrue(model.getUseLatestRestorableTime()) || StringUtils.hasValue(model.getRestoreTime())  ||
                StringUtils.hasValue(model.getSourceDBInstanceAutomatedBackupsArn()) || StringUtils.hasValue(model.getSourceDbiResourceId());
    }

    private static boolean isCertificateAuthorityApplied(final ResourceModel model) {
        return StringUtils.hasValue(model.getCACertificateIdentifier());
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

    public static boolean shouldSetStorageTypeOnRestoreFromSnapshot(final ResourceModel model) {
        return !STORAGE_TYPES_SET_ON_MODIFY_AFTER_CREATE.contains(model.getStorageType()) && shouldUpdateAfterCreate(model);
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
