package software.amazon.rds.dbinstance.util;

import com.amazonaws.util.StringUtils;
import org.apache.commons.lang3.BooleanUtils;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.rds.dbinstance.ResourceModel;

import java.util.Objects;
import java.util.Optional;

public final class ResourceModelHelper {
    private final static String STORAGE_TYPE_IO1 = "io1";

    public static boolean shouldUpdateAfterCreate(final ResourceModel model) {
        return (isReadReplica(model) || isRestoreFromSnapshot(model) || isCertificateAuthorityApplied(model) || isRestoreToPointInTime(model)) &&
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
                                Optional.ofNullable(model.getMaxAllocatedStorage()).orElse(0) > 0
                );
    }

    public static boolean isRestoreToPointInTime(final ResourceModel model) {
//         Parameters to rely on are UseLatestRestorableTime and RestoreTime to tell if this is a RestoreToPointInTime
//         But TargetDBInstanceIdentifier, SourceDBInstanceAutomatedBackupsArn and DbiResourceId are also unique identifying parameters of RestoreToPointInTime
        return (model.getUseLatestRestorableTime() != null && model.getUseLatestRestorableTime() == true) || StringUtils.hasValue(model.getRestoreTime())  ||
                StringUtils.hasValue(model.getTargetDBInstanceIdentifier()) || StringUtils.hasValue(model.getSourceDBInstanceAutomatedBackupsArn()) ||
                StringUtils.hasValue(model.getDbiResourceId());
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

    public static boolean shouldSetStorageTypeOnRestoreFromSnapshot(final ResourceModel model) {
        return !Objects.equals(model.getStorageType(), STORAGE_TYPE_IO1) && shouldUpdateAfterCreate(model);
    }
}
