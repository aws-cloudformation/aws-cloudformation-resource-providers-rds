package software.amazon.rds.dbinstance.util;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.rds.dbinstance.ResourceModel;

import java.util.Optional;

public final class UpdateAfterCreateHelper {
    public static boolean shouldUpdateAfterCreate(final ResourceModel model) {
        return (isReadReplica(model) || isRestoreFromSnapshot(model) || isCertificateAuthorityApplied(model)) &&
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

    private static boolean isCertificateAuthorityApplied(final ResourceModel model) {
        return StringUtils.hasValue(model.getCACertificateIdentifier());
    }


    public static boolean isReadReplica(final ResourceModel model) {
        return software.amazon.awssdk.utils.StringUtils.isNotBlank(model.getSourceDBInstanceIdentifier());
    }

    public static boolean isRestoreFromSnapshot(final ResourceModel model) {
        return software.amazon.awssdk.utils.StringUtils.isNotBlank(model.getDBSnapshotIdentifier());
    }
}
