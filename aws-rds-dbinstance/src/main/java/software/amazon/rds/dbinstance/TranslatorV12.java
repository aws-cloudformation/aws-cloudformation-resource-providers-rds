package software.amazon.rds.dbinstance;

import org.apache.commons.lang3.BooleanUtils;

import software.amazon.awssdk.services.rds.model.CreateDbInstanceReadReplicaRequest;
import software.amazon.awssdk.services.rds.model.CreateDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.RestoreDbInstanceFromDbSnapshotRequest;

public class TranslatorV12 extends BaseTranslator {

    public static CreateDbInstanceRequest createDbInstanceRequest(
            final ResourceModel model
    ) {
        return CreateDbInstanceRequest.builder()
                .allocatedStorage(getAllocatedStorage(model))
                .autoMinorVersionUpgrade(model.getAutoMinorVersionUpgrade())
                .availabilityZone(model.getAvailabilityZone())
                .backupRetentionPeriod(model.getBackupRetentionPeriod())
                .characterSetName(model.getCharacterSetName())
                .dbInstanceClass(model.getDBInstanceClass())
                .dbInstanceIdentifier(model.getDBInstanceIdentifier())
                .dbName(model.getDBName())
                .dbParameterGroupName(model.getDBParameterGroupName())
                .dbSecurityGroups(model.getDBSecurityGroups())
                .dbSubnetGroupName(model.getDBSubnetGroupName())
                .engine(model.getEngine())
                .engineVersion(model.getEngineVersion())
                .iops(model.getIops())
                .licenseModel(model.getLicenseModel())
                .masterUserPassword(model.getMasterUserPassword())
                .masterUsername(model.getMasterUsername())
                .multiAZ(model.getMultiAZ())
                .optionGroupName(model.getOptionGroupName())
                .port(translatePortToSdk(model.getPort()))
                .preferredBackupWindow(model.getPreferredBackupWindow())
                .preferredMaintenanceWindow(model.getPreferredMaintenanceWindow())
                .build();
    }

    public static CreateDbInstanceReadReplicaRequest createDbInstanceReadReplicaRequest(
            final ResourceModel model
    ) {
        return CreateDbInstanceReadReplicaRequest.builder()
                .autoMinorVersionUpgrade(model.getAutoMinorVersionUpgrade())
                .availabilityZone(model.getAvailabilityZone())
                .dbInstanceClass(model.getDBInstanceClass())
                .dbInstanceIdentifier(model.getDBInstanceIdentifier())
                .iops(model.getIops())
                .optionGroupName(model.getOptionGroupName())
                .port(translatePortToSdk(model.getPort()))
                .sourceDBInstanceIdentifier(model.getSourceDBInstanceIdentifier())
                .build();
    }

    public static RestoreDbInstanceFromDbSnapshotRequest restoreDbInstanceFromSnapshotRequest(
            final ResourceModel model
    ) {
        return RestoreDbInstanceFromDbSnapshotRequest.builder()
                .autoMinorVersionUpgrade(model.getAutoMinorVersionUpgrade())
                .availabilityZone(model.getAvailabilityZone())
                .dbInstanceClass(model.getDBInstanceClass())
                .dbInstanceIdentifier(model.getDBInstanceIdentifier())
                .dbName(model.getDBName())
                .dbSnapshotIdentifier(model.getDBSnapshotIdentifier())
                .dbSubnetGroupName(model.getDBSubnetGroupName())
                .engine(model.getEngine())
                .iops(model.getIops())
                .licenseModel(model.getLicenseModel())
                .multiAZ(model.getMultiAZ())
                .optionGroupName(model.getOptionGroupName())
                .port(translatePortToSdk(model.getPort()))
                .build();
    }

    public static ModifyDbInstanceRequest modifyDbInstanceRequest(
            final ResourceModel previousModel,
            final ResourceModel desiredModel,
            final Boolean isRollback
    ) {
        final ModifyDbInstanceRequest.Builder builder = ModifyDbInstanceRequest.builder()
                .dbInstanceIdentifier(desiredModel.getDBInstanceIdentifier())
                .dbInstanceClass(desiredModel.getDBInstanceClass())
                .dbSecurityGroups(desiredModel.getDBSecurityGroups())
                .applyImmediately(Boolean.TRUE)
                .masterUserPassword(desiredModel.getMasterUserPassword())
                .dbParameterGroupName(desiredModel.getDBParameterGroupName())
                .backupRetentionPeriod(desiredModel.getBackupRetentionPeriod())
                .preferredBackupWindow(desiredModel.getPreferredBackupWindow())
                .preferredMaintenanceWindow(desiredModel.getPreferredMaintenanceWindow())
                .multiAZ(desiredModel.getMultiAZ())
                .engineVersion(desiredModel.getEngineVersion())
                .allowMajorVersionUpgrade(desiredModel.getAllowMajorVersionUpgrade())
                .autoMinorVersionUpgrade(desiredModel.getAutoMinorVersionUpgrade())
                .optionGroupName(desiredModel.getOptionGroupName());

        if (previousModel != null && BooleanUtils.isTrue(isRollback)) {
            builder.allocatedStorage(
                    canUpdateAllocatedStorage(previousModel.getAllocatedStorage(), desiredModel.getAllocatedStorage()) ? getAllocatedStorage(desiredModel) : getAllocatedStorage(previousModel)
            );
            builder.iops(
                    canUpdateIops(previousModel.getIops(), desiredModel.getIops()) ? desiredModel.getIops() : previousModel.getIops()
            );
        } else {
            builder.allocatedStorage(getAllocatedStorage(desiredModel));
            builder.iops(desiredModel.getIops());
        }

        return builder.build();
    }
}
