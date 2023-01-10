package software.amazon.rds.dbinstance;

import static software.amazon.rds.common.util.DifferenceUtils.diff;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;

import com.google.common.annotations.VisibleForTesting;
import software.amazon.awssdk.services.ec2.model.DescribeSecurityGroupsRequest;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.rds.model.AddRoleToDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.CloudwatchLogsExportConfiguration;
import software.amazon.awssdk.services.rds.model.CreateDbInstanceReadReplicaRequest;
import software.amazon.awssdk.services.rds.model.CreateDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.DeleteDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbEngineVersionsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbParameterGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsRequest;
import software.amazon.awssdk.services.rds.model.DescribeEventsRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.PromoteReadReplicaRequest;
import software.amazon.awssdk.services.rds.model.RebootDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.RemoveRoleFromDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.RestoreDbInstanceFromDbSnapshotRequest;
import software.amazon.awssdk.services.rds.model.RestoreDbInstanceToPointInTimeRequest;
import software.amazon.awssdk.services.rds.model.SourceType;
import software.amazon.awssdk.utils.StringUtils;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.dbinstance.util.ResourceModelHelper;

public class Translator {
    public static DescribeDbInstancesRequest describeDbInstancesRequest(final ResourceModel model) {
        return DescribeDbInstancesRequest.builder()
                .dbInstanceIdentifier(model.getDBInstanceIdentifier())
                .build();
    }

    public static DescribeDbClustersRequest describeDbClustersRequest(final ResourceModel model) {
        return DescribeDbClustersRequest.builder()
                .dbClusterIdentifier(model.getDBClusterIdentifier())
                .build();
    }

    public static DescribeDbInstancesRequest describeDbInstancesRequest(final String nextToken) {
        return DescribeDbInstancesRequest.builder()
                .marker(nextToken)
                .build();
    }

    public static DescribeDbSnapshotsRequest describeDbSnapshotsRequest(final ResourceModel model) {
        return DescribeDbSnapshotsRequest.builder()
                .dbSnapshotIdentifier(model.getDBSnapshotIdentifier())
                .build();
    }

    public static CreateDbInstanceReadReplicaRequest createDbInstanceReadReplicaRequest(
            final ResourceModel model,
            final Tagging.TagSet tagSet
    ) {
        return CreateDbInstanceReadReplicaRequest.builder()
                .autoMinorVersionUpgrade(model.getAutoMinorVersionUpgrade())
                .availabilityZone(model.getAvailabilityZone())
                .customIamInstanceProfile(model.getCustomIAMInstanceProfile())
                .dbInstanceClass(model.getDBInstanceClass())
                .dbInstanceIdentifier(model.getDBInstanceIdentifier())
                .dbSubnetGroupName(model.getDBSubnetGroupName())
                .deletionProtection(model.getDeletionProtection())
                .domain(model.getDomain())
                .domainIAMRoleName(model.getDomainIAMRoleName())
                .enableCloudwatchLogsExports(model.getEnableCloudwatchLogsExports())
                .enableIAMDatabaseAuthentication(model.getEnableIAMDatabaseAuthentication())
                .enablePerformanceInsights(model.getEnablePerformanceInsights())
                .iops(model.getIops())
                .kmsKeyId(model.getKmsKeyId())
                .monitoringInterval(model.getMonitoringInterval())
                .monitoringRoleArn(model.getMonitoringRoleArn())
                .multiAZ(model.getMultiAZ())
                .networkType(model.getNetworkType())
                .optionGroupName(model.getOptionGroupName())
                .performanceInsightsKMSKeyId(model.getPerformanceInsightsKMSKeyId())
                .performanceInsightsRetentionPeriod(model.getPerformanceInsightsRetentionPeriod())
                .port(translatePortToSdk(model.getPort()))
                .processorFeatures(translateProcessorFeaturesToSdk(model.getProcessorFeatures()))
                .publiclyAccessible(model.getPubliclyAccessible())
                .replicaMode(model.getReplicaMode())
                .sourceDBInstanceIdentifier(model.getSourceDBInstanceIdentifier())
                .sourceRegion(StringUtils.isNotBlank(model.getSourceRegion()) ? model.getSourceRegion() : null)
                .storageThroughput(model.getStorageThroughput())
                .storageType(model.getStorageType())
                .tags(Tagging.translateTagsToSdk(tagSet))
                .useDefaultProcessorFeatures(model.getUseDefaultProcessorFeatures())
                .vpcSecurityGroupIds(CollectionUtils.isNotEmpty(model.getVPCSecurityGroups()) ? model.getVPCSecurityGroups() : null)
                .build();
    }

    public static RestoreDbInstanceFromDbSnapshotRequest restoreDbInstanceFromSnapshotRequestV12(
            final ResourceModel model
    ) {
        final RestoreDbInstanceFromDbSnapshotRequest.Builder builder = RestoreDbInstanceFromDbSnapshotRequest.builder()
                .autoMinorVersionUpgrade(model.getAutoMinorVersionUpgrade())
                .availabilityZone(model.getAvailabilityZone())
                .customIamInstanceProfile(model.getCustomIAMInstanceProfile())
                .dbInstanceClass(model.getDBInstanceClass())
                .dbInstanceIdentifier(model.getDBInstanceIdentifier())
                .dbName(model.getDBName())
                .dbSnapshotIdentifier(model.getDBSnapshotIdentifier())
                .dbSubnetGroupName(model.getDBSubnetGroupName())
                .engine(model.getEngine())
                .licenseModel(model.getLicenseModel())
                .multiAZ(model.getMultiAZ())
                .networkType(model.getNetworkType())
                .optionGroupName(model.getOptionGroupName())
                .port(translatePortToSdk(model.getPort()));

        if (ResourceModelHelper.shouldSetStorageTypeOnRestoreFromSnapshot(model)) {
            builder.storageType(model.getStorageType());
        }

        return builder.build();
    }

    public static RestoreDbInstanceFromDbSnapshotRequest restoreDbInstanceFromSnapshotRequest(
            final ResourceModel model,
            final Tagging.TagSet tagSet
    ) {
        final RestoreDbInstanceFromDbSnapshotRequest.Builder builder = RestoreDbInstanceFromDbSnapshotRequest.builder()
                .autoMinorVersionUpgrade(model.getAutoMinorVersionUpgrade())
                .availabilityZone(model.getAvailabilityZone())
                .customIamInstanceProfile(model.getCustomIAMInstanceProfile())
                .dbInstanceClass(model.getDBInstanceClass())
                .dbInstanceIdentifier(model.getDBInstanceIdentifier())
                .dbName(model.getDBName())
                .dbParameterGroupName(model.getDBParameterGroupName())
                .dbClusterSnapshotIdentifier(model.getDBClusterSnapshotIdentifier())
                .dbSnapshotIdentifier(model.getDBSnapshotIdentifier())
                .dbSubnetGroupName(model.getDBSubnetGroupName())
                .deletionProtection(model.getDeletionProtection())
                .domain(model.getDomain())
                .domainIAMRoleName(model.getDomainIAMRoleName())
                .enableCloudwatchLogsExports(model.getEnableCloudwatchLogsExports())
                .enableIAMDatabaseAuthentication(model.getEnableIAMDatabaseAuthentication())
                .engine(model.getEngine())
                .licenseModel(model.getLicenseModel())
                .multiAZ(model.getMultiAZ())
                .networkType(model.getNetworkType())
                .optionGroupName(model.getOptionGroupName())
                .port(translatePortToSdk(model.getPort()))
                .processorFeatures(translateProcessorFeaturesToSdk(model.getProcessorFeatures()))
                .publiclyAccessible(model.getPubliclyAccessible())
                .tags(Tagging.translateTagsToSdk(tagSet))
                .tdeCredentialArn(model.getTdeCredentialArn())
                .tdeCredentialPassword(model.getTdeCredentialPassword())
                .useDefaultProcessorFeatures(model.getUseDefaultProcessorFeatures())
                .vpcSecurityGroupIds(CollectionUtils.isNotEmpty(model.getVPCSecurityGroups()) ? model.getVPCSecurityGroups() : null);

        if (ResourceModelHelper.shouldSetStorageTypeOnRestoreFromSnapshot(model)) {
            builder.storageType(model.getStorageType());
        }

        return builder.build();
    }

    public static CreateDbInstanceRequest createDbInstanceRequestV12(
            final ResourceModel model
    ) {
        return CreateDbInstanceRequest.builder()
                .allocatedStorage(getAllocatedStorage(model))
                .autoMinorVersionUpgrade(model.getAutoMinorVersionUpgrade())
                .availabilityZone(model.getAvailabilityZone())
                .backupRetentionPeriod(model.getBackupRetentionPeriod())
                .customIamInstanceProfile(model.getCustomIAMInstanceProfile())
                .characterSetName(model.getCharacterSetName())
                .dbInstanceClass(model.getDBInstanceClass())
                .dbInstanceIdentifier(model.getDBInstanceIdentifier())
                .dbName(model.getDBName())
                .dbParameterGroupName(model.getDBParameterGroupName())
                .dbSecurityGroups(model.getDBSecurityGroups())
                .dbSubnetGroupName(model.getDBSubnetGroupName())
                .engine(model.getEngine())
                .engineVersion(model.getEngineVersion())
                .manageMasterUserPassword(model.getManageMasterUserPassword())
                .iops(model.getIops())
                .licenseModel(model.getLicenseModel())
                .masterUserPassword(model.getMasterUserPassword())
                .masterUsername(model.getMasterUsername())
                .masterUserSecretKmsKeyId(model.getMasterUserSecret() == null ? null : model.getMasterUserSecret().getKmsKeyId())
                .multiAZ(model.getMultiAZ())
                .networkType(model.getNetworkType())
                .optionGroupName(model.getOptionGroupName())
                .port(translatePortToSdk(model.getPort()))
                .preferredBackupWindow(model.getPreferredBackupWindow())
                .preferredMaintenanceWindow(model.getPreferredMaintenanceWindow())
                .build();
    }

    public static CreateDbInstanceRequest createDbInstanceRequest(
            final ResourceModel model,
            final Tagging.TagSet tagSet
    ) {
        final CreateDbInstanceRequest.Builder builder = CreateDbInstanceRequest.builder()
                .allocatedStorage(getAllocatedStorage(model))
                .autoMinorVersionUpgrade(model.getAutoMinorVersionUpgrade())
                .availabilityZone(model.getAvailabilityZone())
                .backupRetentionPeriod(model.getBackupRetentionPeriod())
                .caCertificateIdentifier(model.getCACertificateIdentifier())
                .characterSetName(model.getCharacterSetName())
                .copyTagsToSnapshot(model.getCopyTagsToSnapshot())
                .customIamInstanceProfile(model.getCustomIAMInstanceProfile())
                .dbClusterIdentifier(model.getDBClusterIdentifier())
                .dbInstanceClass(model.getDBInstanceClass())
                .dbInstanceIdentifier(model.getDBInstanceIdentifier())
                .dbName(model.getDBName())
                .dbParameterGroupName(model.getDBParameterGroupName())
                .dbSubnetGroupName(model.getDBSubnetGroupName())
                .deletionProtection(model.getDeletionProtection())
                .domain(model.getDomain())
                .domainIAMRoleName(model.getDomainIAMRoleName())
                .enableCloudwatchLogsExports(model.getEnableCloudwatchLogsExports())
                .enableIAMDatabaseAuthentication(model.getEnableIAMDatabaseAuthentication())
                .engine(model.getEngine())
                .engineVersion(model.getEngineVersion())
                .manageMasterUserPassword(model.getManageMasterUserPassword())
                .iops(model.getIops())
                .kmsKeyId(model.getKmsKeyId())
                .licenseModel(model.getLicenseModel())
                .masterUsername(model.getMasterUsername())
                .masterUserPassword(model.getMasterUserPassword())
                .masterUserSecretKmsKeyId(model.getMasterUserSecret() == null ? null : model.getMasterUserSecret().getKmsKeyId())
                .maxAllocatedStorage(model.getMaxAllocatedStorage())
                .monitoringInterval(model.getMonitoringInterval())
                .monitoringRoleArn(model.getMonitoringRoleArn())
                .multiAZ(model.getMultiAZ())
                .ncharCharacterSetName(model.getNcharCharacterSetName())
                .networkType(model.getNetworkType())
                .optionGroupName(model.getOptionGroupName())
                .performanceInsightsRetentionPeriod(model.getPerformanceInsightsRetentionPeriod())
                .port(translatePortToSdk(model.getPort()))
                .preferredBackupWindow(model.getPreferredBackupWindow())
                .preferredMaintenanceWindow(model.getPreferredMaintenanceWindow())
                .promotionTier(model.getPromotionTier())
                .processorFeatures(translateProcessorFeaturesToSdk(model.getProcessorFeatures()))
                .publiclyAccessible(model.getPubliclyAccessible())
                .storageEncrypted(model.getStorageEncrypted())
                .storageThroughput(model.getStorageThroughput())
                .storageType(model.getStorageType())
                .tags(Tagging.translateTagsToSdk(tagSet))
                .tdeCredentialArn(model.getTdeCredentialArn())
                .tdeCredentialPassword(model.getTdeCredentialPassword())
                .timezone(model.getTimezone())
                .vpcSecurityGroupIds(CollectionUtils.isNotEmpty(model.getVPCSecurityGroups()) ? model.getVPCSecurityGroups() : null);

        // Set PerformanceInsightsKMSKeyId only if EnablePerformanceInsights is true.
        // The point is that it is a completely legitimate create from the CFN perspective and
        // enabling performance insights would only require toggling EnablePerformanceInsights to true.
        if (BooleanUtils.isTrue(model.getEnablePerformanceInsights())) {
            builder.enablePerformanceInsights(model.getEnablePerformanceInsights());
            builder.performanceInsightsKMSKeyId(model.getPerformanceInsightsKMSKeyId());
        }

        return builder.build();
    }

    static RestoreDbInstanceToPointInTimeRequest restoreDbInstanceToPointInTimeRequest(
            final ResourceModel model,
            final Tagging.TagSet tagSet
    ) {
        final Instant restoreTimeInstant = StringUtils.isNotBlank(model.getRestoreTime()) ? ZonedDateTime.parse(model.getRestoreTime()).toInstant() : null;

        final RestoreDbInstanceToPointInTimeRequest.Builder builder = RestoreDbInstanceToPointInTimeRequest.builder()
                .autoMinorVersionUpgrade(model.getAutoMinorVersionUpgrade())
                .availabilityZone(model.getAvailabilityZone())
                .copyTagsToSnapshot(model.getCopyTagsToSnapshot())
                .customIamInstanceProfile(model.getCustomIAMInstanceProfile())
                .dbInstanceClass(model.getDBInstanceClass())
                .dbName(model.getDBName())
                .dbParameterGroupName(model.getDBParameterGroupName())
                .dbSubnetGroupName(model.getDBSubnetGroupName())
                .deletionProtection(model.getDeletionProtection())
                .domain(model.getDomain())
                .domainIAMRoleName(model.getDomainIAMRoleName())
                .enableCloudwatchLogsExports(model.getEnableCloudwatchLogsExports())
                .enableIAMDatabaseAuthentication(model.getEnableIAMDatabaseAuthentication())
                .engine(model.getEngine())
                .iops(model.getIops())
                .licenseModel(model.getLicenseModel())
                .maxAllocatedStorage(model.getMaxAllocatedStorage())
                .multiAZ(model.getMultiAZ())
                .networkType(model.getNetworkType())
                .optionGroupName(model.getOptionGroupName())
                .port(translatePortToSdk(model.getPort()))
                .processorFeatures(translateProcessorFeaturesToSdk(model.getProcessorFeatures()))
                .publiclyAccessible(model.getPubliclyAccessible())
                .restoreTime(restoreTimeInstant)
                .sourceDBInstanceIdentifier(model.getSourceDBInstanceIdentifier())
                .sourceDbiResourceId(model.getSourceDbiResourceId())
                .sourceDBInstanceAutomatedBackupsArn(model.getSourceDBInstanceAutomatedBackupsArn())
                .tags(Tagging.translateTagsToSdk(tagSet))
                .targetDBInstanceIdentifier(model.getDBInstanceIdentifier()) // We only work with DBInstanceId not TargetDBInstanceId
                .tdeCredentialArn(model.getTdeCredentialArn())
                .tdeCredentialPassword(model.getTdeCredentialPassword())
                .useDefaultProcessorFeatures(model.getUseDefaultProcessorFeatures())
                .useLatestRestorableTime(model.getUseLatestRestorableTime())
                .vpcSecurityGroupIds(CollectionUtils.isNotEmpty(model.getVPCSecurityGroups()) ? model.getVPCSecurityGroups() : null);

        if (ResourceModelHelper.shouldSetStorageTypeOnRestoreFromSnapshot(model)) {
            builder.storageType(model.getStorageType());
        }

        return builder.build();
    }

    public static ModifyDbInstanceRequest modifyDbInstanceRequestV12(
            final ResourceModel previousModel,
            final ResourceModel desiredModel,
            final Boolean isRollback
    ) {
        final ModifyDbInstanceRequest.Builder builder = ModifyDbInstanceRequest.builder()
                .allowMajorVersionUpgrade(desiredModel.getAllowMajorVersionUpgrade())
                .applyImmediately(Boolean.TRUE)
                .autoMinorVersionUpgrade(diff(previousModel.getAutoMinorVersionUpgrade(), desiredModel.getAutoMinorVersionUpgrade()))
                .backupRetentionPeriod(diff(previousModel.getBackupRetentionPeriod(), desiredModel.getBackupRetentionPeriod()))
                .dbInstanceClass(diff(previousModel.getDBInstanceClass(), desiredModel.getDBInstanceClass()))
                .dbInstanceIdentifier(desiredModel.getDBInstanceIdentifier())
                .dbParameterGroupName(diff(previousModel.getDBParameterGroupName(), desiredModel.getDBParameterGroupName()))
                .dbSecurityGroups(diff(previousModel.getDBSecurityGroups(), desiredModel.getDBSecurityGroups()))
                .engineVersion(diff(previousModel.getEngineVersion(), desiredModel.getEngineVersion()))
                .manageMasterUserPassword(diff(previousModel.getManageMasterUserPassword(), desiredModel.getManageMasterUserPassword()))
                .masterUserPassword(diff(previousModel.getMasterUserPassword(), desiredModel.getMasterUserPassword()))
                .masterUserSecretKmsKeyId(diff(previousModel.getMasterUserSecret(), desiredModel.getMasterUserSecret()) != null ? desiredModel.getMasterUserSecret().getKmsKeyId() : null)
                .multiAZ(diff(previousModel.getMultiAZ(), desiredModel.getMultiAZ()))
                .networkType(diff(previousModel.getNetworkType(), desiredModel.getNetworkType()))
                .optionGroupName(diff(previousModel.getOptionGroupName(), desiredModel.getOptionGroupName()))
                .preferredBackupWindow(diff(previousModel.getPreferredBackupWindow(), desiredModel.getPreferredBackupWindow()))
                .preferredMaintenanceWindow(diff(previousModel.getPreferredMaintenanceWindow(), desiredModel.getPreferredMaintenanceWindow()))
                .publiclyAccessible(diff(previousModel.getPubliclyAccessible(), desiredModel.getPubliclyAccessible()))
                .replicaMode(diff(previousModel.getReplicaMode(), desiredModel.getReplicaMode()));

        if (BooleanUtils.isNotTrue(isRollback)) {
            builder.allocatedStorage(diff(getAllocatedStorage(previousModel), getAllocatedStorage(desiredModel)));
            builder.engineVersion(diff(previousModel.getEngineVersion(), desiredModel.getEngineVersion()));
            builder.iops(diff(previousModel.getIops(), desiredModel.getIops()));
        }

        return builder.build();
    }

    public static ModifyDbInstanceRequest modifyDbInstanceRequest(
            final ResourceModel previousModel,
            final ResourceModel desiredModel,
            final Boolean isRollback
    ) {
        final ModifyDbInstanceRequest.Builder builder = ModifyDbInstanceRequest.builder()
                .allowMajorVersionUpgrade(desiredModel.getAllowMajorVersionUpgrade())
                .applyImmediately(Boolean.TRUE)
                .autoMinorVersionUpgrade(diff(previousModel.getAutoMinorVersionUpgrade(), desiredModel.getAutoMinorVersionUpgrade()))
                .backupRetentionPeriod(diff(previousModel.getBackupRetentionPeriod(), desiredModel.getBackupRetentionPeriod()))
                // always use desired model value for certificateRotationRestart
                .certificateRotationRestart(desiredModel.getCertificateRotationRestart())
                .caCertificateIdentifier(diff(previousModel.getCACertificateIdentifier(), desiredModel.getCACertificateIdentifier()))
                .copyTagsToSnapshot(diff(previousModel.getCopyTagsToSnapshot(), desiredModel.getCopyTagsToSnapshot()))
                .dbInstanceClass(diff(previousModel.getDBInstanceClass(), desiredModel.getDBInstanceClass()))
                .dbInstanceIdentifier(desiredModel.getDBInstanceIdentifier())
                .dbParameterGroupName(diff(previousModel.getDBParameterGroupName(), desiredModel.getDBParameterGroupName()))
                .dbPortNumber(translatePortToSdk(diff(previousModel.getPort(), desiredModel.getPort())))
                .deletionProtection(diff(previousModel.getDeletionProtection(), desiredModel.getDeletionProtection()))
                .domain(diff(previousModel.getDomain(), desiredModel.getDomain()))
                .domainIAMRoleName(diff(previousModel.getDomainIAMRoleName(), desiredModel.getDomainIAMRoleName()))
                .enableIAMDatabaseAuthentication(diff(previousModel.getEnableIAMDatabaseAuthentication(), desiredModel.getEnableIAMDatabaseAuthentication()))
                .licenseModel(diff(previousModel.getLicenseModel(), desiredModel.getLicenseModel()))
                .masterUserPassword(diff(previousModel.getMasterUserPassword(), desiredModel.getMasterUserPassword()))
                .maxAllocatedStorage(diff(previousModel.getMaxAllocatedStorage(), desiredModel.getMaxAllocatedStorage()))
                .monitoringInterval(diff(previousModel.getMonitoringInterval(), desiredModel.getMonitoringInterval()))
                .monitoringRoleArn(diff(previousModel.getMonitoringRoleArn(), desiredModel.getMonitoringRoleArn()))
                .multiAZ(diff(previousModel.getMultiAZ(), desiredModel.getMultiAZ()))
                .networkType(diff(previousModel.getNetworkType(), desiredModel.getNetworkType()))
                .optionGroupName(diff(previousModel.getOptionGroupName(), desiredModel.getOptionGroupName()))
                .performanceInsightsRetentionPeriod(diff(previousModel.getPerformanceInsightsRetentionPeriod(), desiredModel.getPerformanceInsightsRetentionPeriod()))
                .preferredBackupWindow(diff(previousModel.getPreferredBackupWindow(), desiredModel.getPreferredBackupWindow()))
                .preferredMaintenanceWindow(diff(previousModel.getPreferredMaintenanceWindow(), desiredModel.getPreferredMaintenanceWindow()))
                .promotionTier(diff(previousModel.getPromotionTier(), desiredModel.getPromotionTier()))
                .publiclyAccessible(diff(previousModel.getPubliclyAccessible(), desiredModel.getPubliclyAccessible()))
                .replicaMode(diff(previousModel.getReplicaMode(), desiredModel.getReplicaMode()))
                .storageThroughput(diff(previousModel.getStorageThroughput(), desiredModel.getStorageThroughput()))
                .storageType(diff(previousModel.getStorageType(), desiredModel.getStorageType()))
                .tdeCredentialArn(diff(previousModel.getTdeCredentialArn(), desiredModel.getTdeCredentialArn()))
                .tdeCredentialPassword(diff(previousModel.getTdeCredentialPassword(), desiredModel.getTdeCredentialPassword()))
                .vpcSecurityGroupIds(diff(previousModel.getVPCSecurityGroups(), desiredModel.getVPCSecurityGroups()));

        if (!Objects.deepEquals(previousModel.getEnableCloudwatchLogsExports(), desiredModel.getEnableCloudwatchLogsExports())) {
            final CloudwatchLogsExportConfiguration cloudwatchLogsExportConfiguration = buildTranslateCloudwatchLogsExportConfiguration(
                    previousModel.getEnableCloudwatchLogsExports(),
                    desiredModel.getEnableCloudwatchLogsExports()
            );
            builder.cloudwatchLogsExportConfiguration(cloudwatchLogsExportConfiguration);
        }

        if (BooleanUtils.isNotTrue(isRollback)) {
            builder.allocatedStorage(diff(getAllocatedStorage(previousModel), getAllocatedStorage(desiredModel)));
            builder.engineVersion(diff(previousModel.getEngineVersion(), desiredModel.getEngineVersion()));
            builder.iops(diff(previousModel.getIops(), desiredModel.getIops()));
        }

        if (shouldSetProcessorFeatures(previousModel, desiredModel)) {
            builder.processorFeatures(translateProcessorFeaturesToSdk(desiredModel.getProcessorFeatures()));
            builder.useDefaultProcessorFeatures(desiredModel.getUseDefaultProcessorFeatures());
        }

        // EnablePerformanceInsights (EPI) and PerformanceInsightsKMSKeyId (PKI) are coupled parameters.
        // The following logic stands:
        // 0. If EPI or PKI are changed, provide the diff unconditionally.
        // 1. if EPI is true, provide the desired PKI unconditionally.
        // 2. if PKI is changed, provide the desired EPI unconditionally.
        final Boolean enablePerformanceInsightsDiff = diff(previousModel.getEnablePerformanceInsights(), desiredModel.getEnablePerformanceInsights());
        final String performanceInsightsKMSKeyIdDiff = diff(previousModel.getPerformanceInsightsKMSKeyId(), desiredModel.getPerformanceInsightsKMSKeyId());

        if (enablePerformanceInsightsDiff != null || performanceInsightsKMSKeyIdDiff != null) {
            builder.enablePerformanceInsights(enablePerformanceInsightsDiff);
            builder.performanceInsightsKMSKeyId(performanceInsightsKMSKeyIdDiff);
            if (BooleanUtils.isTrue(desiredModel.getEnablePerformanceInsights())) {
                builder.performanceInsightsKMSKeyId(desiredModel.getPerformanceInsightsKMSKeyId());
            }
            if (performanceInsightsKMSKeyIdDiff != null) {
                builder.enablePerformanceInsights(desiredModel.getEnablePerformanceInsights());
            }
        }

        if (BooleanUtils.isTrue(desiredModel.getManageMasterUserPassword())) {
            builder.manageMasterUserPassword(true);
            builder.masterUserSecretKmsKeyId(desiredModel.getMasterUserSecret().getKmsKeyId());
        } else {
            builder.manageMasterUserPassword(getManageMasterUserPassword(previousModel, desiredModel));
        }

        return builder.build();
    }

    private static Boolean getManageMasterUserPassword(final ResourceModel previous, final ResourceModel desired) {
        if (null != desired.getManageMasterUserPassword()) {
            return desired.getManageMasterUserPassword();
        }
        if (BooleanUtils.isTrue(previous.getManageMasterUserPassword()) && BooleanUtils.isNotTrue(desired.getManageMasterUserPassword())) {
            return false;
        }
        return null;
    }

    public static ModifyDbInstanceRequest modifyDbInstanceAfterCreateRequestV12(final ResourceModel model) {
        return ModifyDbInstanceRequest.builder()
                .applyImmediately(Boolean.TRUE)
                .dbInstanceIdentifier(model.getDBInstanceIdentifier())
                .allocatedStorage(getAllocatedStorage(model))
                .backupRetentionPeriod(model.getBackupRetentionPeriod())
                .dbParameterGroupName(model.getDBParameterGroupName())
                .dbSecurityGroups(model.getDBSecurityGroups())
                .engineVersion(model.getEngineVersion())
                .iops(model.getIops())
                .masterUserPassword(model.getMasterUserPassword())
                .preferredBackupWindow(model.getPreferredBackupWindow())
                .preferredMaintenanceWindow(model.getPreferredMaintenanceWindow())
                .build();
    }

    public static ModifyDbInstanceRequest modifyDbInstanceAfterCreateRequest(final ResourceModel model) {
        final ModifyDbInstanceRequest.Builder builder = ModifyDbInstanceRequest.builder()
                .applyImmediately(Boolean.TRUE)
                .dbInstanceIdentifier(model.getDBInstanceIdentifier())
                .allocatedStorage(getAllocatedStorage(model))
                .backupRetentionPeriod(model.getBackupRetentionPeriod())
                .caCertificateIdentifier(model.getCACertificateIdentifier())
                .dbParameterGroupName(model.getDBParameterGroupName())
                .engineVersion(model.getEngineVersion())
                .iops(model.getIops())
                .masterUserPassword(model.getMasterUserPassword())
                .maxAllocatedStorage(model.getMaxAllocatedStorage())
                .preferredBackupWindow(model.getPreferredBackupWindow())
                .preferredMaintenanceWindow(model.getPreferredMaintenanceWindow());

        if (StorageType.GP3.equals(StorageType.fromString(model.getStorageType()))) {
            builder.storageThroughput(model.getStorageThroughput());
            builder.iops(model.getIops());
            builder.storageType(model.getStorageType());
        }
        return builder.build();
    }

    public static ModifyDbInstanceRequest updateAllocatedStorageRequest(final ResourceModel desiredModel) {
        return ModifyDbInstanceRequest.builder()
                .dbInstanceIdentifier(desiredModel.getDBInstanceIdentifier())
                .allocatedStorage(getAllocatedStorage(desiredModel))
                .applyImmediately(Boolean.TRUE)
                .build();
    }

    public static RemoveRoleFromDbInstanceRequest removeRoleFromDbInstanceRequest(
            final ResourceModel model,
            final DBInstanceRole role
    ) {
        return RemoveRoleFromDbInstanceRequest.builder()
                .dbInstanceIdentifier(model.getDBInstanceIdentifier())
                .roleArn(role.getRoleArn())
                .featureName(role.getFeatureName())
                .build();
    }

    public static AddRoleToDbInstanceRequest addRoleToDbInstanceRequest(
            final ResourceModel model,
            final DBInstanceRole role
    ) {
        return AddRoleToDbInstanceRequest.builder()
                .dbInstanceIdentifier(model.getDBInstanceIdentifier())
                .roleArn(role.getRoleArn())
                .featureName(role.getFeatureName())
                .build();
    }

    public static RebootDbInstanceRequest rebootDbInstanceRequest(final ResourceModel model) {
        return RebootDbInstanceRequest.builder()
                .dbInstanceIdentifier(model.getDBInstanceIdentifier())
                .build();
    }

    public static DescribeSecurityGroupsRequest describeSecurityGroupsRequest(final String vpcId, final String groupName) {
        return DescribeSecurityGroupsRequest.builder()
                .filters(
                        Filter.builder().name("vpc-id").values(vpcId).build(),
                        Filter.builder().name("group-name").values(groupName).build()
                ).build();
    }

    public static CloudwatchLogsExportConfiguration buildTranslateCloudwatchLogsExportConfiguration(
            final Collection<String> previousLogExports,
            final Collection<String> desiredLogExports
    ) {
        final Set<String> logTypesToEnable = new LinkedHashSet<>(Optional.ofNullable(desiredLogExports).orElse(Collections.emptyList()));
        final Set<String> logTypesToDisable = new LinkedHashSet<>(Optional.ofNullable(previousLogExports).orElse(Collections.emptyList()));

        logTypesToEnable.removeAll(Optional.ofNullable(previousLogExports).orElse(Collections.emptyList()));
        logTypesToDisable.removeAll(Optional.ofNullable(desiredLogExports).orElse(Collections.emptyList()));

        if (CollectionUtils.isEmpty(logTypesToEnable) && CollectionUtils.isEmpty(logTypesToDisable)) {
            // This is a no-op
            return null;
        }

        return CloudwatchLogsExportConfiguration.builder()
                .enableLogTypes(logTypesToEnable)
                .disableLogTypes(logTypesToDisable)
                .build();
    }

    public static DeleteDbInstanceRequest deleteDbInstanceRequest(
            final ResourceModel model,
            final String finalDBSnapshotIdentifier
    ) {
        final DeleteDbInstanceRequest.Builder builder = DeleteDbInstanceRequest.builder()
                .dbInstanceIdentifier(model.getDBInstanceIdentifier())
                .deleteAutomatedBackups(model.getDeleteAutomatedBackups());
        if (StringUtils.isEmpty(finalDBSnapshotIdentifier)) {
            builder.skipFinalSnapshot(true);
        } else {
            builder.skipFinalSnapshot(false)
                    .finalDBSnapshotIdentifier(finalDBSnapshotIdentifier);
        }
        return builder.build();
    }

    public static PromoteReadReplicaRequest promoteReadReplicaRequest(final ResourceModel model) {
        return PromoteReadReplicaRequest.builder()
                .dbInstanceIdentifier(model.getDBInstanceIdentifier())
                .backupRetentionPeriod(model.getBackupRetentionPeriod())
                .preferredBackupWindow(model.getPreferredBackupWindow())
                .build();
    }

    public static DescribeDbParameterGroupsRequest describeDbParameterGroupsRequest(final String dbParameterGroupName) {
        return DescribeDbParameterGroupsRequest.builder().dbParameterGroupName(dbParameterGroupName).build();
    }

    public static DescribeDbEngineVersionsRequest describeDbEngineVersionsRequest(
            final String dbParameterGroupFamily,
            final String engine,
            final String engineVersion
    ) {
        return DescribeDbEngineVersionsRequest.builder()
                .dbParameterGroupFamily(dbParameterGroupFamily)
                .engine(engine)
                .engineVersion(engineVersion)
                .build();
    }

    public static DescribeEventsRequest describeEventsRequest(
            final SourceType sourceType,
            final String sourceIdentifier,
            final Collection<String> eventCategories,
            final Instant startTime,
            final Instant endTime
    ) {
        return DescribeEventsRequest.builder()
                .eventCategories(eventCategories.toArray(new String[0]))
                .sourceIdentifier(sourceIdentifier)
                .sourceType(sourceType)
                .startTime(startTime)
                .endTime(endTime)
                .build();
    }

    public static List<ResourceModel> translateDbInstancesFromSdk(
            final List<software.amazon.awssdk.services.rds.model.DBInstance> dbInstances
    ) {
        return streamOfOrEmpty(dbInstances)
                .map(Translator::translateDbInstanceFromSdk)
                .collect(Collectors.toList());
    }

    public static ResourceModel.ResourceModelBuilder translateDbInstanceFromSdkBuilder(
            final software.amazon.awssdk.services.rds.model.DBInstance dbInstance
    ) {
        final String dbParameterGroupName = Optional.ofNullable(dbInstance.dbParameterGroups())
                .orElse(Collections.emptyList())
                .stream()
                .map(Translator::translateDBParameterGroupFromSdk)
                .findFirst().orElse(null);

        // {@code DbInstance.port} can contain a null-value, in this case we
        // pick up a value from the corresponding endpoint structure.
        Integer port = dbInstance.dbInstancePort();
        if (port == null || port == 0) {
            if (dbInstance.endpoint() != null) {
                port = dbInstance.endpoint().port();
            }
        }

        Endpoint endpoint = null;
        if (dbInstance.endpoint() != null) {
            endpoint = translateEndpointFromSdk(dbInstance.endpoint());
        }

        final List<Tag> tags = translateTagsFromSdk(dbInstance.tagList());

        String allocatedStorage = null;
        if (dbInstance.allocatedStorage() != null) {
            allocatedStorage = dbInstance.allocatedStorage().toString();
        }

        String domain = null;
        String domainIAMRoleName = null;
        if (CollectionUtils.isNotEmpty(dbInstance.domainMemberships())) {
            domain = dbInstance.domainMemberships().get(0).domain();
            domainIAMRoleName = dbInstance.domainMemberships().get(0).iamRoleName();
        }

        return ResourceModel.builder()
                .allocatedStorage(allocatedStorage)
                .associatedRoles(translateAssociatedRolesFromSdk(dbInstance.associatedRoles()))
                .autoMinorVersionUpgrade(dbInstance.autoMinorVersionUpgrade())
                .availabilityZone(dbInstance.availabilityZone())
                .backupRetentionPeriod(dbInstance.backupRetentionPeriod())
                .certificateDetails(translateCertificateDetailsFromSdk(dbInstance.certificateDetails()))
                .cACertificateIdentifier(dbInstance.caCertificateIdentifier())
                .characterSetName(dbInstance.characterSetName())
                .copyTagsToSnapshot(dbInstance.copyTagsToSnapshot())
                .customIAMInstanceProfile(dbInstance.customIamInstanceProfile())
                .dBClusterIdentifier(dbInstance.dbClusterIdentifier())
                .dBInstanceArn(dbInstance.dbInstanceArn())
                .dBInstanceClass(dbInstance.dbInstanceClass())
                .dBInstanceIdentifier(dbInstance.dbInstanceIdentifier())
                .dbiResourceId(dbInstance.dbiResourceId())
                .dBName(dbInstance.dbName())
                .dBParameterGroupName(dbParameterGroupName)
                .dBSecurityGroups(translateDbSecurityGroupsFromSdk(dbInstance.dbSecurityGroups()))
                .dBSubnetGroupName(translateDbSubnetGroupFromSdk(dbInstance.dbSubnetGroup()))
                .dBSystemId(dbInstance.dbSystemId())
                .domain(domain)
                .domainIAMRoleName(domainIAMRoleName)
                .deletionProtection(dbInstance.deletionProtection())
                .enableCloudwatchLogsExports(translateEnableCloudwatchLogsExport(dbInstance.enabledCloudwatchLogsExports()))
                .enableIAMDatabaseAuthentication(dbInstance.iamDatabaseAuthenticationEnabled())
                .enablePerformanceInsights(dbInstance.performanceInsightsEnabled())
                .endpoint(endpoint)
                .engine(dbInstance.engine())
                .engineVersion(dbInstance.engineVersion())
                .manageMasterUserPassword(dbInstance.masterUserSecret() != null)
                .iops(dbInstance.iops())
                .kmsKeyId(dbInstance.kmsKeyId())
                .licenseModel(dbInstance.licenseModel())
                .masterUsername(dbInstance.masterUsername())
                .masterUserSecret(translateMasterUserSecret(dbInstance.masterUserSecret()))
                .maxAllocatedStorage(dbInstance.maxAllocatedStorage())
                .monitoringInterval(dbInstance.monitoringInterval())
                .monitoringRoleArn(dbInstance.monitoringRoleArn())
                .multiAZ(dbInstance.multiAZ())
                .ncharCharacterSetName(dbInstance.ncharCharacterSetName())
                .networkType(dbInstance.networkType())
                .performanceInsightsKMSKeyId(dbInstance.performanceInsightsKMSKeyId())
                .performanceInsightsRetentionPeriod(dbInstance.performanceInsightsRetentionPeriod())
                .port(port == null ? null : port.toString())
                .preferredBackupWindow(dbInstance.preferredBackupWindow())
                .preferredMaintenanceWindow(dbInstance.preferredMaintenanceWindow())
                .processorFeatures(translateProcessorFeaturesFromSdk(dbInstance.processorFeatures()))
                .promotionTier(dbInstance.promotionTier())
                .publiclyAccessible(dbInstance.publiclyAccessible())
                .replicaMode(dbInstance.replicaModeAsString())
                .sourceDBInstanceIdentifier(dbInstance.readReplicaSourceDBInstanceIdentifier())
                .storageEncrypted(dbInstance.storageEncrypted())
                .storageThroughput(dbInstance.storageThroughput())
                .storageType(dbInstance.storageType())
                .tags(tags)
                .tdeCredentialArn(dbInstance.tdeCredentialArn())
                .timezone(dbInstance.timezone())
                .vPCSecurityGroups(translateVpcSecurityGroupsFromSdk(dbInstance.vpcSecurityGroups()));
    }

    private static Endpoint translateEndpointFromSdk(software.amazon.awssdk.services.rds.model.Endpoint endpoint) {
        return Endpoint.builder()
                .address(endpoint.address())
                .port(endpoint.port() == null ? null : endpoint.port().toString())
                .hostedZoneId(endpoint.hostedZoneId())
                .build();
    }

    public static ResourceModel translateDbInstanceFromSdk(
            final software.amazon.awssdk.services.rds.model.DBInstance dbInstance
    ) {
        return translateDbInstanceFromSdkBuilder(dbInstance).build();
    }

    public static String translateDBParameterGroupFromSdk(final software.amazon.awssdk.services.rds.model.DBParameterGroupStatus parameterGroup) {
        return parameterGroup == null ? null : parameterGroup.dbParameterGroupName();
    }

    public static List<String> translateEnableCloudwatchLogsExport(final Collection<String> enabledCloudwatchLogsExports) {
        return enabledCloudwatchLogsExports == null ? null : new ArrayList<>(enabledCloudwatchLogsExports);
    }

    public static List<String> translateVpcSecurityGroupsFromSdk(
            final Collection<software.amazon.awssdk.services.rds.model.VpcSecurityGroupMembership> vpcSecurityGroups
    ) {
        return vpcSecurityGroups == null ? null : vpcSecurityGroups.stream()
                .map(software.amazon.awssdk.services.rds.model.VpcSecurityGroupMembership::vpcSecurityGroupId)
                .collect(Collectors.toList());
    }

    public static CertificateDetails translateCertificateDetailsFromSdk(software.amazon.awssdk.services.rds.model.CertificateDetails certificateDetails) {
        return certificateDetails == null ? null : CertificateDetails.builder()
                .cAIdentifier(certificateDetails.caIdentifier())
                .validTill(certificateDetails.validTill().toString()).
                build();
    }

    public static List<Tag> translateTagsFromSdk(final Collection<software.amazon.awssdk.services.rds.model.Tag> sdkTags) {
        return streamOfOrEmpty(sdkTags)
                .map(tag -> Tag
                        .builder()
                        .key(tag.key())
                        .value(tag.value())
                        .build()
                )
                .collect(Collectors.toList());
    }

    public static Map<String, String> translateTagsToRequest(final Collection<Tag> tags) {
        return streamOfOrEmpty(tags)
                .collect(Collectors.toMap(Tag::getKey, Tag::getValue, (v1, v2) -> v2, LinkedHashMap::new));
    }

    public static Set<software.amazon.awssdk.services.rds.model.Tag> translateTagsToSdk(final Collection<Tag> tags) {
        return streamOfOrEmpty(tags)
                .map(tag -> software.amazon.awssdk.services.rds.model.Tag.builder()
                        .key(tag.getKey())
                        .value(tag.getValue())
                        .build()
                )
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    public static List<ProcessorFeature> translateProcessorFeaturesFromSdk(
            final Collection<software.amazon.awssdk.services.rds.model.ProcessorFeature> sdkProcessorFeatures
    ) {
        return sdkProcessorFeatures == null ? null : sdkProcessorFeatures.stream()
                .map(processorFeature -> ProcessorFeature
                        .builder()
                        .name(processorFeature.name())
                        .value(processorFeature.value())
                        .build())
                .collect(Collectors.toList());
    }

    public static Set<software.amazon.awssdk.services.rds.model.ProcessorFeature> translateProcessorFeaturesToSdk(
            final Collection<ProcessorFeature> processorFeatures
    ) {
        return processorFeatures == null ? null : processorFeatures.stream()
                .map(processorFeature -> software.amazon.awssdk.services.rds.model.ProcessorFeature
                        .builder()
                        .name(processorFeature.getName())
                        .value(processorFeature.getValue())
                        .build())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    public static List<String> translateDbSecurityGroupsFromSdk(
            final List<software.amazon.awssdk.services.rds.model.DBSecurityGroupMembership> dbSecurityGroupMemberships
    ) {
        return dbSecurityGroupMemberships == null ? null : dbSecurityGroupMemberships.stream()
                .map(software.amazon.awssdk.services.rds.model.DBSecurityGroupMembership::dbSecurityGroupName)
                .collect(Collectors.toList());
    }

    public static String translateDbSubnetGroupFromSdk(
            final software.amazon.awssdk.services.rds.model.DBSubnetGroup dbSubnetGroup
    ) {
        return dbSubnetGroup == null ? null : dbSubnetGroup.dbSubnetGroupName();
    }

    public static List<DBInstanceRole> translateAssociatedRolesFromSdk(
            final Collection<software.amazon.awssdk.services.rds.model.DBInstanceRole> associatedRoles
    ) {
        return streamOfOrEmpty(associatedRoles)
                .map(role -> DBInstanceRole
                        .builder()
                        .featureName(role.featureName())
                        .roleArn(role.roleArn())
                        .build())
                .collect(Collectors.toList());
    }

    public static Collection<software.amazon.awssdk.services.rds.model.DBInstanceRole> translateAssociatedRolesToSdk(
            final Collection<DBInstanceRole> associatedRoles
    ) {
        return streamOfOrEmpty(associatedRoles)
                .map(role -> software.amazon.awssdk.services.rds.model.DBInstanceRole.builder()
                        .featureName(role.getFeatureName())
                        .roleArn(role.getRoleArn())
                        .build())
                .collect(Collectors.toList());
    }

    public static Integer translatePortToSdk(final String portStr) {
        if (StringUtils.isEmpty(portStr)) {
            return null;
        }
        //NumberFormatException
        return Integer.parseInt(portStr, 10);
    }

    private static <T> Stream<T> streamOfOrEmpty(final Collection<T> collection) {
        return Optional.ofNullable(collection)
                .map(Collection::stream)
                .orElseGet(Stream::empty);
    }

    @VisibleForTesting
    static boolean shouldSetProcessorFeatures(final ResourceModel previousModel, final ResourceModel desiredModel) {
        return previousModel != null && desiredModel != null &&
                (!Objects.deepEquals(previousModel.getProcessorFeatures(), desiredModel.getProcessorFeatures()) ||
                        !Objects.equals(previousModel.getUseDefaultProcessorFeatures(), desiredModel.getUseDefaultProcessorFeatures()));
    }

    public static Integer getAllocatedStorage(final ResourceModel model) {
        Integer allocatedStorage = null;
        if (StringUtils.isNotBlank(model.getAllocatedStorage())) {
            allocatedStorage = Integer.parseInt(model.getAllocatedStorage());
        }
        return allocatedStorage;
    }

    public static MasterUserSecret translateMasterUserSecret(final software.amazon.awssdk.services.rds.model.MasterUserSecret sdkSecret) {
        if (sdkSecret == null) {
            return null;
        }

        return MasterUserSecret.builder()
                .secretArn(sdkSecret.secretArn())
                .kmsKeyId(sdkSecret.kmsKeyId())
                .build();
    }
}
