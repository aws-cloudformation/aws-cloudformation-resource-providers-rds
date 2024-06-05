package software.amazon.rds.dbinstance;

import static software.amazon.rds.common.util.DifferenceUtils.diff;

import java.time.Instant;
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
import org.apache.commons.lang3.ObjectUtils;

import com.amazonaws.arn.Arn;
import com.google.common.annotations.VisibleForTesting;
import software.amazon.awssdk.services.ec2.model.DescribeSecurityGroupsRequest;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.rds.model.AddRoleToDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.CloudwatchLogsExportConfiguration;
import software.amazon.awssdk.services.rds.model.CreateDbInstanceReadReplicaRequest;
import software.amazon.awssdk.services.rds.model.CreateDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.DeleteDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterSnapshotsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbEngineVersionsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbInstanceAutomatedBackupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbParameterGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsRequest;
import software.amazon.awssdk.services.rds.model.DomainMembership;
import software.amazon.awssdk.services.rds.model.ModifyDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.PromoteReadReplicaRequest;
import software.amazon.awssdk.services.rds.model.RebootDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.RemoveRoleFromDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.RestoreDbInstanceFromDbSnapshotRequest;
import software.amazon.awssdk.services.rds.model.RestoreDbInstanceToPointInTimeRequest;
import software.amazon.awssdk.services.rds.model.StartDbInstanceAutomatedBackupsReplicationRequest;
import software.amazon.awssdk.services.rds.model.StopDbInstanceAutomatedBackupsReplicationRequest;
import software.amazon.awssdk.utils.StringUtils;
import software.amazon.rds.common.handler.Commons;
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

    public static DescribeDbClustersRequest describeDbClusterRequest(final String dbClusterIdentifier) {
        return DescribeDbClustersRequest.builder()
                .dbClusterIdentifier(dbClusterIdentifier)
                .build();
    }

    public static DescribeDbInstancesRequest describeDbInstancesRequest(final String nextToken) {
        return DescribeDbInstancesRequest.builder()
                .marker(nextToken)
                .build();
    }

    public static DescribeDbInstancesRequest describeDbInstanceByDBInstanceIdentifierRequest(final String dbInstanceIdentifier) {
        return DescribeDbInstancesRequest.builder()
                .dbInstanceIdentifier(dbInstanceIdentifier)
                .build();
    }

    public static DescribeDbInstancesRequest describeDbInstanceByResourceIdRequest(final String resourceId) {
        return DescribeDbInstancesRequest.builder()
                .filters(software.amazon.awssdk.services.rds.model.Filter.builder()
                        .name("dbi-resource-id").values(resourceId).build())
                .build();
    }

    public static DescribeDbInstanceAutomatedBackupsRequest describeDBInstanceAutomaticBackup(final String automaticBackupArn) {
        return DescribeDbInstanceAutomatedBackupsRequest.builder()
                .dbInstanceAutomatedBackupsArn(automaticBackupArn)
                .build();
    }

    public static DescribeDbSnapshotsRequest describeDbSnapshotsRequest(final ResourceModel model) {
        return DescribeDbSnapshotsRequest.builder()
                .dbSnapshotIdentifier(model.getDBSnapshotIdentifier())
                .build();
    }

    public static DescribeDbClusterSnapshotsRequest describeDbClusterSnapshotsRequest(final ResourceModel model) {
        return DescribeDbClusterSnapshotsRequest.builder()
                .dbClusterSnapshotIdentifier(model.getDBClusterSnapshotIdentifier())
                .build();
    }

    public static CreateDbInstanceReadReplicaRequest createDbInstanceReadReplicaRequest(
            final ResourceModel model,
            final Tagging.TagSet tagSet,
            final String currentRegion
    ) {
        final CreateDbInstanceReadReplicaRequest.Builder builder = CreateDbInstanceReadReplicaRequest.builder()
                .autoMinorVersionUpgrade(model.getAutoMinorVersionUpgrade())
                .availabilityZone(model.getAvailabilityZone())
                .copyTagsToSnapshot(model.getCopyTagsToSnapshot())
                .customIamInstanceProfile(model.getCustomIAMInstanceProfile())
                .dbInstanceClass(model.getDBInstanceClass())
                .dbInstanceIdentifier(model.getDBInstanceIdentifier())
                .dbSubnetGroupName(model.getDBSubnetGroupName())
                .deletionProtection(model.getDeletionProtection())
                .dedicatedLogVolume(model.getDedicatedLogVolume())
                .domain(model.getDomain())
                .domainAuthSecretArn(model.getDomainAuthSecretArn())
                .domainDnsIps(model.getDomainDnsIps())
                .domainFqdn(model.getDomainFqdn())
                .domainIAMRoleName(model.getDomainIAMRoleName())
                .domainOu(model.getDomainOu())
                .enableCloudwatchLogsExports(model.getEnableCloudwatchLogsExports())
                .enableIAMDatabaseAuthentication(model.getEnableIAMDatabaseAuthentication())
                .enablePerformanceInsights(model.getEnablePerformanceInsights())
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
                .sourceDBClusterIdentifier(model.getSourceDBClusterIdentifier())
                .sourceRegion(StringUtils.isNotBlank(model.getSourceRegion()) ? model.getSourceRegion() : null)
                .tags(Tagging.translateTagsToSdk(tagSet))
                .useDefaultProcessorFeatures(model.getUseDefaultProcessorFeatures())
                .vpcSecurityGroupIds(CollectionUtils.isNotEmpty(model.getVPCSecurityGroups()) ? model.getVPCSecurityGroups() : null);
        if (!ResourceModelHelper.isSqlServer(model)) {
            builder.allocatedStorage(getAllocatedStorage(model));
            builder.iops(model.getIops());
            builder.storageThroughput(model.getStorageThroughput());
            builder.storageType(model.getStorageType());
        }

        if (ResourceModelHelper.isCrossRegionDBInstanceReadReplica(model, currentRegion) && ResourceModelHelper.isMySQL(model) ) {
            builder.dbParameterGroupName(model.getDBParameterGroupName());
        }

        return builder.build();
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

        return builder.build();
    }

    public static RestoreDbInstanceFromDbSnapshotRequest restoreDbInstanceFromSnapshotRequest(
            final ResourceModel model,
            final Tagging.TagSet tagSet
    ) {
        final RestoreDbInstanceFromDbSnapshotRequest.Builder builder = RestoreDbInstanceFromDbSnapshotRequest.builder()
                .autoMinorVersionUpgrade(model.getAutoMinorVersionUpgrade())
                .availabilityZone(model.getAvailabilityZone())
                .copyTagsToSnapshot(model.getCopyTagsToSnapshot())
                .customIamInstanceProfile(model.getCustomIAMInstanceProfile())
                .dbClusterSnapshotIdentifier(model.getDBClusterSnapshotIdentifier())
                .dbInstanceClass(model.getDBInstanceClass())
                .dbInstanceIdentifier(model.getDBInstanceIdentifier())
                .dbName(model.getDBName())
                .dbParameterGroupName(model.getDBParameterGroupName())
                .dbSnapshotIdentifier(model.getDBSnapshotIdentifier())
                .dbSubnetGroupName(model.getDBSubnetGroupName())
                .deletionProtection(model.getDeletionProtection())
                .dedicatedLogVolume(model.getDedicatedLogVolume())
                .domain(model.getDomain())
                .domainAuthSecretArn(model.getDomainAuthSecretArn())
                .domainDnsIps(model.getDomainDnsIps())
                .domainFqdn(model.getDomainFqdn())
                .domainIAMRoleName(model.getDomainIAMRoleName())
                .domainOu(model.getDomainOu())
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
                .vpcSecurityGroupIds(CollectionUtils.isNotEmpty(model.getVPCSecurityGroups()) ? model.getVPCSecurityGroups() : null)
                .engineLifecycleSupport(model.getEngineLifecycleSupport());
        if (!ResourceModelHelper.isSqlServer(model)) {
            builder.allocatedStorage(getAllocatedStorage(model));
            builder.iops(model.getIops());
            builder.storageThroughput(model.getStorageThroughput());
            builder.storageType(model.getStorageType());
        }

        if (ResourceModelHelper.isOracleCDBEngine(model.getEngine())) {
            builder.engine(null);
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
                .dedicatedLogVolume(model.getDedicatedLogVolume())
                .deletionProtection(model.getDeletionProtection())
                .domain(model.getDomain())
                .domainAuthSecretArn(model.getDomainAuthSecretArn())
                .domainDnsIps(model.getDomainDnsIps())
                .domainFqdn(model.getDomainFqdn())
                .domainIAMRoleName(model.getDomainIAMRoleName())
                .domainOu(model.getDomainOu())
                .enableCloudwatchLogsExports(model.getEnableCloudwatchLogsExports())
                .enableIAMDatabaseAuthentication(model.getEnableIAMDatabaseAuthentication())
                .engine(model.getEngine())
                .engineVersion(model.getEngineVersion())
                .iops(model.getIops())
                .kmsKeyId(model.getKmsKeyId())
                .licenseModel(model.getLicenseModel())
                .manageMasterUserPassword(model.getManageMasterUserPassword())
                .masterUserPassword(model.getMasterUserPassword())
                .masterUserSecretKmsKeyId(model.getMasterUserSecret() == null ? null : model.getMasterUserSecret().getKmsKeyId())
                .masterUsername(model.getMasterUsername())
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
                .processorFeatures(translateProcessorFeaturesToSdk(model.getProcessorFeatures()))
                .promotionTier(model.getPromotionTier())
                .publiclyAccessible(model.getPubliclyAccessible())
                .storageEncrypted(model.getStorageEncrypted())
                .storageThroughput(model.getStorageThroughput())
                .storageType(model.getStorageType())
                .tags(Tagging.translateTagsToSdk(tagSet))
                .tdeCredentialArn(model.getTdeCredentialArn())
                .tdeCredentialPassword(model.getTdeCredentialPassword())
                .timezone(model.getTimezone())
                .vpcSecurityGroupIds(CollectionUtils.isNotEmpty(model.getVPCSecurityGroups()) ? model.getVPCSecurityGroups() : null)
                .engineLifecycleSupport(model.getEngineLifecycleSupport());

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
        final Instant restoreTimeInstant = StringUtils.isNotBlank(model.getRestoreTime()) ? Commons.parseTimestamp(model.getRestoreTime()) : null;

        final RestoreDbInstanceToPointInTimeRequest.Builder builder = RestoreDbInstanceToPointInTimeRequest.builder()
                .autoMinorVersionUpgrade(model.getAutoMinorVersionUpgrade())
                .availabilityZone(model.getAvailabilityZone())
                .copyTagsToSnapshot(model.getCopyTagsToSnapshot())
                .customIamInstanceProfile(model.getCustomIAMInstanceProfile())
                .dbInstanceClass(model.getDBInstanceClass())
                .dbName(model.getDBName())
                .dbParameterGroupName(model.getDBParameterGroupName())
                .dbSubnetGroupName(model.getDBSubnetGroupName())
                .dedicatedLogVolume(model.getDedicatedLogVolume())
                .deletionProtection(model.getDeletionProtection())
                .domain(model.getDomain())
                .domainAuthSecretArn(model.getDomainAuthSecretArn())
                .domainDnsIps(model.getDomainDnsIps())
                .domainFqdn(model.getDomainFqdn())
                .domainIAMRoleName(model.getDomainIAMRoleName())
                .domainOu(model.getDomainOu())
                .enableCloudwatchLogsExports(model.getEnableCloudwatchLogsExports())
                .enableIAMDatabaseAuthentication(model.getEnableIAMDatabaseAuthentication())
                .engine(model.getEngine())
                .licenseModel(model.getLicenseModel())
                .maxAllocatedStorage(model.getMaxAllocatedStorage())
                .multiAZ(model.getMultiAZ())
                .networkType(model.getNetworkType())
                .optionGroupName(model.getOptionGroupName())
                .port(translatePortToSdk(model.getPort()))
                .processorFeatures(translateProcessorFeaturesToSdk(model.getProcessorFeatures()))
                .publiclyAccessible(model.getPubliclyAccessible())
                .restoreTime(restoreTimeInstant)
                .sourceDBInstanceAutomatedBackupsArn(model.getSourceDBInstanceAutomatedBackupsArn())
                .sourceDBInstanceIdentifier(model.getSourceDBInstanceIdentifier())
                .sourceDbiResourceId(model.getSourceDbiResourceId())
                .tags(Tagging.translateTagsToSdk(tagSet))
                .targetDBInstanceIdentifier(model.getDBInstanceIdentifier()) // We only work with DBInstanceId not TargetDBInstanceId
                .tdeCredentialArn(model.getTdeCredentialArn())
                .tdeCredentialPassword(model.getTdeCredentialPassword())
                .useDefaultProcessorFeatures(model.getUseDefaultProcessorFeatures())
                .useLatestRestorableTime(model.getUseLatestRestorableTime())
                .vpcSecurityGroupIds(CollectionUtils.isNotEmpty(model.getVPCSecurityGroups()) ? model.getVPCSecurityGroups() : null)
                .engineLifecycleSupport(model.getEngineLifecycleSupport());
        if (!ResourceModelHelper.isSqlServer(model)) {
            builder.allocatedStorage(getAllocatedStorage(model));
            builder.iops(model.getIops());
            builder.storageThroughput(model.getStorageThroughput());
            builder.storageType(model.getStorageType());
        }

        if (ResourceModelHelper.isOracleCDBEngine(model.getEngine())) {
            builder.engine(null);
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
                .engine(diff(previousModel.getEngine(), desiredModel.getEngine()))
                .engineVersion(diff(previousModel.getEngineVersion(), desiredModel.getEngineVersion()))
                .masterUserPassword(diff(previousModel.getMasterUserPassword(), desiredModel.getMasterUserPassword()))
                .multiAZ(diff(previousModel.getMultiAZ(), desiredModel.getMultiAZ()))
                .optionGroupName(diff(previousModel.getOptionGroupName(), desiredModel.getOptionGroupName()))
                .preferredBackupWindow(diff(previousModel.getPreferredBackupWindow(), desiredModel.getPreferredBackupWindow()))
                .preferredMaintenanceWindow(diff(previousModel.getPreferredMaintenanceWindow(), desiredModel.getPreferredMaintenanceWindow()));

        if (BooleanUtils.isNotTrue(isRollback)) {
            builder.engineVersion(diff(previousModel.getEngineVersion(), desiredModel.getEngineVersion()));
            if (isProvisionedIoStorage(desiredModel)) {
                builder.allocatedStorage(getAllocatedStorage(desiredModel));
                builder.iops(desiredModel.getIops());
            } else {
                builder.allocatedStorage(diff(getAllocatedStorage(previousModel), getAllocatedStorage(desiredModel)));
                builder.iops(diff(previousModel.getIops(), desiredModel.getIops()));
            }
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
                .copyTagsToSnapshot(diff(previousModel.getCopyTagsToSnapshot(), desiredModel.getCopyTagsToSnapshot()))
                .dbInstanceClass(diff(previousModel.getDBInstanceClass(), desiredModel.getDBInstanceClass()))
                .dbInstanceIdentifier(desiredModel.getDBInstanceIdentifier())
                .dbParameterGroupName(diff(previousModel.getDBParameterGroupName(), desiredModel.getDBParameterGroupName()))
                .dbPortNumber(translatePortToSdk(diff(previousModel.getPort(), desiredModel.getPort())))
                .dedicatedLogVolume(diff(previousModel.getDedicatedLogVolume(), desiredModel.getDedicatedLogVolume()))
                .deletionProtection(diff(previousModel.getDeletionProtection(), desiredModel.getDeletionProtection()))
                .domain(diff(previousModel.getDomain(), desiredModel.getDomain()))
                .domainAuthSecretArn(desiredModel.getDomainAuthSecretArn())
                .domainDnsIps(desiredModel.getDomainDnsIps())
                .domainFqdn(desiredModel.getDomainFqdn())
                .domainIAMRoleName(diff(previousModel.getDomainIAMRoleName(), desiredModel.getDomainIAMRoleName()))
                .domainOu(desiredModel.getDomainOu())
                .enableIAMDatabaseAuthentication(diff(previousModel.getEnableIAMDatabaseAuthentication(), desiredModel.getEnableIAMDatabaseAuthentication()))
                .engine(diff(previousModel.getEngine(), desiredModel.getEngine()))
                .licenseModel(diff(previousModel.getLicenseModel(), desiredModel.getLicenseModel()))
                .masterUserPassword(diff(previousModel.getMasterUserPassword(), desiredModel.getMasterUserPassword()))
                .maxAllocatedStorage(diff(previousModel.getMaxAllocatedStorage(), desiredModel.getMaxAllocatedStorage()))
                .monitoringInterval(diff(previousModel.getMonitoringInterval(), desiredModel.getMonitoringInterval()))
                .monitoringRoleArn(diff(previousModel.getMonitoringRoleArn(), desiredModel.getMonitoringRoleArn()))
                .multiAZ(diff(previousModel.getMultiAZ(), desiredModel.getMultiAZ()))
                .networkType(diff(previousModel.getNetworkType(), desiredModel.getNetworkType()))
                .optionGroupName(diff(previousModel.getOptionGroupName(), desiredModel.getOptionGroupName()))
                .preferredBackupWindow(diff(previousModel.getPreferredBackupWindow(), desiredModel.getPreferredBackupWindow()))
                .preferredMaintenanceWindow(diff(previousModel.getPreferredMaintenanceWindow(), desiredModel.getPreferredMaintenanceWindow()))
                .promotionTier(desiredModel.getPromotionTier()) // promotion tier is set unconditionally
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

        if (BooleanUtils.isTrue(isRollback)) {
            if (isProvisionedIoStorage(desiredModel)) {
                builder.allocatedStorage(max(getAllocatedStorage(previousModel), getAllocatedStorage(desiredModel)));
                builder.iops(desiredModel.getIops());
            }
        } else {
            builder.engineVersion(diff(previousModel.getEngineVersion(), desiredModel.getEngineVersion()));
            if (isProvisionedIoStorage(desiredModel)) {
                builder.allocatedStorage(getAllocatedStorage(desiredModel));
                builder.iops(desiredModel.getIops());
            } else {
                builder.allocatedStorage(diff(getAllocatedStorage(previousModel), getAllocatedStorage(desiredModel)));
                builder.iops(diff(previousModel.getIops(), desiredModel.getIops()));
            }
        }

        if (shouldSetProcessorFeatures(previousModel, desiredModel)) {
            builder.processorFeatures(translateProcessorFeaturesToSdk(desiredModel.getProcessorFeatures()));
            builder.useDefaultProcessorFeatures(desiredModel.getUseDefaultProcessorFeatures());
        }

        // Only pass both CACertificateIdentifier and CertificateRotationRestart if the CACertificateIdentifier changes
        // The certificateRotationRestart flag isn't persistent and only changes how the certificate rotation is performed
        // when the CA is changed, we don't want to send both params if only the certificateRotationRestart changes, because
        // it makes no sense to inadvertently restart the instance when CA doesn't change
        final String caCertificateIdentifierDiff = diff(previousModel.getCACertificateIdentifier(), desiredModel.getCACertificateIdentifier());
        if (caCertificateIdentifierDiff != null) {
            builder.caCertificateIdentifier(desiredModel.getCACertificateIdentifier());
            builder.certificateRotationRestart(desiredModel.getCertificateRotationRestart());
        }

        // EnablePerformanceInsights (EPI), PerformanceInsightsKMSKeyId (PKI) and PerformanceInsightsRetentionPeriod (PIP)
        // are coupled parameters. The following logic stands:
        // 0. If EPI or PKI are changed, provide the diff unconditionally.
        // 1. if EPI is true, provide the desired PKI unconditionally.
        // 2. if PKI is changed, provide the desired EPI unconditionally.
        // 3. if PIP is changed, provide the desired EPI unconditionally.
        final Boolean enablePerformanceInsightsDiff = diff(previousModel.getEnablePerformanceInsights(), desiredModel.getEnablePerformanceInsights());
        final String performanceInsightsKMSKeyIdDiff = diff(previousModel.getPerformanceInsightsKMSKeyId(), desiredModel.getPerformanceInsightsKMSKeyId());
        final Integer performanceInsightsRetentionPeriodDiff = diff(previousModel.getPerformanceInsightsRetentionPeriod(), desiredModel.getPerformanceInsightsRetentionPeriod());

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

        if (performanceInsightsRetentionPeriodDiff != null) {
            builder.performanceInsightsRetentionPeriod(performanceInsightsRetentionPeriodDiff);
            builder.enablePerformanceInsights(desiredModel.getEnablePerformanceInsights());
	    builder.performanceInsightsKMSKeyId(desiredModel.getPerformanceInsightsKMSKeyId());
        }

        if (BooleanUtils.isTrue(desiredModel.getManageMasterUserPassword())) {
            builder.manageMasterUserPassword(true);
            if (desiredModel.getMasterUserSecret() != null) {
                builder.masterUserSecretKmsKeyId(desiredModel.getMasterUserSecret().getKmsKeyId());
            }
        } else {
            builder.manageMasterUserPassword(getManageMasterUserPassword(previousModel, desiredModel));
        }

        if(shouldDisableDomain(previousModel, desiredModel)) {
            builder.disableDomain(Boolean.TRUE);
        }
        return builder.build();
    }

    private static Boolean isProvisionedIoStorage(final ResourceModel model) {
        return isIo1Storage(model) || isIo2Storage(model) || isGp3Storage(model);
    }

    private static Boolean isGp3Storage(final ResourceModel model) {
        return StorageType.GP3.toString().equalsIgnoreCase(model.getStorageType());
    }

    private static Boolean isIo1Storage(final ResourceModel model) {
        return StorageType.IO1.toString().equalsIgnoreCase(model.getStorageType()) ||
                (StringUtils.isEmpty(model.getStorageType()) && model.getIops() != null);
    }

    private static Boolean isIo2Storage(final ResourceModel model) {
        return StorageType.IO2.toString().equalsIgnoreCase(model.getStorageType());
    }

    private static Boolean shouldDisableDomain(final ResourceModel previous, final ResourceModel desired) {
        return ObjectUtils.allNull(desired.getDomain(), desired.getDomainAuthSecretArn(), desired.getDomainDnsIps(),
                desired.getDomainFqdn(), desired.getDomainOu(), desired.getDomainIAMRoleName())
                && ObjectUtils.anyNotNull(previous.getDomain(), previous.getDomainAuthSecretArn(), previous.getDomainDnsIps(),
                previous.getDomainFqdn(), previous.getDomainOu(), previous.getDomainIAMRoleName());
    }

    private static Boolean getManageMasterUserPassword(final ResourceModel previous, final ResourceModel desired) {
        if (Objects.equals(previous.getManageMasterUserPassword(), desired.getManageMasterUserPassword())) {
            return null;
        }
        if (null != desired.getManageMasterUserPassword()) {
            return desired.getManageMasterUserPassword();
        }
        if (BooleanUtils.isTrue(previous.getManageMasterUserPassword())) {
            return false;
        }
        return null;
    }

    public static ModifyDbInstanceRequest modifyDbInstanceAfterCreateRequestV12(final ResourceModel model) {
        return ModifyDbInstanceRequest.builder()
                .applyImmediately(Boolean.TRUE)
                .dbInstanceIdentifier(model.getDBInstanceIdentifier())
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
                .allowMajorVersionUpgrade(model.getAllowMajorVersionUpgrade())
                .applyImmediately(Boolean.TRUE)
                .backupRetentionPeriod(model.getBackupRetentionPeriod())
                .caCertificateIdentifier(model.getCACertificateIdentifier())
                .dbInstanceIdentifier(model.getDBInstanceIdentifier())
                .dbParameterGroupName(model.getDBParameterGroupName())
                .deletionProtection(model.getDeletionProtection())
                .enablePerformanceInsights(model.getEnablePerformanceInsights())
                .engineVersion(model.getEngineVersion())
                .manageMasterUserPassword(model.getManageMasterUserPassword())
                .masterUserPassword(model.getMasterUserPassword())
                .masterUserSecretKmsKeyId(model.getMasterUserSecret() != null ? model.getMasterUserSecret().getKmsKeyId() : null)
                .maxAllocatedStorage(model.getMaxAllocatedStorage())
                .monitoringInterval(model.getMonitoringInterval())
                .monitoringRoleArn(model.getMonitoringRoleArn())
                .performanceInsightsRetentionPeriod(model.getPerformanceInsightsRetentionPeriod())
                .performanceInsightsKMSKeyId(model.getPerformanceInsightsKMSKeyId())
                .preferredBackupWindow(model.getPreferredBackupWindow())
                .preferredMaintenanceWindow(model.getPreferredMaintenanceWindow());

        if (ResourceModelHelper.isStorageParametersModified(model) && ResourceModelHelper.isSqlServer(model)) {
            builder.allocatedStorage(getAllocatedStorage(model));
            builder.iops(model.getIops());
            builder.storageThroughput(model.getStorageThroughput());
            builder.storageType(model.getStorageType());
        }

        if (ResourceModelHelper.isOracleCDBEngine(model.getEngine())) {
            builder.engine(model.getEngine());
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

    public static StartDbInstanceAutomatedBackupsReplicationRequest startDbInstanceAutomatedBackupsReplicationRequest(
            final String dbInstanceArn,
            final String kmsKeyId
    ) {
        return StartDbInstanceAutomatedBackupsReplicationRequest.builder()
                .sourceDBInstanceArn(dbInstanceArn)
                .kmsKeyId(kmsKeyId)
                .build();
    }

    public static StopDbInstanceAutomatedBackupsReplicationRequest stopDbInstanceAutomatedBackupsReplicationRequest(
            final String dbInstanceArn
    ) {
        return StopDbInstanceAutomatedBackupsReplicationRequest.builder()
                .sourceDBInstanceArn(dbInstanceArn)
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
        String domainAuthSecretArn = null;
        String domainFqdn = null;
        String domainOu = null;
        List<String> domainDnsIps = null;
        if (CollectionUtils.isNotEmpty(dbInstance.domainMemberships())) {
            DomainMembership domainMembership = dbInstance.domainMemberships().get(0);
            domain = domainMembership.domain();
            domainIAMRoleName = domainMembership.iamRoleName();
            domainAuthSecretArn = domainMembership.authSecretArn();
            domainFqdn = domainMembership.fqdn();
            domainOu = domainMembership.ou();
            domainDnsIps = domainMembership.dnsIps();
        }

        String optionGroupName = null;
        if (CollectionUtils.isNotEmpty(dbInstance.optionGroupMemberships())) {
            optionGroupName = dbInstance.optionGroupMemberships().get(0).optionGroupName();
        }

        String automatedReplicationRegion = null;
        if (dbInstance.hasDbInstanceAutomatedBackupsReplications() && !dbInstance.dbInstanceAutomatedBackupsReplications().isEmpty()) {
            automatedReplicationRegion = Arn.fromString(dbInstance.dbInstanceAutomatedBackupsReplications()
                    .get(0).dbInstanceAutomatedBackupsArn()).getRegion();
        }

        return ResourceModel.builder()
                .allocatedStorage(allocatedStorage)
                .associatedRoles(translateAssociatedRolesFromSdk(dbInstance.associatedRoles()))
                .automaticBackupReplicationRegion(automatedReplicationRegion)
                .autoMinorVersionUpgrade(dbInstance.autoMinorVersionUpgrade())
                .availabilityZone(dbInstance.availabilityZone())
                .backupRetentionPeriod(dbInstance.backupRetentionPeriod())
                .cACertificateIdentifier(dbInstance.caCertificateIdentifier())
                .certificateDetails(translateCertificateDetailsFromSdk(dbInstance.certificateDetails()))
                .characterSetName(dbInstance.characterSetName())
                .copyTagsToSnapshot(dbInstance.copyTagsToSnapshot())
                .customIAMInstanceProfile(dbInstance.customIamInstanceProfile())
                .dBClusterIdentifier(dbInstance.dbClusterIdentifier())
                .dBInstanceArn(dbInstance.dbInstanceArn())
                .dBInstanceClass(dbInstance.dbInstanceClass())
                .dBInstanceIdentifier(dbInstance.dbInstanceIdentifier())
                .dBName(dbInstance.dbName())
                .dBParameterGroupName(dbParameterGroupName)
                .dBSecurityGroups(translateDbSecurityGroupsFromSdk(dbInstance.dbSecurityGroups()))
                .dBSubnetGroupName(translateDbSubnetGroupFromSdk(dbInstance.dbSubnetGroup()))
                .dBSystemId(dbInstance.dbSystemId())
                .dbiResourceId(dbInstance.dbiResourceId())
                .dedicatedLogVolume(dbInstance.dedicatedLogVolume())
                .deletionProtection(dbInstance.deletionProtection())
                .domain(domain)
                .domainAuthSecretArn(domainAuthSecretArn)
                .domainDnsIps(domainDnsIps)
                .domainFqdn(domainFqdn)
                .domainIAMRoleName(domainIAMRoleName)
                .domainOu(domainOu)
                .enableCloudwatchLogsExports(translateEnableCloudwatchLogsExport(dbInstance.enabledCloudwatchLogsExports()))
                .enableIAMDatabaseAuthentication(dbInstance.iamDatabaseAuthenticationEnabled())
                .enablePerformanceInsights(dbInstance.performanceInsightsEnabled())
                .endpoint(endpoint)
                .engine(dbInstance.engine())
                .engineLifecycleSupport(dbInstance.engineLifecycleSupport())
                .engineVersion(dbInstance.engineVersion())
                .iops(dbInstance.iops())
                .kmsKeyId(dbInstance.kmsKeyId())
                .licenseModel(dbInstance.licenseModel())
                .manageMasterUserPassword(dbInstance.masterUserSecret() != null)
                .masterUserSecret(translateMasterUserSecret(dbInstance.masterUserSecret()))
                .masterUsername(dbInstance.masterUsername())
                .maxAllocatedStorage(dbInstance.maxAllocatedStorage())
                .monitoringInterval(dbInstance.monitoringInterval())
                .monitoringRoleArn(dbInstance.monitoringRoleArn())
                .multiAZ(dbInstance.multiAZ())
                .ncharCharacterSetName(dbInstance.ncharCharacterSetName())
                .networkType(dbInstance.networkType())
                .optionGroupName(optionGroupName)
                .performanceInsightsKMSKeyId(dbInstance.performanceInsightsKMSKeyId())
                .performanceInsightsRetentionPeriod(dbInstance.performanceInsightsRetentionPeriod())
                .port(port == null ? null : port.toString())
                .preferredBackupWindow(dbInstance.preferredBackupWindow())
                .preferredMaintenanceWindow(dbInstance.preferredMaintenanceWindow())
                .processorFeatures(translateProcessorFeaturesFromSdk(dbInstance.processorFeatures()))
                .promotionTier(dbInstance.promotionTier())
                .publiclyAccessible(dbInstance.publiclyAccessible())
                .sourceDBClusterIdentifier(dbInstance.readReplicaSourceDBClusterIdentifier())
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
                .validTill(certificateDetails.validTill() == null ? null : certificateDetails.validTill().toString())
                .build();
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
            return MasterUserSecret.builder().build();
        }

        return MasterUserSecret.builder()
                .secretArn(sdkSecret.secretArn())
                .kmsKeyId(sdkSecret.kmsKeyId())
                .build();
    }

    private static Integer max(Integer a, Integer b) {
        return a == null ? b : b == null ? a : Math.max(a, b);
    }
}
