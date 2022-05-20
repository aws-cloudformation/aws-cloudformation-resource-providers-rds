package software.amazon.rds.dbinstance;

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

import org.apache.commons.lang3.BooleanUtils;

import software.amazon.awssdk.services.ec2.model.DescribeSecurityGroupsRequest;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.rds.model.AddRoleToDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.CloudwatchLogsExportConfiguration;
import software.amazon.awssdk.services.rds.model.CreateDbInstanceReadReplicaRequest;
import software.amazon.awssdk.services.rds.model.CreateDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.DBParameterGroupStatus;
import software.amazon.awssdk.services.rds.model.DBSubnetGroup;
import software.amazon.awssdk.services.rds.model.DeleteDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbEngineVersionsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbParameterGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.RebootDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.RemoveRoleFromDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.RestoreDbInstanceFromDbSnapshotRequest;
import software.amazon.awssdk.utils.StringUtils;
import software.amazon.rds.common.handler.Tagging;

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
                .dbInstanceClass(model.getDBInstanceClass())
                .dbInstanceIdentifier(model.getDBInstanceIdentifier())
                .dbParameterGroupName(model.getDBParameterGroupName())
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
                .optionGroupName(model.getOptionGroupName())
                .performanceInsightsKMSKeyId(model.getPerformanceInsightsKMSKeyId())
                .performanceInsightsRetentionPeriod(model.getPerformanceInsightsRetentionPeriod())
                .port(translatePortToSdk(model.getPort()))
                .processorFeatures(translateProcessorFeaturesToSdk(model.getProcessorFeatures()))
                .publiclyAccessible(model.getPubliclyAccessible())
                .sourceDBInstanceIdentifier(model.getSourceDBInstanceIdentifier())
                .sourceRegion(model.getSourceRegion())
                .storageType(model.getStorageType())
                .tags(Tagging.translateTagsToSdk(tagSet))
                .useDefaultProcessorFeatures(model.getUseDefaultProcessorFeatures())
                .vpcSecurityGroupIds(model.getVPCSecurityGroups())
                .build();
    }

    public static RestoreDbInstanceFromDbSnapshotRequest restoreDbInstanceFromSnapshotRequestV12(
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

    public static RestoreDbInstanceFromDbSnapshotRequest restoreDbInstanceFromSnapshotRequest(
            final ResourceModel model,
            final Tagging.TagSet tagSet
    ) {
        return RestoreDbInstanceFromDbSnapshotRequest.builder()
                .autoMinorVersionUpgrade(model.getAutoMinorVersionUpgrade())
                .availabilityZone(model.getAvailabilityZone())
                .dbInstanceClass(model.getDBInstanceClass())
                .dbInstanceIdentifier(model.getDBInstanceIdentifier())
                .dbName(model.getDBName())
                .dbParameterGroupName(model.getDBParameterGroupName())
                .dbSnapshotIdentifier(model.getDBSnapshotIdentifier())
                .dbSubnetGroupName(model.getDBSubnetGroupName())
                .deletionProtection(model.getDeletionProtection())
                .domain(model.getDomain())
                .domainIAMRoleName(model.getDomainIAMRoleName())
                .enableCloudwatchLogsExports(model.getEnableCloudwatchLogsExports())
                .enableIAMDatabaseAuthentication(model.getEnableIAMDatabaseAuthentication())
                .engine(model.getEngine())
                .iops(model.getIops())
                .licenseModel(model.getLicenseModel())
                .multiAZ(model.getMultiAZ())
                .optionGroupName(model.getOptionGroupName())
                .port(translatePortToSdk(model.getPort()))
                .processorFeatures(translateProcessorFeaturesToSdk(model.getProcessorFeatures()))
                .publiclyAccessible(model.getPubliclyAccessible())
                .storageType(model.getStorageType())
                .tags(Tagging.translateTagsToSdk(tagSet))
                .tdeCredentialArn(model.getTdeCredentialArn())
                .tdeCredentialPassword(model.getTdeCredentialPassword())
                .useDefaultProcessorFeatures(model.getUseDefaultProcessorFeatures())
                .vpcSecurityGroupIds(model.getVPCSecurityGroups())
                .build();
    }

    public static CreateDbInstanceRequest createDbInstanceRequestV12(
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

    public static CreateDbInstanceRequest createDbInstanceRequest(
            final ResourceModel model,
            final Tagging.TagSet tagSet
    ) {
        return CreateDbInstanceRequest.builder()
                .allocatedStorage(getAllocatedStorage(model))
                .autoMinorVersionUpgrade(model.getAutoMinorVersionUpgrade())
                .availabilityZone(model.getAvailabilityZone())
                .backupRetentionPeriod(model.getBackupRetentionPeriod())
                .characterSetName(model.getCharacterSetName())
                .copyTagsToSnapshot(model.getCopyTagsToSnapshot())
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
                .enablePerformanceInsights(model.getEnablePerformanceInsights())
                .engine(model.getEngine())
                .engineVersion(model.getEngineVersion())
                .iops(model.getIops())
                .kmsKeyId(model.getKmsKeyId())
                .licenseModel(model.getLicenseModel())
                .masterUsername(model.getMasterUsername())
                .masterUserPassword(model.getMasterUserPassword())
                .maxAllocatedStorage(model.getMaxAllocatedStorage())
                .monitoringInterval(model.getMonitoringInterval())
                .monitoringRoleArn(model.getMonitoringRoleArn())
                .multiAZ(model.getMultiAZ())
                .optionGroupName(model.getOptionGroupName())
                .performanceInsightsKMSKeyId(model.getPerformanceInsightsKMSKeyId())
                .performanceInsightsRetentionPeriod(model.getPerformanceInsightsRetentionPeriod())
                .port(translatePortToSdk(model.getPort()))
                .preferredBackupWindow(model.getPreferredBackupWindow())
                .preferredMaintenanceWindow(model.getPreferredMaintenanceWindow())
                .promotionTier(model.getPromotionTier())
                .processorFeatures(translateProcessorFeaturesToSdk(model.getProcessorFeatures()))
                .publiclyAccessible(model.getPubliclyAccessible())
                .storageEncrypted(model.getStorageEncrypted())
                .storageType(model.getStorageType())
                .tags(Tagging.translateTagsToSdk(tagSet))
                .tdeCredentialArn(model.getTdeCredentialArn())
                .tdeCredentialPassword(model.getTdeCredentialPassword())
                .timezone(model.getTimezone())
                .vpcSecurityGroupIds(model.getVPCSecurityGroups())
                .build();
    }

    public static ModifyDbInstanceRequest modifyDbInstanceRequestV12(
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

    public static ModifyDbInstanceRequest modifyDbInstanceRequest(
            final ResourceModel previousModel,
            final ResourceModel desiredModel,
            final Boolean isRollback
    ) {
        ModifyDbInstanceRequest.Builder builder = ModifyDbInstanceRequest.builder()
                .allowMajorVersionUpgrade(desiredModel.getAllowMajorVersionUpgrade())
                .applyImmediately(Boolean.TRUE)
                .autoMinorVersionUpgrade(desiredModel.getAutoMinorVersionUpgrade())
                .backupRetentionPeriod(desiredModel.getBackupRetentionPeriod())
                .caCertificateIdentifier(desiredModel.getCACertificateIdentifier())
                .copyTagsToSnapshot(desiredModel.getCopyTagsToSnapshot())
                .dbInstanceClass(desiredModel.getDBInstanceClass())
                .dbInstanceIdentifier(desiredModel.getDBInstanceIdentifier())
                .dbParameterGroupName(desiredModel.getDBParameterGroupName())
                .dbPortNumber(translatePortToSdk(desiredModel.getPort()))
                .deletionProtection(desiredModel.getDeletionProtection())
                .domain(desiredModel.getDomain())
                .domainIAMRoleName(desiredModel.getDomainIAMRoleName())
                .enableIAMDatabaseAuthentication(desiredModel.getEnableIAMDatabaseAuthentication())
                .enablePerformanceInsights(desiredModel.getEnablePerformanceInsights())
                .engineVersion(desiredModel.getEngineVersion())
                .licenseModel(desiredModel.getLicenseModel())
                .masterUserPassword(desiredModel.getMasterUserPassword())
                .maxAllocatedStorage(desiredModel.getMaxAllocatedStorage())
                .monitoringInterval(desiredModel.getMonitoringInterval())
                .monitoringRoleArn(desiredModel.getMonitoringRoleArn())
                .multiAZ(desiredModel.getMultiAZ())
                .optionGroupName(desiredModel.getOptionGroupName())
                .performanceInsightsKMSKeyId(desiredModel.getPerformanceInsightsKMSKeyId())
                .performanceInsightsRetentionPeriod(desiredModel.getPerformanceInsightsRetentionPeriod())
                .preferredBackupWindow(desiredModel.getPreferredBackupWindow())
                .preferredMaintenanceWindow(desiredModel.getPreferredMaintenanceWindow())
                .promotionTier(desiredModel.getPromotionTier())
                .storageType(desiredModel.getStorageType())
                .tdeCredentialArn(desiredModel.getTdeCredentialArn())
                .tdeCredentialPassword(desiredModel.getTdeCredentialPassword())
                .vpcSecurityGroupIds(desiredModel.getVPCSecurityGroups());

        final CloudwatchLogsExportConfiguration cloudwatchLogsExportConfiguration = buildTranslateCloudwatchLogsExportConfiguration(
                Optional.ofNullable(previousModel).map(ResourceModel::getEnableCloudwatchLogsExports).orElse(Collections.emptyList()),
                desiredModel.getEnableCloudwatchLogsExports()
        );
        builder.cloudwatchLogsExportConfiguration(cloudwatchLogsExportConfiguration);

        if (previousModel != null) {
            if (BooleanUtils.isTrue(isRollback)) {
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
        }

        if (shouldSetProcessorFeatures(previousModel, desiredModel)) {
            builder.processorFeatures(translateProcessorFeaturesToSdk(desiredModel.getProcessorFeatures()));
            builder.useDefaultProcessorFeatures(desiredModel.getUseDefaultProcessorFeatures());
        }

        return builder.build();
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
                .dbInstanceIdentifier(model.getDBInstanceIdentifier());
        if (StringUtils.isEmpty(finalDBSnapshotIdentifier)) {
            builder.skipFinalSnapshot(true);
        } else {
            builder.skipFinalSnapshot(false)
                    .finalDBSnapshotIdentifier(finalDBSnapshotIdentifier);
        }
        return builder.build();
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

        return ResourceModel.builder()
                .allocatedStorage(allocatedStorage)
                .associatedRoles(translateAssociatedRolesFromSdk(dbInstance.associatedRoles()))
                .autoMinorVersionUpgrade(dbInstance.autoMinorVersionUpgrade())
                .availabilityZone(dbInstance.availabilityZone())
                .backupRetentionPeriod(dbInstance.backupRetentionPeriod())
                .cACertificateIdentifier(dbInstance.caCertificateIdentifier())
                .characterSetName(dbInstance.characterSetName())
                .copyTagsToSnapshot(dbInstance.copyTagsToSnapshot())
                .dBClusterIdentifier(dbInstance.dbClusterIdentifier())
                .dBInstanceClass(dbInstance.dbInstanceClass())
                .dBInstanceIdentifier(dbInstance.dbInstanceIdentifier())
                .dBName(dbInstance.dbName())
                .dBParameterGroupName(dbParameterGroupName)
                .dBSecurityGroups(translateDbSecurityGroupsFromSdk(dbInstance.dbSecurityGroups()))
                .dBSubnetGroupName(translateDbSubnetGroupFromSdk(dbInstance.dbSubnetGroup()))
                .deletionProtection(dbInstance.deletionProtection())
                .enableCloudwatchLogsExports(translateEnableCloudwatchLogsExport(dbInstance.enabledCloudwatchLogsExports()))
                .enableIAMDatabaseAuthentication(dbInstance.iamDatabaseAuthenticationEnabled())
                .enablePerformanceInsights(dbInstance.performanceInsightsEnabled())
                .endpoint(endpoint)
                .engine(dbInstance.engine())
                .engineVersion(dbInstance.engineVersion())
                .iops(dbInstance.iops())
                .kmsKeyId(dbInstance.kmsKeyId())
                .licenseModel(dbInstance.licenseModel())
                .masterUsername(dbInstance.masterUsername())
                .maxAllocatedStorage(dbInstance.maxAllocatedStorage())
                .monitoringInterval(dbInstance.monitoringInterval())
                .monitoringRoleArn(dbInstance.monitoringRoleArn())
                .multiAZ(dbInstance.multiAZ())
                .performanceInsightsKMSKeyId(dbInstance.performanceInsightsKMSKeyId())
                .performanceInsightsRetentionPeriod(dbInstance.performanceInsightsRetentionPeriod())
                .port(port == null ? null : port.toString())
                .preferredBackupWindow(dbInstance.preferredBackupWindow())
                .preferredMaintenanceWindow(dbInstance.preferredMaintenanceWindow())
                .processorFeatures(translateProcessorFeaturesFromSdk(dbInstance.processorFeatures()))
                .promotionTier(dbInstance.promotionTier())
                .publiclyAccessible(dbInstance.publiclyAccessible())
                .sourceDBInstanceIdentifier(dbInstance.readReplicaSourceDBInstanceIdentifier())
                .storageEncrypted(dbInstance.storageEncrypted())
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
        return Optional.ofNullable(parameterGroup).map(DBParameterGroupStatus::dbParameterGroupName).orElse(null);
    }

    public static List<String> translateEnableCloudwatchLogsExport(final Collection<String> enabledCloudwatchLogsExports) {
        return new ArrayList<>(Optional.ofNullable(enabledCloudwatchLogsExports)
                .orElse(Collections.emptyList()));
    }

    public static List<String> translateVpcSecurityGroupsFromSdk(
            final Collection<software.amazon.awssdk.services.rds.model.VpcSecurityGroupMembership> vpcSecurityGroups
    ) {
        return streamOfOrEmpty(vpcSecurityGroups)
                .map(software.amazon.awssdk.services.rds.model.VpcSecurityGroupMembership::vpcSecurityGroupId)
                .collect(Collectors.toList());
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
        return streamOfOrEmpty(sdkProcessorFeatures)
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
        return streamOfOrEmpty(processorFeatures)
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
        return streamOfOrEmpty(dbSecurityGroupMemberships)
                .map(software.amazon.awssdk.services.rds.model.DBSecurityGroupMembership::dbSecurityGroupName)
                .collect(Collectors.toList());
    }

    public static String translateDbSubnetGroupFromSdk(
            final software.amazon.awssdk.services.rds.model.DBSubnetGroup dbSubnetGroup
    ) {
        return Optional.ofNullable(dbSubnetGroup).map(DBSubnetGroup::dbSubnetGroupName).orElse(null);
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

    private static boolean canUpdateAllocatedStorage(final String fromAllocatedStorage, final String toAllocatedStorage) {
        if (fromAllocatedStorage == null || toAllocatedStorage == null) {
            return true;
        }
        final int from, to;
        try {
            from = Integer.parseInt(fromAllocatedStorage);
            to = Integer.parseInt(toAllocatedStorage);
        } catch (NumberFormatException e) {
            return true;
        }
        return to >= from;
    }

    private static boolean canUpdateIops(final Integer fromIops, final Integer toIops) {
        return fromIops == null || toIops == null || toIops >= fromIops;
    }

    private static boolean shouldSetProcessorFeatures(final ResourceModel previousModel, final ResourceModel desiredModel) {
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
}
