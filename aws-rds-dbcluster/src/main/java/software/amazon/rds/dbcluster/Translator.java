package software.amazon.rds.dbcluster;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;

import com.amazonaws.util.StringUtils;
import com.google.common.collect.Sets;
import software.amazon.awssdk.services.ec2.model.DescribeSecurityGroupsRequest;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.rds.model.AddRoleToDbClusterRequest;
import software.amazon.awssdk.services.rds.model.CloudwatchLogsExportConfiguration;
import software.amazon.awssdk.services.rds.model.CreateDbClusterRequest;
import software.amazon.awssdk.services.rds.model.DeleteDbClusterRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbSubnetGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeGlobalClustersRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbClusterRequest;
import software.amazon.awssdk.services.rds.model.RebootDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.RemoveFromGlobalClusterRequest;
import software.amazon.awssdk.services.rds.model.RemoveRoleFromDbClusterRequest;
import software.amazon.awssdk.services.rds.model.RestoreDbClusterFromSnapshotRequest;
import software.amazon.awssdk.services.rds.model.RestoreDbClusterToPointInTimeRequest;
import software.amazon.awssdk.services.rds.model.VpcSecurityGroupMembership;
import software.amazon.rds.common.handler.Tagging;

public class Translator {
    static CreateDbClusterRequest createDbClusterRequest(
            final ResourceModel model,
            final Tagging.TagSet tagSet
    ) {
        return CreateDbClusterRequest.builder()
                .allocatedStorage(model.getAllocatedStorage())
                .autoMinorVersionUpgrade(model.getAutoMinorVersionUpgrade())
                .availabilityZones(model.getAvailabilityZones())
                .backtrackWindow(castToLong(model.getBacktrackWindow()))
                .backupRetentionPeriod(model.getBackupRetentionPeriod())
                .copyTagsToSnapshot(model.getCopyTagsToSnapshot())
                .databaseName(model.getDatabaseName())
                .dbClusterIdentifier(model.getDBClusterIdentifier())
                .dbClusterInstanceClass(model.getDBClusterInstanceClass())
                .dbClusterParameterGroupName(model.getDBClusterParameterGroupName())
                .dbSubnetGroupName(model.getDBSubnetGroupName())
                .deletionProtection(model.getDeletionProtection())
                .domain(model.getDomain())
                .domainIAMRoleName(model.getDomainIAMRoleName())
                .enableCloudwatchLogsExports(model.getEnableCloudwatchLogsExports())
                .enableHttpEndpoint(model.getEnableHttpEndpoint())
                .enableIAMDatabaseAuthentication(model.getEnableIAMDatabaseAuthentication())
                .enablePerformanceInsights(model.getPerformanceInsightsEnabled())
                .engine(model.getEngine())
                .engineMode(model.getEngineMode())
                .engineVersion(model.getEngineVersion())
                .globalClusterIdentifier(model.getGlobalClusterIdentifier())
                .iops(model.getIops())
                .kmsKeyId(model.getKmsKeyId())
                .masterUserPassword(model.getMasterUserPassword())
                .masterUsername(model.getMasterUsername())
                .monitoringInterval(model.getMonitoringInterval())
                .monitoringRoleArn(model.getMonitoringRoleArn())
                .networkType(model.getNetworkType())
                .performanceInsightsKMSKeyId(model.getPerformanceInsightsKmsKeyId())
                .performanceInsightsRetentionPeriod(model.getPerformanceInsightsRetentionPeriod())
                .port(model.getPort())
                .preferredBackupWindow(model.getPreferredBackupWindow())
                .preferredMaintenanceWindow(model.getPreferredMaintenanceWindow())
                .publiclyAccessible(model.getPubliclyAccessible())
                .replicationSourceIdentifier(model.getReplicationSourceIdentifier())
                .serverlessV2ScalingConfiguration(translateServerlessV2ScalingConfiguration(model.getServerlessV2ScalingConfiguration()))
                .scalingConfiguration(translateScalingConfigurationToSdk(model.getScalingConfiguration()))
                .sourceRegion(model.getSourceRegion())
                .storageEncrypted(model.getStorageEncrypted())
                .storageType(model.getStorageType())
                .tags(Tagging.translateTagsToSdk(tagSet))
                .vpcSecurityGroupIds(model.getVpcSecurityGroupIds())
                .build();
    }

    static RestoreDbClusterToPointInTimeRequest restoreDbClusterToPointInTimeRequest(
            final ResourceModel model,
            final Tagging.TagSet tagSet
    ) {
        return RestoreDbClusterToPointInTimeRequest.builder()
                .copyTagsToSnapshot(model.getCopyTagsToSnapshot())
                .dbClusterIdentifier(model.getDBClusterIdentifier())
                .dbClusterInstanceClass(model.getDBClusterInstanceClass())
                .dbSubnetGroupName(model.getDBSubnetGroupName())
                .domain(model.getDomain())
                .domainIAMRoleName(model.getDomainIAMRoleName())
                .iops(model.getIops())
                .kmsKeyId(model.getKmsKeyId())
                .networkType(model.getNetworkType())
                .publiclyAccessible(model.getPubliclyAccessible())
                .restoreType(model.getRestoreType())
                .scalingConfiguration(translateScalingConfigurationToSdk(model.getScalingConfiguration()))
                .serverlessV2ScalingConfiguration(translateServerlessV2ScalingConfiguration(model.getServerlessV2ScalingConfiguration()))
                .sourceDBClusterIdentifier(model.getSourceDBClusterIdentifier())
                .storageType(model.getStorageType())
                .tags(Tagging.translateTagsToSdk(tagSet))
                .useLatestRestorableTime(model.getUseLatestRestorableTime())
                .vpcSecurityGroupIds(model.getVpcSecurityGroupIds())
                .build();
    }

    static RestoreDbClusterFromSnapshotRequest restoreDbClusterFromSnapshotRequest(
            final ResourceModel model,
            final Tagging.TagSet tagSet
    ) {
        return RestoreDbClusterFromSnapshotRequest.builder()
                .availabilityZones(model.getAvailabilityZones())
                .backtrackWindow(castToLong(model.getBacktrackWindow()))
                .copyTagsToSnapshot(model.getCopyTagsToSnapshot())
                .databaseName(model.getDatabaseName())
                .dbClusterIdentifier(model.getDBClusterIdentifier())
                .dbClusterInstanceClass(model.getDBClusterInstanceClass())
                .dbClusterParameterGroupName(model.getDBClusterParameterGroupName())
                .dbSubnetGroupName(model.getDBSubnetGroupName())
                .domain(model.getDomain())
                .domainIAMRoleName(model.getDomainIAMRoleName())
                .deletionProtection(model.getDeletionProtection())
                .enableCloudwatchLogsExports(model.getEnableCloudwatchLogsExports())
                .enableIAMDatabaseAuthentication(model.getEnableIAMDatabaseAuthentication())
                .engine(model.getEngine())
                .engineMode(model.getEngineMode())
                .engineVersion(model.getEngineVersion())
                .iops(model.getIops())
                .kmsKeyId(model.getKmsKeyId())
                .networkType(model.getNetworkType())
                .port(model.getPort())
                .publiclyAccessible(model.getPubliclyAccessible())
                .scalingConfiguration(translateScalingConfigurationToSdk(model.getScalingConfiguration()))
                .serverlessV2ScalingConfiguration(translateServerlessV2ScalingConfiguration(model.getServerlessV2ScalingConfiguration()))
                .snapshotIdentifier(model.getSnapshotIdentifier())
                .storageType(model.getStorageType())
                .tags(Tagging.translateTagsToSdk(tagSet))
                .vpcSecurityGroupIds(model.getVpcSecurityGroupIds())
                .build();
    }

    static Long castToLong(Object object) {
        return object == null ? null : Long.parseLong(String.valueOf(object));
    }

    static Integer castToInt(Object object) {
        return object == null ? null : Integer.parseInt(String.valueOf(object));
    }

    static AddRoleToDbClusterRequest addRoleToDbClusterRequest(
            final String dbClusterIdentifier,
            final String roleArn,
            final String featureName
    ) {
        return AddRoleToDbClusterRequest.builder()
                .dbClusterIdentifier(dbClusterIdentifier)
                .roleArn(roleArn)
                .featureName(featureName)
                .build();
    }

    static RemoveRoleFromDbClusterRequest removeRoleFromDbClusterRequest(
            final String dbClusterIdentifier,
            final String roleArn,
            final String featureName
    ) {
        return RemoveRoleFromDbClusterRequest.builder()
                .dbClusterIdentifier(dbClusterIdentifier)
                .roleArn(roleArn)
                .featureName(featureName)
                .build();
    }

    static ModifyDbClusterRequest modifyDbClusterRequest(final ResourceModel model) {
        return modifyDbClusterRequest(null, model, false);
    }

    static ModifyDbClusterRequest modifyDbClusterRequest(
            final ResourceModel previousModel,
            final ResourceModel desiredModel,
            final boolean isRollback
    ) {

        final CloudwatchLogsExportConfiguration config = cloudwatchLogsExportConfiguration(previousModel, desiredModel);

        ModifyDbClusterRequest.Builder builder = ModifyDbClusterRequest.builder()
                .allocatedStorage(desiredModel.getAllocatedStorage())
                .applyImmediately(Boolean.TRUE)
                .autoMinorVersionUpgrade(desiredModel.getAutoMinorVersionUpgrade())
                .backtrackWindow(castToLong(desiredModel.getBacktrackWindow()))
                .backupRetentionPeriod(desiredModel.getBackupRetentionPeriod())
                .cloudwatchLogsExportConfiguration(config)
                .copyTagsToSnapshot(desiredModel.getCopyTagsToSnapshot())
                .dbClusterIdentifier(desiredModel.getDBClusterIdentifier())
                .dbClusterInstanceClass(desiredModel.getDBClusterInstanceClass())
                .dbClusterParameterGroupName(desiredModel.getDBClusterParameterGroupName())
                .deletionProtection(desiredModel.getDeletionProtection())
                .domain(desiredModel.getDomain())
                .domainIAMRoleName(desiredModel.getDomainIAMRoleName())
                .enableHttpEndpoint(desiredModel.getEnableHttpEndpoint())
                .enablePerformanceInsights(desiredModel.getPerformanceInsightsEnabled())
                .iops(desiredModel.getIops())
                .monitoringInterval(desiredModel.getMonitoringInterval())
                .monitoringRoleArn(desiredModel.getMonitoringRoleArn())
                .networkType(desiredModel.getNetworkType())
                .performanceInsightsKMSKeyId(desiredModel.getPerformanceInsightsKmsKeyId())
                .performanceInsightsRetentionPeriod(desiredModel.getPerformanceInsightsRetentionPeriod())
                .port(desiredModel.getPort())
                .scalingConfiguration(translateScalingConfigurationToSdk(desiredModel.getScalingConfiguration()))
                .serverlessV2ScalingConfiguration(translateServerlessV2ScalingConfiguration(desiredModel.getServerlessV2ScalingConfiguration()))
                .storageType(desiredModel.getStorageType());

        if (previousModel != null) {
            if (!Objects.equals(previousModel.getMasterUserPassword(), desiredModel.getMasterUserPassword())) {
                builder.masterUserPassword(desiredModel.getMasterUserPassword());
            }
            if (!(isRollback || Objects.equals(previousModel.getEngineVersion(), desiredModel.getEngineVersion()))) {
                builder.engineVersion(desiredModel.getEngineVersion());
                builder.allowMajorVersionUpgrade(true);
                if (!Objects.equals(previousModel.getDBInstanceParameterGroupName(), desiredModel.getDBInstanceParameterGroupName())) {
                    builder.dbInstanceParameterGroupName(desiredModel.getDBInstanceParameterGroupName());
                }
            }
            if (!Objects.equals(previousModel.getPreferredBackupWindow(), desiredModel.getPreferredBackupWindow())) {
                builder.preferredBackupWindow(desiredModel.getPreferredBackupWindow());
            }
            if (!Objects.equals(previousModel.getPreferredMaintenanceWindow(), desiredModel.getPreferredMaintenanceWindow())) {
                builder.preferredMaintenanceWindow(desiredModel.getPreferredMaintenanceWindow());
            }
            if (!Objects.equals(previousModel.getEnableIAMDatabaseAuthentication(), desiredModel.getEnableIAMDatabaseAuthentication())) {
                builder.enableIAMDatabaseAuthentication(desiredModel.getEnableIAMDatabaseAuthentication());
            }
        }
        //only include VPC SG ids if they are changed from previous.
        final Set<String> desiredVpcSgIds = streamOfOrEmpty(desiredModel.getVpcSecurityGroupIds()).collect(Collectors.toSet());
        if (!desiredVpcSgIds.isEmpty()) {
            if (previousModel != null) {
                final Set<String> previousVpcSgIds = streamOfOrEmpty(previousModel.getVpcSecurityGroupIds()).collect(Collectors.toSet());
                if (!desiredVpcSgIds.equals(previousVpcSgIds)) {
                    builder.vpcSecurityGroupIds(desiredModel.getVpcSecurityGroupIds());
                }
            } else {
                builder.vpcSecurityGroupIds(desiredModel.getVpcSecurityGroupIds());
            }
        }

        return builder.build();
    }

    static CloudwatchLogsExportConfiguration cloudwatchLogsExportConfiguration(
            final ResourceModel previousModel,
            final ResourceModel desiredModel
    ) {
        CloudwatchLogsExportConfiguration.Builder config = CloudwatchLogsExportConfiguration.builder();

        final List<String> currentLogsExports = desiredModel.getEnableCloudwatchLogsExports();
        final List<String> previousLogsExports = previousModel == null ? Collections.emptyList() : previousModel.getEnableCloudwatchLogsExports();

        final Set<String> existingLogs = new HashSet<>(Optional.ofNullable(previousLogsExports).orElse(Collections.emptyList()));
        final Set<String> newLogsExports = new HashSet<>(Optional.ofNullable(currentLogsExports).orElse(Collections.emptyList()));

        final Set<String> logTypesToEnable = Sets.difference(newLogsExports, existingLogs);
        final Set<String> logTypesToDisable = Sets.difference(existingLogs, newLogsExports);

        return config.enableLogTypes(logTypesToEnable).disableLogTypes(logTypesToDisable).build();
    }

    static DeleteDbClusterRequest deleteDbClusterRequest(
            final ResourceModel model,
            final String finalDBSnapshotIdentifier
    ) {
        final DeleteDbClusterRequest.Builder builder = DeleteDbClusterRequest.builder()
                .dbClusterIdentifier(model.getDBClusterIdentifier());
        if (StringUtils.isNullOrEmpty(finalDBSnapshotIdentifier)) {
            builder.skipFinalSnapshot(true);
        } else {
            builder.skipFinalSnapshot(false)
                    .finalDBSnapshotIdentifier(finalDBSnapshotIdentifier);
        }
        return builder.build();
    }

    static RemoveFromGlobalClusterRequest removeFromGlobalClusterRequest(final String globalClusterIdentifier,
                                                                         final String clusterArn) {
        return RemoveFromGlobalClusterRequest.builder()
                .dbClusterIdentifier(clusterArn)
                .globalClusterIdentifier(globalClusterIdentifier)
                .build();
    }

    static DescribeDbClustersRequest describeDbClustersRequest(
            final ResourceModel model
    ) {
        return DescribeDbClustersRequest.builder()
                .dbClusterIdentifier(model.getDBClusterIdentifier())
                .build();
    }

    static DescribeDbClustersRequest describeDbClustersRequest(
            final String nextToken
    ) {
        return DescribeDbClustersRequest.builder()
                .marker(nextToken)
                .build();
    }

    static Set<software.amazon.awssdk.services.rds.model.Tag> translateTagsToSdk(
            final Collection<Tag> tags
    ) {
        return Optional.ofNullable(tags).orElse(Collections.emptySet())
                .stream()
                .map(tag -> software.amazon.awssdk.services.rds.model.Tag.builder()
                        .key(tag.getKey())
                        .value(tag.getValue())
                        .build())
                .collect(Collectors.toSet());
    }

    static Set<Tag> translateTagsFromSdk(
            final Collection<software.amazon.awssdk.services.rds.model.Tag> tags
    ) {
        return Optional.ofNullable(tags).orElse(Collections.emptySet())
                .stream()
                .map(tag -> software.amazon.rds.dbcluster.Tag.builder()
                        .key(tag.key())
                        .value(tag.value())
                        .build())
                .collect(Collectors.toSet());
    }

    static software.amazon.awssdk.services.rds.model.ServerlessV2ScalingConfiguration translateServerlessV2ScalingConfiguration(
            final ServerlessV2ScalingConfiguration serverlessV2ScalingConfiguration
    ) {
        if (serverlessV2ScalingConfiguration == null) {
            return null;
        }
        return software.amazon.awssdk.services.rds.model.ServerlessV2ScalingConfiguration.builder()
                .maxCapacity(serverlessV2ScalingConfiguration.getMaxCapacity())
                .minCapacity(serverlessV2ScalingConfiguration.getMinCapacity())
                .build();
    }

    static software.amazon.awssdk.services.rds.model.ScalingConfiguration translateScalingConfigurationToSdk(
            final ScalingConfiguration scalingConfiguration
    ) {
        if (scalingConfiguration == null) {
            return null;
        }
        return software.amazon.awssdk.services.rds.model.ScalingConfiguration.builder()
                .autoPause(scalingConfiguration.getAutoPause())
                .maxCapacity(scalingConfiguration.getMaxCapacity())
                .minCapacity(scalingConfiguration.getMinCapacity())
                .timeoutAction(scalingConfiguration.getTimeoutAction())
                .secondsUntilAutoPause(scalingConfiguration.getSecondsUntilAutoPause())
                .build();
    }

    static ServerlessV2ScalingConfiguration translateServerlessV2ScalingConfigurationFromSdk(
            final software.amazon.awssdk.services.rds.model.ServerlessV2ScalingConfigurationInfo serverlessV2ScalingConfiguration
    ) {
        if (serverlessV2ScalingConfiguration == null) {
            return null;
        }
        return ServerlessV2ScalingConfiguration.builder()
                .maxCapacity(serverlessV2ScalingConfiguration.maxCapacity())
                .minCapacity(serverlessV2ScalingConfiguration.minCapacity())
                .build();
    }

    static ScalingConfiguration translateScalingConfigurationFromSdk(
            final software.amazon.awssdk.services.rds.model.ScalingConfigurationInfo scalingConfiguration
    ) {
        if (scalingConfiguration == null) {
            return null;
        }
        return ScalingConfiguration.builder()
                .autoPause(scalingConfiguration.autoPause())
                .maxCapacity(scalingConfiguration.maxCapacity())
                .minCapacity(scalingConfiguration.minCapacity())
                .timeoutAction(scalingConfiguration.timeoutAction())
                .secondsUntilAutoPause(scalingConfiguration.secondsUntilAutoPause())
                .build();
    }

    public static DBClusterRole transformDBCusterRoleFromSdk(
            final software.amazon.awssdk.services.rds.model.DBClusterRole dbClusterRole
    ) {
        return DBClusterRole.builder()
                .featureName(dbClusterRole.featureName())
                .roleArn(dbClusterRole.roleArn())
                .build();
    }

    public static Map<String, String> translateTagsToRequest(final Collection<Tag> tags) {
        return Optional.ofNullable(tags).orElse(Collections.emptyList())
                .stream()
                .collect(Collectors.toMap(Tag::getKey, Tag::getValue));
    }

    public static ResourceModel translateDbClusterFromSdk(
            final software.amazon.awssdk.services.rds.model.DBCluster dbCluster
    ) {
        String domain = null;
        String domainIAMRoleName = null;
        if (CollectionUtils.isNotEmpty(dbCluster.domainMemberships())) {
            domain = dbCluster.domainMemberships().get(0).domain();
            domainIAMRoleName = dbCluster.domainMemberships().get(0).iamRoleName();
        }

        return ResourceModel.builder()
                .allocatedStorage(dbCluster.allocatedStorage())
                .associatedRoles(
                        Optional.ofNullable(dbCluster.associatedRoles())
                                .orElse(Collections.emptyList())
                                .stream()
                                .map(Translator::transformDBCusterRoleFromSdk)
                                .collect(Collectors.toList())
                )
                .availabilityZones(dbCluster.availabilityZones())
                .autoMinorVersionUpgrade(dbCluster.autoMinorVersionUpgrade())
                .backtrackWindow(Translator.castToInt(dbCluster.backtrackWindow()))
                .backupRetentionPeriod(dbCluster.backupRetentionPeriod())
                .copyTagsToSnapshot(dbCluster.copyTagsToSnapshot())
                .databaseName(dbCluster.databaseName())
                .dBClusterArn(dbCluster.dbClusterArn())
                .dBClusterIdentifier(dbCluster.dbClusterIdentifier())
                .dBClusterInstanceClass(dbCluster.dbClusterInstanceClass())
                .dBClusterParameterGroupName(dbCluster.dbClusterParameterGroup())
                .dBClusterResourceId(dbCluster.dbClusterResourceId())
                .dBSubnetGroupName(dbCluster.dbSubnetGroup())
                .deletionProtection(dbCluster.deletionProtection())
                .domain(domain)
                .domainIAMRoleName(domainIAMRoleName)
                .enableCloudwatchLogsExports(dbCluster.enabledCloudwatchLogsExports())
                .enableHttpEndpoint(dbCluster.httpEndpointEnabled())
                .enableIAMDatabaseAuthentication(dbCluster.iamDatabaseAuthenticationEnabled())
                .endpoint(
                        Endpoint.builder()
                                .address(dbCluster.endpoint())
                                .port(Optional.ofNullable(dbCluster.port()).map(Object::toString).orElse(""))
                                .build()
                )
                .engine(dbCluster.engine())
                .engineMode(dbCluster.engineMode())
                .engineVersion(dbCluster.engineVersion())
                .iops(dbCluster.iops())
                .kmsKeyId(dbCluster.kmsKeyId())
                .masterUsername(dbCluster.masterUsername())
                .monitoringInterval(dbCluster.monitoringInterval())
                .monitoringRoleArn(dbCluster.monitoringRoleArn())
                .networkType(dbCluster.networkType())
                .performanceInsightsEnabled(dbCluster.performanceInsightsEnabled())
                .performanceInsightsKmsKeyId(dbCluster.performanceInsightsKMSKeyId())
                .performanceInsightsRetentionPeriod(dbCluster.performanceInsightsRetentionPeriod())
                .port(dbCluster.port())
                .preferredBackupWindow(dbCluster.preferredBackupWindow())
                .preferredMaintenanceWindow(dbCluster.preferredMaintenanceWindow())
                .publiclyAccessible(dbCluster.publiclyAccessible())
                .readEndpoint(
                        ReadEndpoint.builder()
                                .address(dbCluster.readerEndpoint())
                                .build()
                )
                .replicationSourceIdentifier(dbCluster.replicationSourceIdentifier())
                .serverlessV2ScalingConfiguration(translateServerlessV2ScalingConfigurationFromSdk(dbCluster.serverlessV2ScalingConfiguration()))
                .scalingConfiguration(translateScalingConfigurationFromSdk(dbCluster.scalingConfigurationInfo()))
                .storageEncrypted(dbCluster.storageEncrypted())
                .storageType(dbCluster.storageType())
                .tags(translateTagsFromSdk(dbCluster.tagList()))
                .vpcSecurityGroupIds(
                        Optional.ofNullable(dbCluster.vpcSecurityGroups())
                                .orElse(Collections.emptyList())
                                .stream()
                                .map(VpcSecurityGroupMembership::vpcSecurityGroupId)
                                .collect(Collectors.toList())
                )
                .build();
    }

    public static DescribeGlobalClustersRequest describeGlobalClustersRequest(final String globalClusterIdentifier) {
        return DescribeGlobalClustersRequest.builder().globalClusterIdentifier(globalClusterIdentifier).build();
    }

    public static DescribeDbInstancesRequest describeDbInstancesRequest(final String dbInstanceIdentifier) {
        return DescribeDbInstancesRequest.builder().dbInstanceIdentifier(dbInstanceIdentifier).build();
    }

    public static RebootDbInstanceRequest rebootDbInstanceRequest(final String dbInstanceIdentifier) {
        return RebootDbInstanceRequest.builder().dbInstanceIdentifier(dbInstanceIdentifier).build();
    }

    private static <T> Stream<T> streamOfOrEmpty(final Collection<T> collection) {
        return Optional.ofNullable(collection)
                .map(Collection::stream)
                .orElseGet(Stream::empty);
    }

    public static DescribeDbSubnetGroupsRequest describeDbSubnetGroup(final String DbSubnetGroupName) {
        return DescribeDbSubnetGroupsRequest.builder()
                .dbSubnetGroupName(DbSubnetGroupName)
                .build();
    }

    public static DescribeSecurityGroupsRequest describeSecurityGroupsRequest(final String vpcId,
                                                                              final String groupName) {
        return DescribeSecurityGroupsRequest.builder()
                .filters(
                        Filter.builder().name("vpc-id").values(vpcId).build(),
                        Filter.builder().name("group-name").values(groupName).build()
                ).build();
    }
}
