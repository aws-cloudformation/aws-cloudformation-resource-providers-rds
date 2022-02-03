package software.amazon.rds.dbcluster;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.amazonaws.util.StringUtils;
import com.google.common.collect.Sets;
import software.amazon.awssdk.services.rds.model.AddRoleToDbClusterRequest;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.CloudwatchLogsExportConfiguration;
import software.amazon.awssdk.services.rds.model.CreateDbClusterRequest;
import software.amazon.awssdk.services.rds.model.CreateDbClusterSnapshotRequest;
import software.amazon.awssdk.services.rds.model.DeleteDbClusterRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterSnapshotsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbClusterRequest;
import software.amazon.awssdk.services.rds.model.RemoveFromGlobalClusterRequest;
import software.amazon.awssdk.services.rds.model.RemoveRoleFromDbClusterRequest;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceRequest;
import software.amazon.awssdk.services.rds.model.RestoreDbClusterFromSnapshotRequest;
import software.amazon.awssdk.services.rds.model.RestoreDbClusterToPointInTimeRequest;
import software.amazon.awssdk.services.rds.model.VpcSecurityGroupMembership;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class Translator {
    static CreateDbClusterRequest createDbClusterRequest(final ResourceModel model) {
        return CreateDbClusterRequest.builder()
                .availabilityZones(model.getAvailabilityZones())
                .backtrackWindow(castToLong(model.getBacktrackWindow()))
                .backupRetentionPeriod(model.getBackupRetentionPeriod())
                .copyTagsToSnapshot(model.getCopyTagsToSnapshot())
                .databaseName(model.getDatabaseName())
                .dbClusterIdentifier(model.getDBClusterIdentifier())
                .dbClusterParameterGroupName(model.getDBClusterParameterGroupName())
                .dbSubnetGroupName(model.getDBSubnetGroupName())
                .deletionProtection(model.getDeletionProtection())
                .enableCloudwatchLogsExports(model.getEnableCloudwatchLogsExports())
                .enableHttpEndpoint(model.getEnableHttpEndpoint())
                .enableIAMDatabaseAuthentication(model.getEnableIAMDatabaseAuthentication())
                .engine(model.getEngine())
                .engineMode(model.getEngineMode())
                .engineVersion(model.getEngineVersion())
                .globalClusterIdentifier(model.getGlobalClusterIdentifier())
                .kmsKeyId(model.getKmsKeyId())
                .masterUserPassword(model.getMasterUserPassword())
                .masterUsername(model.getMasterUsername())
                .port(model.getPort())
                .preferredBackupWindow(model.getPreferredBackupWindow())
                .preferredMaintenanceWindow(model.getPreferredMaintenanceWindow())
                .replicationSourceIdentifier(model.getReplicationSourceIdentifier())
                .scalingConfiguration(translateScalingConfigurationToSdk(model.getScalingConfiguration()))
                .sourceRegion(model.getSourceRegion())
                .storageEncrypted(model.getStorageEncrypted())
                .tags(translateTagsToSdk(model.getTags()))
                .vpcSecurityGroupIds(model.getVpcSecurityGroupIds())
                .build();
    }

    static RestoreDbClusterToPointInTimeRequest restoreDbClusterToPointInTimeRequest(final ResourceModel model) {
        return RestoreDbClusterToPointInTimeRequest.builder()
                .copyTagsToSnapshot(model.getCopyTagsToSnapshot())
                .dbClusterIdentifier(model.getDBClusterIdentifier())
                .dbSubnetGroupName(model.getDBSubnetGroupName())
                .restoreType(model.getRestoreType())
                .sourceDBClusterIdentifier(model.getSourceDBClusterIdentifier())
                .useLatestRestorableTime(model.getUseLatestRestorableTime())
                .build();
    }

    static RestoreDbClusterFromSnapshotRequest restoreDbClusterFromSnapshotRequest(final ResourceModel model) {
        return RestoreDbClusterFromSnapshotRequest.builder()
                .availabilityZones(model.getAvailabilityZones())
                .backtrackWindow(castToLong(model.getBacktrackWindow()))
                .copyTagsToSnapshot(model.getCopyTagsToSnapshot())
                .databaseName(model.getDatabaseName())
                .dbClusterIdentifier(model.getDBClusterIdentifier())
                .dbSubnetGroupName(model.getDBSubnetGroupName())
                .deletionProtection(model.getDeletionProtection())
                .enableCloudwatchLogsExports(model.getEnableCloudwatchLogsExports())
                .enableIAMDatabaseAuthentication(model.getEnableIAMDatabaseAuthentication())
                .engine(model.getEngine())
                .engineMode(model.getEngineMode())
                .engineVersion(model.getEngineVersion())
                .kmsKeyId(model.getKmsKeyId())
                .port(model.getPort())
                .scalingConfiguration(translateScalingConfigurationToSdk(model.getScalingConfiguration()))
                .snapshotIdentifier(model.getSnapshotIdentifier())
                .tags(translateTagsToSdk(model.getTags()))
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
        return modifyDbClusterRequest(model, CloudwatchLogsExportConfiguration.builder().build());
    }

    static ModifyDbClusterRequest modifyDbClusterRequest(
            final ResourceModel model,
            final CloudwatchLogsExportConfiguration config
    ) {
        return ModifyDbClusterRequest.builder()
                .backtrackWindow(castToLong(model.getBacktrackWindow()))
                .backupRetentionPeriod(model.getBackupRetentionPeriod())
                .cloudwatchLogsExportConfiguration(config)
                .copyTagsToSnapshot(model.getCopyTagsToSnapshot())
                .dbClusterIdentifier(model.getDBClusterIdentifier())
                .dbClusterParameterGroupName(model.getDBClusterParameterGroupName())
                .deletionProtection(model.getDeletionProtection())
                .enableHttpEndpoint(model.getEnableHttpEndpoint())
                .enableIAMDatabaseAuthentication(model.getEnableIAMDatabaseAuthentication())
                .engineVersion(model.getEngineVersion())
                .masterUserPassword(model.getMasterUserPassword())
                .port(model.getPort())
                .preferredBackupWindow(model.getPreferredBackupWindow())
                .preferredMaintenanceWindow(model.getPreferredMaintenanceWindow())
                .scalingConfiguration(translateScalingConfigurationToSdk(model.getScalingConfiguration()))
                .vpcSecurityGroupIds(model.getVpcSecurityGroupIds())
                .build();
    }

    static CloudwatchLogsExportConfiguration cloudwatchLogsExportConfiguration(
            final ResourceHandlerRequest<ResourceModel> request
    ) {
        CloudwatchLogsExportConfiguration.Builder config = CloudwatchLogsExportConfiguration.builder();

        final List<String> currentLogsExports = request.getDesiredResourceState().getEnableCloudwatchLogsExports();
        final List<String> previousLogsExports = request.getPreviousResourceState().getEnableCloudwatchLogsExports();


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

    static RemoveFromGlobalClusterRequest removeFromGlobalClusterRequest(final ResourceModel model, final String clusterArn) {
        return RemoveFromGlobalClusterRequest.builder()
                .dbClusterIdentifier(clusterArn)
                .globalClusterIdentifier(model.getGlobalClusterIdentifier())
                .build();
    }

    static CreateDbClusterSnapshotRequest createDbClusterSnapshotRequest(
            final ResourceModel model,
            final String finalDBSnapshotIdentifier
    ) {
        return CreateDbClusterSnapshotRequest.builder()
                .dbClusterIdentifier(model.getDBClusterIdentifier())
                .dbClusterSnapshotIdentifier(finalDBSnapshotIdentifier)
                .build();
    }

    static DescribeDbClusterSnapshotsRequest describeDbClusterSnapshotsRequest(
            final String dbSnapshotIdentifier
    ) {
        return DescribeDbClusterSnapshotsRequest.builder()
                .dbClusterSnapshotIdentifier(dbSnapshotIdentifier)
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

    static AddTagsToResourceRequest addTagsToResourceRequest(
            final String dbClusterParameterGroupArn,
            final Set<Tag> tags
    ) {
        return AddTagsToResourceRequest.builder()
                .resourceName(dbClusterParameterGroupArn)
                .tags(translateTagsToSdk(tags))
                .build();
    }

    static RemoveTagsFromResourceRequest removeTagsFromResourceRequest(
            final String dbClusterParameterGroupArn,
            final Set<Tag> tags
    ) {
        return RemoveTagsFromResourceRequest.builder()
                .resourceName(dbClusterParameterGroupArn)
                .tagKeys(tags
                        .stream()
                        .map(Tag::getKey)
                        .collect(Collectors.toSet()))
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
                .secondsUntilAutoPause(scalingConfiguration.getSecondsUntilAutoPause()).build();
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
                .secondsUntilAutoPause(scalingConfiguration.secondsUntilAutoPause()).build();
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

    public static Set<Tag> translateTagsFromRequest(final Map<String, String> tags) {
        return Optional.ofNullable(tags).orElse(Collections.emptyMap())
                .entrySet()
                .stream()
                .map(entry -> Tag.builder()
                        .key(entry.getKey())
                        .value(entry.getValue())
                        .build())
                .collect(Collectors.toSet());
    }

    public static ResourceModel translateDbClusterFromSdk(
            final software.amazon.awssdk.services.rds.model.DBCluster dbCluster
    ) {
        return ResourceModel.builder()
                .associatedRoles(
                        Optional.ofNullable(dbCluster.associatedRoles())
                                .orElse(Collections.emptyList())
                                .stream()
                                .map(Translator::transformDBCusterRoleFromSdk)
                                .collect(Collectors.toList())
                )
                .availabilityZones(dbCluster.availabilityZones())
                .backtrackWindow(Translator.castToInt(dbCluster.backtrackWindow()))
                .backupRetentionPeriod(dbCluster.backupRetentionPeriod())
                .copyTagsToSnapshot(dbCluster.copyTagsToSnapshot())
                .databaseName(dbCluster.databaseName())
                .dBClusterIdentifier(dbCluster.dbClusterIdentifier())
                .dBClusterParameterGroupName(dbCluster.dbClusterParameterGroup())
                .dBSubnetGroupName(dbCluster.dbSubnetGroup())
                .deletionProtection(dbCluster.deletionProtection())
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
                .kmsKeyId(dbCluster.kmsKeyId())
                .masterUsername(dbCluster.masterUsername())
                .port(dbCluster.port())
                .preferredBackupWindow(dbCluster.preferredBackupWindow())
                .preferredMaintenanceWindow(dbCluster.preferredMaintenanceWindow())
                .readEndpoint(
                        ReadEndpoint.builder()
                                .address(dbCluster.readerEndpoint())
                                .build()
                )
                .replicationSourceIdentifier(dbCluster.replicationSourceIdentifier())
                .scalingConfiguration(Translator.translateScalingConfigurationFromSdk(dbCluster.scalingConfigurationInfo()))
                .storageEncrypted(dbCluster.storageEncrypted())
                .tags(Translator.translateTagsFromSdk(dbCluster.tagList()))
                .vpcSecurityGroupIds(
                        Optional.ofNullable(dbCluster.vpcSecurityGroups())
                                .orElse(Collections.emptyList())
                                .stream()
                                .map(VpcSecurityGroupMembership::vpcSecurityGroupId)
                                .collect(Collectors.toList())
                )
                .build();
    }
}
