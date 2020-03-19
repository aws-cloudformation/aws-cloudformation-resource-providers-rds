package software.amazon.rds.dbcluster;

import com.amazonaws.util.StringUtils;
import com.google.common.collect.Sets;
import software.amazon.awssdk.services.rds.model.CloudwatchLogsExportConfiguration;
import software.amazon.awssdk.services.rds.model.CreateDbClusterRequest;
import software.amazon.awssdk.services.rds.model.CreateDbClusterSnapshotRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterSnapshotsRequest;
import software.amazon.awssdk.services.rds.model.RestoreDbClusterToPointInTimeRequest;
import software.amazon.awssdk.services.rds.model.RestoreDbClusterFromSnapshotRequest;
import software.amazon.awssdk.services.rds.model.AddRoleToDbClusterRequest;
import software.amazon.awssdk.services.rds.model.RemoveRoleFromDbClusterRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbClusterRequest;
import software.amazon.awssdk.services.rds.model.DeleteDbClusterRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceRequest;
import software.amazon.awssdk.services.rds.model.ScalingConfigurationInfo;
import software.amazon.awssdk.services.rds.model.ScalingConfiguration;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class Translator {
    static CreateDbClusterRequest createDbClusterRequest(final ResourceModel model) {
        return CreateDbClusterRequest.builder()
                .availabilityZones(model.getAvailabilityZones())
                .backupRetentionPeriod(model.getBackupRetentionPeriod())
                .copyTagsToSnapshot(true)
                .databaseName(model.getDatabaseName())
                .dbClusterIdentifier(model.getDBClusterIdentifier())
                .dbClusterParameterGroupName(model.getDBClusterParameterGroupName())
                .dbSubnetGroupName(model.getDBSubnetGroupName())
                .engine(model.getEngine())
                .engineVersion(model.getEngineVersion())
                .kmsKeyId(model.getKmsKeyId())
                .masterUsername(model.getMasterUsername())
                .masterUserPassword(model.getMasterUserPassword())
                .port(model.getPort())
                .preferredBackupWindow(model.getPreferredBackupWindow())
                .preferredMaintenanceWindow(model.getPreferredMaintenanceWindow())
                .replicationSourceIdentifier(model.getReplicationSourceIdentifier())
                .storageEncrypted(model.getStorageEncrypted())
                .vpcSecurityGroupIds(model.getVpcSecurityGroupIds())
                .tags(translateTagsToSdk(model.getTags()))
                .engineMode(model.getEngineMode())
                .scalingConfiguration(translateScalingConfigurationToSdk(model.getScalingConfiguration()))
                .backtrackWindow(castToLong(model.getBacktrackWindow()))
                .enableCloudwatchLogsExports(model.getEnableCloudwatchLogsExports())
                .enableIAMDatabaseAuthentication(model.getEnableIAMDatabaseAuthentication())
                .deletionProtection(model.getDeletionProtection())
                .enableHttpEndpoint(model.getEnableHttpEndpoint())
                .build();
    }

    static RestoreDbClusterToPointInTimeRequest restoreDbClusterToPointInTimeRequest(final ResourceModel model) {
        return RestoreDbClusterToPointInTimeRequest.builder()
                .dbClusterIdentifier(model.getDBClusterIdentifier())
                .sourceDBClusterIdentifier(model.getSourceDBClusterIdentifier())
                .useLatestRestorableTime(model.getUseLatestRestorableTime())
                .restoreType(model.getRestoreType())
                .build();
    }

    static RestoreDbClusterFromSnapshotRequest restoreDbClusterFromSnapshotRequest(final ResourceModel model) {
        return RestoreDbClusterFromSnapshotRequest.builder()
                .dbClusterIdentifier(model.getDBClusterIdentifier())
                .snapshotIdentifier(model.getSnapshotIdentifier())
                .engine(model.getEngine())
                .engineVersion(model.getEngineVersion())
                .availabilityZones(model.getAvailabilityZones())
                .port(model.getPort())
                .dbSubnetGroupName(model.getDBSubnetGroupName())
                .databaseName(model.getDatabaseName())
                .kmsKeyId(model.getKmsKeyId())
                .vpcSecurityGroupIds(model.getVpcSecurityGroupIds())
                .tags(translateTagsToSdk(model.getTags()))
                .backtrackWindow(castToLong(model.getBacktrackWindow()))
                .enableCloudwatchLogsExports(model.getEnableCloudwatchLogsExports())
                .enableIAMDatabaseAuthentication(model.getEnableIAMDatabaseAuthentication())
                .deletionProtection(model.getDeletionProtection())
                .engineMode(model.getEngineMode())
                .scalingConfiguration(translateScalingConfigurationToSdk(model.getScalingConfiguration()))
                .build();
    }

    static Long castToLong(Object object) {
        return object == null ? null : Long.parseLong(String.valueOf(object));
    }
    static Integer castToInt(Object object) {
        return object == null ? null : Integer.parseInt(String.valueOf(object));
    }

    static AddRoleToDbClusterRequest addRoleToDbClusterRequest(final String dbClusterIdentifier,
                                                               final String roleArn,
                                                               final String featureName) {
        return AddRoleToDbClusterRequest.builder()
                .dbClusterIdentifier(dbClusterIdentifier)
                .roleArn(roleArn)
                .featureName(featureName)
                .build();
    }

    static RemoveRoleFromDbClusterRequest removeRoleFromDbClusterRequest(final String dbClusterIdentifier,
                                                                         final String roleArn,
                                                                         final String featureName) {
        return RemoveRoleFromDbClusterRequest.builder()
                .dbClusterIdentifier(dbClusterIdentifier)
                .roleArn(roleArn)
                .featureName(featureName)
                .build();
    }

    static ModifyDbClusterRequest modifyDbClusterRequest(final ResourceModel model,
                                                         final CloudwatchLogsExportConfiguration config) {
        return ModifyDbClusterRequest.builder()
                .backtrackWindow(castToLong(model.getBacktrackWindow()))
                .cloudwatchLogsExportConfiguration(config)
                .dbClusterIdentifier(model.getDBClusterIdentifier())
                .backupRetentionPeriod(model.getBackupRetentionPeriod())
                .dbClusterParameterGroupName(model.getDBClusterParameterGroupName())
                .deletionProtection(model.getDeletionProtection())
                .enableIAMDatabaseAuthentication(model.getEnableIAMDatabaseAuthentication())
                .enableHttpEndpoint(model.getEnableHttpEndpoint())
                .masterUserPassword(model.getMasterUserPassword())
                .port(model.getPort())
                .preferredBackupWindow(model.getPreferredBackupWindow())
                .preferredMaintenanceWindow(model.getPreferredMaintenanceWindow())
                .scalingConfiguration(translateScalingConfigurationToSdk(model.getScalingConfiguration()))
                .vpcSecurityGroupIds(model.getVpcSecurityGroupIds())
                .engineVersion(model.getEngineVersion())
                .build();
    }

    static CloudwatchLogsExportConfiguration cloudwatchLogsExportConfiguration(final ResourceHandlerRequest<ResourceModel> request) {
        CloudwatchLogsExportConfiguration.Builder config = CloudwatchLogsExportConfiguration.builder();

        final List<String> currentLogsExports = request.getDesiredResourceState().getEnableCloudwatchLogsExports();
        final List<String> previousLogsExports = request.getPreviousResourceState().getEnableCloudwatchLogsExports();


        final Set<String> existingLogs = new HashSet<>(Optional.ofNullable(previousLogsExports).orElse(Collections.emptyList()));
        final Set<String> newLogsExports = new HashSet<>(Optional.ofNullable(currentLogsExports).orElse(Collections.emptyList()));

        final Set<String> logTypesToEnable = Sets.difference(newLogsExports, existingLogs);
        final Set<String> logTypesToDisable = Sets.difference(existingLogs, newLogsExports);

        return config.enableLogTypes(logTypesToEnable).disableLogTypes(logTypesToDisable).build();
    }

    static DeleteDbClusterRequest deleteDbClusterRequest(final ResourceModel model) {
        return DeleteDbClusterRequest.builder()
                .dbClusterIdentifier(model.getDBClusterIdentifier())
                .skipFinalSnapshot(true)
                .build();
    }

    static DeleteDbClusterRequest deleteDbClusterRequest(final ResourceModel model,
                                                         final String snapshotIdentifier) {
        return DeleteDbClusterRequest.builder()
                .dbClusterIdentifier(model.getDBClusterIdentifier())
                .finalDBSnapshotIdentifier(snapshotIdentifier)
                .skipFinalSnapshot(false)
                .build();
    }

    static DescribeDbClustersRequest describeDbClustersRequest(final ResourceModel model) {
        return DescribeDbClustersRequest.builder()
                .dbClusterIdentifier(model.getDBClusterIdentifier())
                .build();
    }

    static DescribeDbClustersRequest describeDbClustersRequest(final String nextToken) {
        return DescribeDbClustersRequest.builder()
                .marker(nextToken)
                .build();
    }

    static ListTagsForResourceRequest listTagsForResourceRequest(final String dbClusterArn) {
        return ListTagsForResourceRequest.builder()
                .resourceName(dbClusterArn)
                .build();
    }

    static AddTagsToResourceRequest addTagsToResourceRequest(final String dbClusterParameterGroupArn,
                                                             final Set<software.amazon.rds.dbcluster.Tag> tags) {
        return AddTagsToResourceRequest.builder()
                .resourceName(dbClusterParameterGroupArn)
                .tags(translateTagsToSdk(tags))
                .build();
    }

    static RemoveTagsFromResourceRequest removeTagsFromResourceRequest(final String dbClusterParameterGroupArn,
                                                                       final Set<software.amazon.rds.dbcluster.Tag> tags) {
        return RemoveTagsFromResourceRequest.builder()
                .resourceName(dbClusterParameterGroupArn)
                .tagKeys(tags
                        .stream()
                        .map(software.amazon.rds.dbcluster.Tag::getKey)
                        .collect(Collectors.toSet()))
                .build();
    }

    static Set<Tag> translateTagsToSdk(final Collection<software.amazon.rds.dbcluster.Tag> tags) {
        return Optional.ofNullable(tags).orElse(Collections.emptySet())
                .stream()
                .map(tag -> Tag.builder().key(tag.getKey()).value(tag.getValue()).build())
                .collect(Collectors.toSet());
    }

    static Set<software.amazon.rds.dbcluster.Tag> translateTagsFromSdk(final Collection<Tag> tags) {
        return Optional.ofNullable(tags).orElse(Collections.emptySet())
                .stream()
                .map(tag -> software.amazon.rds.dbcluster.Tag.builder().key(tag.key()).value(tag.value()).build())
                .collect(Collectors.toSet());
    }

    static ScalingConfiguration translateScalingConfigurationToSdk(final software.amazon.rds.dbcluster.ScalingConfiguration scalingConfiguration) {
        if (scalingConfiguration == null) return null;
        return ScalingConfiguration.builder()
                .autoPause(scalingConfiguration.getAutoPause())
                .maxCapacity(scalingConfiguration.getMaxCapacity())
                .minCapacity(scalingConfiguration.getMinCapacity())
                .secondsUntilAutoPause(scalingConfiguration.getSecondsUntilAutoPause()).build();
    }

    static software.amazon.rds.dbcluster.ScalingConfiguration translateScalingConfigurationFromSdk(final ScalingConfigurationInfo scalingConfiguration) {
        if (scalingConfiguration == null) return null;
        return software.amazon.rds.dbcluster.ScalingConfiguration.builder()
                .autoPause(scalingConfiguration.autoPause())
                .maxCapacity(scalingConfiguration.maxCapacity())
                .minCapacity(scalingConfiguration.minCapacity())
                .secondsUntilAutoPause(scalingConfiguration.secondsUntilAutoPause()).build();
    }
}
