package software.amazon.rds.dbcluster;

import static software.amazon.rds.common.util.DifferenceUtils.diff;

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
import org.apache.commons.lang3.BooleanUtils;

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
import software.amazon.awssdk.services.rds.model.DisableHttpEndpointRequest;
import software.amazon.awssdk.services.rds.model.EnableHttpEndpointRequest;
import software.amazon.awssdk.services.rds.model.LocalWriteForwardingStatus;
import software.amazon.awssdk.services.rds.model.ModifyDbClusterRequest;
import software.amazon.awssdk.services.rds.model.RebootDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.RemoveFromGlobalClusterRequest;
import software.amazon.awssdk.services.rds.model.RemoveRoleFromDbClusterRequest;
import software.amazon.awssdk.services.rds.model.RestoreDbClusterFromSnapshotRequest;
import software.amazon.awssdk.services.rds.model.RestoreDbClusterToPointInTimeRequest;
import software.amazon.awssdk.services.rds.model.VpcSecurityGroupMembership;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.Tagging;

public class Translator {

    private final static String STORAGE_TYPE_AURORA = "aurora";

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
                .dbSystemId(model.getDBSystemId())
                .deletionProtection(model.getDeletionProtection())
                .domain(model.getDomain())
                .domainIAMRoleName(model.getDomainIAMRoleName())
                .enableCloudwatchLogsExports(model.getEnableCloudwatchLogsExports())
                .enableGlobalWriteForwarding(model.getEnableGlobalWriteForwarding())
                .enableHttpEndpoint(model.getEnableHttpEndpoint())
                .enableIAMDatabaseAuthentication(model.getEnableIAMDatabaseAuthentication())
                .enableLocalWriteForwarding(model.getEnableLocalWriteForwarding())
                .enablePerformanceInsights(model.getPerformanceInsightsEnabled())
                .engine(model.getEngine())
                .engineMode(model.getEngineMode())
                .engineVersion(model.getEngineVersion())
                .globalClusterIdentifier(model.getGlobalClusterIdentifier())
                .manageMasterUserPassword(model.getManageMasterUserPassword())
                .iops(model.getIops())
                .kmsKeyId(model.getKmsKeyId())
                .masterUserPassword(model.getMasterUserPassword())
                .masterUsername(model.getMasterUsername())
                .masterUserSecretKmsKeyId(model.getMasterUserSecret() != null ? model.getMasterUserSecret().getKmsKeyId() : null)
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
                .engineLifecycleSupport(model.getEngineLifecycleSupport())
                .build();
    }

    static RestoreDbClusterToPointInTimeRequest restoreDbClusterToPointInTimeRequest(
            final ResourceModel model,
            final Tagging.TagSet tagSet
    ) {
        RestoreDbClusterToPointInTimeRequest.Builder builder = RestoreDbClusterToPointInTimeRequest.builder()
                .copyTagsToSnapshot(model.getCopyTagsToSnapshot())
                .dbClusterIdentifier(model.getDBClusterIdentifier())
                .dbClusterInstanceClass(model.getDBClusterInstanceClass())
                .dbSubnetGroupName(model.getDBSubnetGroupName())
                .domain(model.getDomain())
                .domainIAMRoleName(model.getDomainIAMRoleName())
                .enableCloudwatchLogsExports(model.getEnableCloudwatchLogsExports())
                .iops(model.getIops())
                .kmsKeyId(model.getKmsKeyId())
                .networkType(model.getNetworkType())
                .port(model.getPort())
                .publiclyAccessible(model.getPubliclyAccessible())
                .scalingConfiguration(translateScalingConfigurationToSdk(model.getScalingConfiguration()))
                .serverlessV2ScalingConfiguration(translateServerlessV2ScalingConfiguration(model.getServerlessV2ScalingConfiguration()))
                .sourceDBClusterIdentifier(model.getSourceDBClusterIdentifier())
                .storageType(model.getStorageType())
                .restoreType(model.getRestoreType())
                .tags(Tagging.translateTagsToSdk(tagSet))
                .useLatestRestorableTime(model.getUseLatestRestorableTime())
                .vpcSecurityGroupIds(model.getVpcSecurityGroupIds())
                .engineLifecycleSupport(model.getEngineLifecycleSupport());

        if (StringUtils.hasValue(model.getRestoreToTime())
                && BooleanUtils.isNotTrue(model.getUseLatestRestorableTime())) {
            builder.restoreToTime(Commons.parseTimestamp(model.getRestoreToTime()));
        }

        return builder.build();
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
                .engineLifecycleSupport(model.getEngineLifecycleSupport())
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

    static ModifyDbClusterRequest modifyDbClusterAfterCreateRequest(final ResourceModel desiredModel) {
        final CloudwatchLogsExportConfiguration config = cloudwatchLogsExportConfiguration(null, desiredModel);

        final ModifyDbClusterRequest.Builder builder = ModifyDbClusterRequest.builder()
                .applyImmediately(Boolean.TRUE)
                .autoMinorVersionUpgrade(desiredModel.getAutoMinorVersionUpgrade())
                .backupRetentionPeriod(desiredModel.getBackupRetentionPeriod())
                .cloudwatchLogsExportConfiguration(config)
                .copyTagsToSnapshot(desiredModel.getCopyTagsToSnapshot())
                .dbClusterIdentifier(desiredModel.getDBClusterIdentifier())
                .dbClusterInstanceClass(desiredModel.getDBClusterInstanceClass())
                .dbClusterParameterGroupName(desiredModel.getDBClusterParameterGroupName())
                .deletionProtection(desiredModel.getDeletionProtection())
                .domain(desiredModel.getDomain())
                .domainIAMRoleName(desiredModel.getDomainIAMRoleName())
                .enableGlobalWriteForwarding(desiredModel.getEnableGlobalWriteForwarding())
                .enableLocalWriteForwarding(desiredModel.getEnableLocalWriteForwarding())
                .enablePerformanceInsights(desiredModel.getPerformanceInsightsEnabled())
                .iops(desiredModel.getIops())
                .masterUserPassword(desiredModel.getMasterUserPassword())
                .monitoringInterval(desiredModel.getMonitoringInterval())
                .monitoringRoleArn(desiredModel.getMonitoringRoleArn())
                .networkType(desiredModel.getNetworkType())
                .performanceInsightsKMSKeyId(desiredModel.getPerformanceInsightsKmsKeyId())
                .performanceInsightsRetentionPeriod(desiredModel.getPerformanceInsightsRetentionPeriod())
                .preferredMaintenanceWindow(desiredModel.getPreferredMaintenanceWindow())
                .scalingConfiguration(translateScalingConfigurationToSdk(desiredModel.getScalingConfiguration()))
                .storageType(desiredModel.getStorageType());

        if (EngineMode.fromString(desiredModel.getEngineMode()) != EngineMode.Serverless) {
            builder.allocatedStorage(desiredModel.getAllocatedStorage())
                    .backtrackWindow(castToLong(desiredModel.getBacktrackWindow()))
                    .enableIAMDatabaseAuthentication(desiredModel.getEnableIAMDatabaseAuthentication())
                    .preferredBackupWindow(desiredModel.getPreferredBackupWindow());
        } else {
            builder.enableHttpEndpoint(desiredModel.getEnableHttpEndpoint());
        }

        if (BooleanUtils.isTrue(desiredModel.getManageMasterUserPassword())) {
            builder.manageMasterUserPassword(true);
            builder.masterUserSecretKmsKeyId(desiredModel.getMasterUserSecret() != null ? desiredModel.getMasterUserSecret().getKmsKeyId() : null);
        }

        return builder.build();
    }

    static ModifyDbClusterRequest modifyDbClusterRequest(
            final ResourceModel previousModel,
            final ResourceModel desiredModel,
            final boolean isRollback
    ) {

        final CloudwatchLogsExportConfiguration config = cloudwatchLogsExportConfiguration(previousModel, desiredModel);

        final ModifyDbClusterRequest.Builder builder = ModifyDbClusterRequest.builder()
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
                .enableGlobalWriteForwarding(desiredModel.getEnableGlobalWriteForwarding())
                .enableIAMDatabaseAuthentication(diff(previousModel.getEnableIAMDatabaseAuthentication(), desiredModel.getEnableIAMDatabaseAuthentication()))
                .enableLocalWriteForwarding(desiredModel.getEnableLocalWriteForwarding())
                .enablePerformanceInsights(desiredModel.getPerformanceInsightsEnabled())
                .iops(desiredModel.getIops())
                .masterUserPassword(diff(previousModel.getMasterUserPassword(), desiredModel.getMasterUserPassword()))
                .monitoringInterval(desiredModel.getMonitoringInterval())
                .monitoringRoleArn(desiredModel.getMonitoringRoleArn())
                .networkType(desiredModel.getNetworkType())
                .performanceInsightsKMSKeyId(desiredModel.getPerformanceInsightsKmsKeyId())
                .performanceInsightsRetentionPeriod(desiredModel.getPerformanceInsightsRetentionPeriod())
                .port(diff(previousModel.getPort(), desiredModel.getPort()))
                .preferredBackupWindow(diff(previousModel.getPreferredBackupWindow(), desiredModel.getPreferredBackupWindow()))
                .preferredMaintenanceWindow(diff(previousModel.getPreferredMaintenanceWindow(), desiredModel.getPreferredMaintenanceWindow()))
                .scalingConfiguration(translateScalingConfigurationToSdk(desiredModel.getScalingConfiguration()))
                .serverlessV2ScalingConfiguration(
                        diff(
                                translateServerlessV2ScalingConfiguration(previousModel.getServerlessV2ScalingConfiguration()),
                                translateServerlessV2ScalingConfiguration(desiredModel.getServerlessV2ScalingConfiguration())
                        )
                )
                .storageType(desiredModel.getStorageType());

        if (!(isRollback || Objects.equals(previousModel.getEngineVersion(), desiredModel.getEngineVersion()))) {
            builder.engineVersion(desiredModel.getEngineVersion());
            builder.allowMajorVersionUpgrade(true);
            builder.dbInstanceParameterGroupName(diff(previousModel.getDBInstanceParameterGroupName(), desiredModel.getDBInstanceParameterGroupName()));
        }
        //only include VPC SG ids if they are changed from previous.
        final Set<String> desiredVpcSgIds = streamOfOrEmpty(desiredModel.getVpcSecurityGroupIds()).collect(Collectors.toSet());
        if (!desiredVpcSgIds.isEmpty()) {
            final Set<String> previousVpcSgIds = streamOfOrEmpty(previousModel.getVpcSecurityGroupIds()).collect(Collectors.toSet());
            if (!desiredVpcSgIds.equals(previousVpcSgIds)) {
                builder.vpcSecurityGroupIds(desiredModel.getVpcSecurityGroupIds());
            }
        }

        if (BooleanUtils.isTrue(desiredModel.getManageMasterUserPassword())) {
            builder.manageMasterUserPassword(true);
            builder.masterUserSecretKmsKeyId(desiredModel.getMasterUserSecret() != null ? desiredModel.getMasterUserSecret().getKmsKeyId() : null);
        } else {
            builder.manageMasterUserPassword(getManageMasterUserPassword(previousModel, desiredModel));
        }

        if (EngineMode.fromString(desiredModel.getEngineMode()) == EngineMode.Serverless) {
            builder.enableHttpEndpoint(desiredModel.getEnableHttpEndpoint());
        }

        return builder.build();
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

    static RemoveFromGlobalClusterRequest removeFromGlobalClusterRequest(
            final String globalClusterIdentifier,
            final String clusterArn
    ) {
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

    static EnableHttpEndpointRequest enableHttpEndpointRequest(
            final String clusterArn
    ) {
        return EnableHttpEndpointRequest.builder()
                .resourceArn(clusterArn)
                .build();
    }

    static DisableHttpEndpointRequest disableHttpEndpointRequest(
            final String clusterArn
    ) {
        return DisableHttpEndpointRequest.builder()
                .resourceArn(clusterArn)
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

    static boolean translateLocalWriteForwardingStatus(final LocalWriteForwardingStatus status) {
        /*
         * LocalWriteForwarding reports status as an enum rather than a boolean.
         * CFN stabilization requires a boolean value to stabilize.
         * This method projects the status into ENABLED X DISABLED.
         * Both ENABLING and DISABLING states are stabilized on and are considered transient.
         */
        return status == LocalWriteForwardingStatus.REQUESTED ||
                status == LocalWriteForwardingStatus.ENABLED;
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
                .secondsBeforeTimeout(scalingConfiguration.getSecondsBeforeTimeout())
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
                .secondsBeforeTimeout(scalingConfiguration.secondsBeforeTimeout())
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
                .dBSystemId(dbCluster.dbSystemId())
                .deletionProtection(dbCluster.deletionProtection())
                .domain(domain)
                .domainIAMRoleName(domainIAMRoleName)
                .enableCloudwatchLogsExports(dbCluster.enabledCloudwatchLogsExports())
                .enableGlobalWriteForwarding(dbCluster.globalWriteForwardingRequested())
                .enableHttpEndpoint(dbCluster.httpEndpointEnabled())
                .enableIAMDatabaseAuthentication(dbCluster.iamDatabaseAuthenticationEnabled())
                .enableLocalWriteForwarding(translateLocalWriteForwardingStatus(dbCluster.localWriteForwardingStatus()))
                .endpoint(
                        Endpoint.builder()
                                .address(dbCluster.endpoint())
                                .port(Optional.ofNullable(dbCluster.port()).map(Object::toString).orElse(""))
                                .build()
                )
                .engine(dbCluster.engine())
                .engineLifecycleSupport(dbCluster.engineLifecycleSupport())
                .engineMode(dbCluster.engineMode())
                .engineVersion(dbCluster.engineVersion())
                .manageMasterUserPassword(dbCluster.masterUserSecret() != null)
                .iops(dbCluster.iops())
                .kmsKeyId(dbCluster.kmsKeyId())
                .masterUsername(dbCluster.masterUsername())
                .masterUserSecret(translateMasterUserSecretFromSdk(dbCluster.masterUserSecret()))
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
                .storageThroughput(dbCluster.storageThroughput())
                .storageType(Optional.ofNullable(dbCluster.storageType()).orElse(STORAGE_TYPE_AURORA))
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

    public static DescribeSecurityGroupsRequest describeSecurityGroupsRequest(
            final String vpcId,
            final String groupName
    ) {
        return DescribeSecurityGroupsRequest.builder()
                .filters(
                        Filter.builder().name("vpc-id").values(vpcId).build(),
                        Filter.builder().name("group-name").values(groupName).build()
                ).build();
    }

    public static MasterUserSecret translateMasterUserSecretFromSdk(
            final software.amazon.awssdk.services.rds.model.MasterUserSecret sdkSecret
    ) {
        if (sdkSecret == null) {
            return MasterUserSecret.builder().build();
        }

        return MasterUserSecret.builder()
                .secretArn(sdkSecret.secretArn())
                .kmsKeyId(sdkSecret.kmsKeyId())
                .build();
    }
}
