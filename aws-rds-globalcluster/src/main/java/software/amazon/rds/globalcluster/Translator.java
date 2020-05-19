package software.amazon.rds.globalcluster;

import com.google.common.collect.Lists;
import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import com.google.common.collect.Sets;
import software.amazon.awssdk.services.rds.model.CloudwatchLogsExportConfiguration;
import software.amazon.awssdk.services.rds.model.CreateDbClusterRequest;
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
import software.amazon.awssdk.services.rds.model.CreateGlobalClusterRequest;
import software.amazon.awssdk.services.rds.model.ModifyGlobalClusterRequest;
import software.amazon.awssdk.services.rds.model.RemoveFromGlobalClusterRequest;
import software.amazon.awssdk.services.rds.model.DescribeGlobalClustersRequest;
import software.amazon.awssdk.services.rds.model.DeleteGlobalClusterRequest;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class is a centralized placeholder for
 *  - api request construction
 *  - object translation to/from aws sdk
 *  - resource model construction for read/list handlers
 */

public class Translator {

  static CreateGlobalClusterRequest createGlobalClusterRequest(final ResourceModel model) {
    return CreateGlobalClusterRequest.builder()
            .databaseName(model.getDatabaseName())
            .engine(model.getEngine())
            .engineVersion(model.getEngineVersion())
            .deletionProtection(model.getDeletionProtection())
            .globalClusterIdentifier(model.getGlobalClusterIdentifier())
            .sourceDBClusterIdentifier(model.getSourceDBClusterIdentifier())
            .storageEncrypted(model.getStorageEncrypted())
            .build();
  }

  static RemoveFromGlobalClusterRequest removeFromGlobalClusterRequest(final ResourceModel model) {
    return RemoveFromGlobalClusterRequest.builder()
            .dbClusterIdentifier(model.getDBClusterIdentifier())
            .globalClusterIdentifier(model.getGlobalClusterIdentifier())
            .build();
  }

  static ModifyGlobalClusterRequest modifyGlobalClusterRequest(final ResourceModel model) {
    return ModifyGlobalClusterRequest.builder()
            .globalClusterIdentifier(model.getGlobalClusterIdentifier())
            .newGlobalClusterIdentifier(model.getNewGlobalClusterIdentifier())
            .deletionProtection(model.getDeletionProtection())
            .build();
  }

  static DeleteGlobalClusterRequest deleteGlobalClusterRequest(final ResourceModel model) {
    return DeleteGlobalClusterRequest.builder()
            .globalClusterIdentifier(model.getGlobalClusterIdentifier())
            .build();
  }

  static DescribeGlobalClustersRequest describeGlobalClusterRequest(final ResourceModel model) {
    return DescribeGlobalClustersRequest.builder()
            .globalClusterIdentifier(model.getGlobalClusterIdentifier())
            .build();
  }

  static DescribeGlobalClustersRequest describeGlobalClusterRequest(final String nextToken) {
    return DescribeGlobalClustersRequest.builder()
            .marker(nextToken)
            .build();
  }
}
