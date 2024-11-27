package software.amazon.rds.globalcluster;

import java.util.Objects;

import software.amazon.awssdk.services.rds.model.CreateGlobalClusterRequest;
import software.amazon.awssdk.services.rds.model.ModifyGlobalClusterRequest;
import software.amazon.awssdk.services.rds.model.DescribeGlobalClustersRequest;
import software.amazon.awssdk.services.rds.model.DeleteGlobalClusterRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.RemoveFromGlobalClusterRequest;
import software.amazon.awssdk.utils.StringUtils;


/**
 * This class is a centralized placeholder for
 *  - api request construction
 *  - object translation to/from aws sdk
 *  - resource model construction for read/list handlers
 */

public class Translator {

  static CreateGlobalClusterRequest createGlobalClusterRequest(final ResourceModel model) {
    return createGlobalClusterRequest(model, null, Tagging.TagSet.emptySet());
  }

  static CreateGlobalClusterRequest createGlobalClusterRequest(final ResourceModel model, String dbClusterArn, final Tagging.TagSet tagSet) {
    return CreateGlobalClusterRequest.builder()
            .engine(model.getEngine())
            .engineVersion(model.getEngineVersion())
            .deletionProtection(model.getDeletionProtection())
            .globalClusterIdentifier(model.getGlobalClusterIdentifier())
            .sourceDBClusterIdentifier(StringUtils.isBlank(dbClusterArn) ? model.getSourceDBClusterIdentifier() : dbClusterArn)
            .storageEncrypted(model.getStorageEncrypted())
            .engineLifecycleSupport(model.getEngineLifecycleSupport())
            .tags(Tagging.translateTagsToSdk(tagSet))
            .build();
  }

  static ModifyGlobalClusterRequest modifyGlobalClusterRequest(
          final ResourceModel previousModel,
          final ResourceModel desiredModel,
          final boolean isRollback
  ) {
    ModifyGlobalClusterRequest.Builder builder = ModifyGlobalClusterRequest.builder()
            .globalClusterIdentifier(desiredModel.getGlobalClusterIdentifier())
            .deletionProtection(desiredModel.getDeletionProtection());

    if (previousModel != null) {
      if (!(isRollback || Objects.equals(previousModel.getEngineVersion(), desiredModel.getEngineVersion()))) {
        builder.engineVersion(desiredModel.getEngineVersion());
        builder.allowMajorVersionUpgrade(true);
      }
    }

    return builder.build();
  }

  static DeleteGlobalClusterRequest deleteGlobalClusterRequest(final ResourceModel model) {
    return DeleteGlobalClusterRequest.builder()
            .globalClusterIdentifier(model.getGlobalClusterIdentifier())
            .build();
  }

  static RemoveFromGlobalClusterRequest removeFromGlobalClusterRequest(final ResourceModel model, String arn) {
    return RemoveFromGlobalClusterRequest.builder()
            .globalClusterIdentifier(model.getGlobalClusterIdentifier())
            .dbClusterIdentifier(arn)
            .build();
  }

  static DescribeGlobalClustersRequest describeGlobalClustersRequest(final ResourceModel model) {
    return DescribeGlobalClustersRequest.builder()
            .globalClusterIdentifier(model.getGlobalClusterIdentifier())
            .build();
  }

  static DescribeGlobalClustersRequest describeGlobalClusterRequest(final String nextToken) {
    return DescribeGlobalClustersRequest.builder()
            .marker(nextToken)
            .build();
  }

  static DescribeDbClustersRequest describeDbClustersRequest(final ResourceModel model) {
    return DescribeDbClustersRequest.builder()
            .dbClusterIdentifier(model.getSourceDBClusterIdentifier())
            .build();
  }
}
