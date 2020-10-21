package software.amazon.rds.globalcluster;

import software.amazon.awssdk.services.rds.model.CreateGlobalClusterRequest;
import software.amazon.awssdk.services.rds.model.ModifyGlobalClusterRequest;
import software.amazon.awssdk.services.rds.model.DescribeGlobalClustersRequest;
import software.amazon.awssdk.services.rds.model.DeleteGlobalClusterRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.RemoveFromGlobalClusterRequest;


/**
 * This class is a centralized placeholder for
 *  - api request construction
 *  - object translation to/from aws sdk
 *  - resource model construction for read/list handlers
 */

public class Translator {

  static CreateGlobalClusterRequest createGlobalClusterRequest(final ResourceModel model) {
    return createGlobalClusterRequest(model, null);
  }

  static CreateGlobalClusterRequest createGlobalClusterRequest(final ResourceModel model, String dbClusterArn) {
    return CreateGlobalClusterRequest.builder()
            .engine(model.getEngine())
            .engineVersion(model.getEngineVersion())
            .deletionProtection(model.getDeletionProtection())
            .globalClusterIdentifier(model.getGlobalClusterIdentifier())
            .sourceDBClusterIdentifier(dbClusterArn)
            .storageEncrypted(model.getStorageEncrypted())
            .build();
  }

  static ModifyGlobalClusterRequest modifyGlobalClusterRequest(final ResourceModel model) {
    return ModifyGlobalClusterRequest.builder()
            .globalClusterIdentifier(model.getGlobalClusterIdentifier())
            .deletionProtection(model.getDeletionProtection())
            .build();
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
