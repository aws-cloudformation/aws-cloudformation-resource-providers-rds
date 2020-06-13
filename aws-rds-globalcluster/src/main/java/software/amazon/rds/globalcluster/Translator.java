package software.amazon.rds.globalcluster;

import software.amazon.awssdk.services.rds.model.CreateGlobalClusterRequest;
import software.amazon.awssdk.services.rds.model.ModifyGlobalClusterRequest;
import software.amazon.awssdk.services.rds.model.DescribeGlobalClustersRequest;
import software.amazon.awssdk.services.rds.model.DeleteGlobalClusterRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;


/**
 * This class is a centralized placeholder for
 *  - api request construction
 *  - object translation to/from aws sdk
 *  - resource model construction for read/list handlers
 */

public class Translator {

  static CreateGlobalClusterRequest createGlobalClusterRequest(final ResourceModel model) {
    return CreateGlobalClusterRequest.builder()
            .engine(model.getEngine())
            .engineVersion(model.getEngineVersion())
            .storageEncrypted(model.getStorageEncrypted())
            .deletionProtection(model.getDeletionProtection())
            .globalClusterIdentifier(model.getGlobalClusterIdentifier())
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
}
