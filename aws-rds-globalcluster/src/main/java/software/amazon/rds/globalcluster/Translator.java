package software.amazon.rds.globalcluster;

import com.google.common.collect.Lists;
import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.awssdk.services.rds.model.CreateGlobalClusterRequest;
import software.amazon.awssdk.services.rds.model.ModifyGlobalClusterRequest;
import software.amazon.awssdk.services.rds.model.DescribeGlobalClustersRequest;
import software.amazon.awssdk.services.rds.model.DeleteGlobalClusterRequest;

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
            .engine(model.getEngine())
            .engineVersion(model.getEngineVersion())
            .storageEncrypted(model.getStorageEncrypted())
            .deletionProtection(model.getDeletionProtection())
            .globalClusterIdentifier(model.getGlobalClusterIdentifier())
            .sourceDBClusterIdentifier(model.getSourceDBClusterIdentifier())
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
