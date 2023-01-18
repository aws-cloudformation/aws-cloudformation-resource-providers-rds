package software.amazon.rds.dbclustersnapshot;

import com.google.common.collect.Lists;
import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CreateDbClusterSnapshotRequest;
import software.amazon.awssdk.services.rds.model.CreateDbSnapshotRequest;
import software.amazon.awssdk.services.rds.model.DBClusterSnapshot;
import software.amazon.awssdk.services.rds.model.DBSnapshot;
import software.amazon.awssdk.services.rds.model.DeleteDbClusterSnapshotRequest;
import software.amazon.awssdk.services.rds.model.DeleteDbSnapshotRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterSnapshotsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterSnapshotsResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsResponse;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.common.handler.Tagging;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class is a centralized placeholder for
 *  - api request construction
 *  - object translation to/from aws sdk
 *  - resource model construction for read/list handlers
 */

public class Translator {
  static DescribeDbClustersRequest describeDbClustersRequest(
          final ResourceModel model
  ) {
    return DescribeDbClustersRequest.builder()
            .dbClusterIdentifier(model.getDBClusterIdentifier())
            .build();
  }

  public static DescribeDbClusterSnapshotsRequest describeDbClusterSnapshotsRequest(final ResourceModel model) {
    return DescribeDbClusterSnapshotsRequest.builder()
            .dbClusterSnapshotIdentifier(model.getDBClusterSnapshotIdentifier())
            .build();
  }

  public static DescribeDbClusterSnapshotsRequest describeDbClusterSnapshotsRequest(final String nextToken) {
    return DescribeDbClusterSnapshotsRequest.builder()
            .marker(nextToken)
            .build();
  }

  public static CreateDbClusterSnapshotRequest createDbClusterSnapshotRequest(final ResourceModel model,
                                                                       final Tagging.TagSet tags) {
    return CreateDbClusterSnapshotRequest.builder()
            .dbClusterSnapshotIdentifier(model.getDBClusterSnapshotIdentifier())
            .dbClusterIdentifier(model.getDBClusterIdentifier())
            .tags(Tagging.translateTagsToSdk(tags))
            .build();
  }

  public static DeleteDbClusterSnapshotRequest deleteDbClusterSnapshotRequest(final ResourceModel resourceModel) {
    return DeleteDbClusterSnapshotRequest.builder()
            .dbClusterSnapshotIdentifier(resourceModel.getDBClusterSnapshotIdentifier())
            .build();
  }

  public static Set<software.amazon.awssdk.services.rds.model.Tag> translateTagsToSdk(final List<Tag> tags) {
    return streamOfOrEmpty(tags)
            .map(tag -> software.amazon.awssdk.services.rds.model.Tag.builder()
                    .key(tag.getKey())
                    .value(tag.getValue())
                    .build()
            )
            .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  public static List<Tag> translateToModel(final Collection<software.amazon.awssdk.services.rds.model.Tag> sdkTags) {
    return streamOfOrEmpty(sdkTags)
            .map(tag -> Tag
                    .builder()
                    .key(tag.key())
                    .value(tag.value())
                    .build()
            )
            .collect(Collectors.toList());
  }

  public static ResourceModel translateToModel(DBClusterSnapshot dbClusterSnapshotSnapshot) {
    return ResourceModel.builder()
            .dBClusterSnapshotIdentifier(dbClusterSnapshotSnapshot.dbClusterSnapshotIdentifier())
            .dBClusterIdentifier(dbClusterSnapshotSnapshot.dbClusterIdentifier())
            .dBClusterSnapshotArn(dbClusterSnapshotSnapshot.dbClusterSnapshotArn())
//            .engineVersion(dbSnapshot.engineVersion())
//            .optionGroupName(dbSnapshot.optionGroupName())
            .tags(translateToModel(dbClusterSnapshotSnapshot.tagList()))
            .build();
  }



  /////////////////////////////////

  /**
   * Request to create a resource
   * @param model resource model
   * @return awsRequest the aws service request to create a resource
   */
  static AwsRequest translateToCreateRequest(final ResourceModel model) {
    final AwsRequest awsRequest = null;
    // TODO: construct a request
    // e.g. https://github.com/aws-cloudformation/aws-cloudformation-resource-providers-logs/blob/2077c92299aeb9a68ae8f4418b5e932b12a8b186/aws-logs-loggroup/src/main/java/com/aws/logs/loggroup/Translator.java#L39-L43
    return awsRequest;
  }

  /**
   * Request to read a resource
   * @param model resource model
   * @return awsRequest the aws service request to describe a resource
   */
  static AwsRequest translateToReadRequest(final ResourceModel model) {
    final AwsRequest awsRequest = null;
    // TODO: construct a request
    // e.g. https://github.com/aws-cloudformation/aws-cloudformation-resource-providers-logs/blob/2077c92299aeb9a68ae8f4418b5e932b12a8b186/aws-logs-loggroup/src/main/java/com/aws/logs/loggroup/Translator.java#L20-L24
    return awsRequest;
  }

  /**
   * Translates resource object from sdk into a resource model
   * @param awsResponse the aws service describe resource response
   * @return model resource model
   */
  static ResourceModel translateFromReadResponse(final AwsResponse awsResponse) {
    // e.g. https://github.com/aws-cloudformation/aws-cloudformation-resource-providers-logs/blob/2077c92299aeb9a68ae8f4418b5e932b12a8b186/aws-logs-loggroup/src/main/java/com/aws/logs/loggroup/Translator.java#L58-L73
    return ResourceModel.builder()
        //.someProperty(response.property())
        .build();
  }

  /**
   * Request to delete a resource
   * @param model resource model
   * @return awsRequest the aws service request to delete a resource
   */
  static AwsRequest translateToDeleteRequest(final ResourceModel model) {
    final AwsRequest awsRequest = null;
    // TODO: construct a request
    // e.g. https://github.com/aws-cloudformation/aws-cloudformation-resource-providers-logs/blob/2077c92299aeb9a68ae8f4418b5e932b12a8b186/aws-logs-loggroup/src/main/java/com/aws/logs/loggroup/Translator.java#L33-L37
    return awsRequest;
  }

  /**
   * Request to update properties of a previously created resource
   * @param model resource model
   * @return awsRequest the aws service request to modify a resource
   */
  static AwsRequest translateToFirstUpdateRequest(final ResourceModel model) {
    final AwsRequest awsRequest = null;
    // TODO: construct a request
    // e.g. https://github.com/aws-cloudformation/aws-cloudformation-resource-providers-logs/blob/2077c92299aeb9a68ae8f4418b5e932b12a8b186/aws-logs-loggroup/src/main/java/com/aws/logs/loggroup/Translator.java#L45-L50
    return awsRequest;
  }

  /**
   * Request to update some other properties that could not be provisioned through first update request
   * @param model resource model
   * @return awsRequest the aws service request to modify a resource
   */
  static AwsRequest translateToSecondUpdateRequest(final ResourceModel model) {
    final AwsRequest awsRequest = null;
    // TODO: construct a request
    return awsRequest;
  }

  /**
   * Request to list resources
   * @param nextToken token passed to the aws service list resources request
   * @return awsRequest the aws service request to list resources within aws account
   */
  static AwsRequest translateToListRequest(final String nextToken) {
    final AwsRequest awsRequest = null;
    // TODO: construct a request
    // e.g. https://github.com/aws-cloudformation/aws-cloudformation-resource-providers-logs/blob/2077c92299aeb9a68ae8f4418b5e932b12a8b186/aws-logs-loggroup/src/main/java/com/aws/logs/loggroup/Translator.java#L26-L31
    return awsRequest;
  }

  /**
   * Translates resource objects from sdk into a resource model (primary identifier only)
   * @param awsResponse the aws service describe resource response
   * @return list of resource models
   */
  static List<ResourceModel> translateFromListRequest(final AwsResponse awsResponse) {
    // e.g. https://github.com/aws-cloudformation/aws-cloudformation-resource-providers-logs/blob/2077c92299aeb9a68ae8f4418b5e932b12a8b186/aws-logs-loggroup/src/main/java/com/aws/logs/loggroup/Translator.java#L75-L82
    return streamOfOrEmpty(Lists.newArrayList())
        .map(resource -> ResourceModel.builder()
            // include only primary identifier
            .build())
        .collect(Collectors.toList());
  }

  private static <T> Stream<T> streamOfOrEmpty(final Collection<T> collection) {
    return Optional.ofNullable(collection)
        .map(Collection::stream)
        .orElseGet(Stream::empty);
  }

  /**
   * Request to add tags to a resource
   * @param model resource model
   * @return awsRequest the aws service request to create a resource
   */
  static AwsRequest tagResourceRequest(final ResourceModel model, final Map<String, String> addedTags) {
    final AwsRequest awsRequest = null;
    // TODO: construct a request
    // e.g. https://github.com/aws-cloudformation/aws-cloudformation-resource-providers-logs/blob/2077c92299aeb9a68ae8f4418b5e932b12a8b186/aws-logs-loggroup/src/main/java/com/aws/logs/loggroup/Translator.java#L39-L43
    return awsRequest;
  }

  /**
   * Request to add tags to a resource
   * @param model resource model
   * @return awsRequest the aws service request to create a resource
   */
  static AwsRequest untagResourceRequest(final ResourceModel model, final Set<String> removedTags) {
    final AwsRequest awsRequest = null;
    // TODO: construct a request
    // e.g. https://github.com/aws-cloudformation/aws-cloudformation-resource-providers-logs/blob/2077c92299aeb9a68ae8f4418b5e932b12a8b186/aws-logs-loggroup/src/main/java/com/aws/logs/loggroup/Translator.java#L39-L43
    return awsRequest;
  }
}
