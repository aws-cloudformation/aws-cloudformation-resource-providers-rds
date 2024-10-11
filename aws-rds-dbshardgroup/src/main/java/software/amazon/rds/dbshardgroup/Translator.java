package software.amazon.rds.dbshardgroup;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.Optional;
import java.util.stream.Collectors;

import software.amazon.awssdk.services.rds.model.CreateDbShardGroupRequest;
import software.amazon.awssdk.services.rds.model.DBShardGroup;
import software.amazon.awssdk.services.rds.model.DeleteDbShardGroupRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbShardGroupsRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbShardGroupRequest;
import software.amazon.rds.common.handler.Tagging;

public class Translator {

  static CreateDbShardGroupRequest createDbShardGroupRequest(final ResourceModel model, final Tagging.TagSet tags) {
    return CreateDbShardGroupRequest.builder()
            .computeRedundancy(model.getComputeRedundancy())
            .dbClusterIdentifier(model.getDBClusterIdentifier())
            .dbShardGroupIdentifier(model.getDBShardGroupIdentifier())
            .maxACU(model.getMaxACU())
            .minACU(model.getMinACU())
            .publiclyAccessible(model.getPubliclyAccessible())
            .tags(Tagging.translateTagsToSdk(tags))
            .build();
  }

  static ModifyDbShardGroupRequest modifyDbShardGroupRequest(final ResourceModel desiredModel) {
    return ModifyDbShardGroupRequest.builder()
            .computeRedundancy(desiredModel.getComputeRedundancy())
            .dbShardGroupIdentifier(desiredModel.getDBShardGroupIdentifier())
            .maxACU(desiredModel.getMaxACU())
            .minACU(desiredModel.getMinACU())
            .build();
  }

  static DescribeDbShardGroupsRequest describeDbShardGroupsRequest(final ResourceModel model) {
    return DescribeDbShardGroupsRequest.builder()
            .dbShardGroupIdentifier(model.getDBShardGroupIdentifier())
            .build();
  }

  static DescribeDbClustersRequest describeDbClustersRequest(final ResourceModel model) {
    return DescribeDbClustersRequest.builder()
            .dbClusterIdentifier(model.getDBClusterIdentifier())
            .build();
  }

  static DescribeDbShardGroupsRequest describeDbShardGroupsRequest(final String nextToken) {
    return DescribeDbShardGroupsRequest.builder()
            .marker(nextToken)
            .build();
  }

  static DeleteDbShardGroupRequest deleteDbShardGroupRequest(final ResourceModel model) {
    return DeleteDbShardGroupRequest.builder()
            .dbShardGroupIdentifier(model.getDBShardGroupIdentifier())
            .build();
  }

  public static Map<String, String> translateTagsToRequest(final Collection<Tag> tags) {
    return Optional.ofNullable(tags).orElse(Collections.emptyList())
            .stream()
            .collect(Collectors.toMap(Tag::getKey, Tag::getValue));
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
            .map(tag -> software.amazon.rds.dbshardgroup.Tag.builder()
                    .key(tag.key())
                    .value(tag.value())
                    .build())
            .collect(Collectors.toSet());
  }

  static ResourceModel translateDbShardGroupFromSdk(final DBShardGroup dbShardGroup, final Set<software.amazon.awssdk.services.rds.model.Tag> tags) {
    return ResourceModel.builder()
            .dBShardGroupResourceId(dbShardGroup.dbShardGroupResourceId())
            .dBShardGroupIdentifier(dbShardGroup.dbShardGroupIdentifier())
            .dBClusterIdentifier(dbShardGroup.dbClusterIdentifier())
            .computeRedundancy(dbShardGroup.computeRedundancy())
            .maxACU(dbShardGroup.maxACU())
            // TODO: Return minACU when describeDBShardGroup includes value
//            .minACU(dbShardGroup.minACU())
            .publiclyAccessible(dbShardGroup.publiclyAccessible())
            .tags(translateTagsFromSdk(tags))
            .endpoint(dbShardGroup.endpoint())
            .build();
  }
}
