package software.amazon.rds.dbsubnetgroup;

import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.CreateDbSubnetGroupRequest;
import software.amazon.awssdk.services.rds.model.DeleteDbSubnetGroupRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbSubnetGroupsRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbSubnetGroupRequest;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceRequest;
import software.amazon.awssdk.services.rds.model.Tag;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class Translator {
    static CreateDbSubnetGroupRequest createDbSubnetGroupRequest(final ResourceModel model) {
        return CreateDbSubnetGroupRequest.builder()
                .dbSubnetGroupName(model.getDBSubnetGroupName())
                .dbSubnetGroupDescription(model.getDBSubnetGroupDescription())
                .subnetIds(model.getSubnetIds())
                .tags(translateTagsToSdk(model.getTags())).build();
    }

    static DescribeDbSubnetGroupsRequest describeDbSubnetGroupsRequest(final ResourceModel model) {
        return DescribeDbSubnetGroupsRequest.builder()
                .dbSubnetGroupName(model.getDBSubnetGroupName())
                .build();
    }

    static DescribeDbSubnetGroupsRequest describeDbSubnetGroupsRequest(final String nextToken) {
        return DescribeDbSubnetGroupsRequest.builder()
            .marker(nextToken)
            .build();
    }

    static ModifyDbSubnetGroupRequest modifyDbSubnetGroupRequest(final ResourceModel model) {
        return ModifyDbSubnetGroupRequest.builder()
                .dbSubnetGroupName(model.getDBSubnetGroupName())
                .dbSubnetGroupDescription(model.getDBSubnetGroupDescription())
                .subnetIds(model.getSubnetIds())
                .build();
    }

    static DeleteDbSubnetGroupRequest deleteDbSubnetGroupRequest(final ResourceModel model) {
        return DeleteDbSubnetGroupRequest.builder()
                .dbSubnetGroupName(model.getDBSubnetGroupName())
                .build();
    }

    static ListTagsForResourceRequest listTagsForResourceRequest(final String arn) {
        return ListTagsForResourceRequest.builder()
                .resourceName(arn)
                .build();
    }

  static AddTagsToResourceRequest addTagsToResourceRequest(
      final String dbClusterParameterGroupArn,
      final Set<software.amazon.rds.dbsubnetgroup.Tag> tags) {
        return AddTagsToResourceRequest.builder()
                .resourceName(dbClusterParameterGroupArn)
                .tags(translateTagsToSdk(tags))
                .build();
    }

  static RemoveTagsFromResourceRequest removeTagsFromResourceRequest(
      final String dbClusterParameterGroupArn,
      final Set<software.amazon.rds.dbsubnetgroup.Tag> tags) {
        return RemoveTagsFromResourceRequest.builder()
                .resourceName(dbClusterParameterGroupArn)
                .tagKeys(tags
                        .stream()
                        .map(software.amazon.rds.dbsubnetgroup.Tag::getKey)
                        .collect(Collectors.toSet()))
                .build();
    }

    static Set<Tag> translateTagsToSdk(final Collection<software.amazon.rds.dbsubnetgroup.Tag> tags) {
        return Optional.ofNullable(tags).orElse(Collections.emptySet())
                .stream()
                .map(tag -> Tag.builder().key(tag.getKey()).value(tag.getValue()).build())
                .collect(Collectors.toSet());
    }

    static Set<software.amazon.rds.dbsubnetgroup.Tag> translateTagsFromSdk(final Collection<Tag> tags) {
        return Optional.ofNullable(tags).orElse(Collections.emptySet())
                .stream()
                .map(tag -> software.amazon.rds.dbsubnetgroup.Tag.builder()
                    .key(tag.key())
                    .value(tag.value())
                    .build())
                .collect(Collectors.toSet());
    }
}
