package software.amazon.rds.dbsubnetgroup;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import software.amazon.awssdk.services.rds.model.CreateDbSubnetGroupRequest;
import software.amazon.awssdk.services.rds.model.DeleteDbSubnetGroupRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbSubnetGroupsRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbSubnetGroupRequest;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.rds.common.handler.Tagging;

public class Translator {
    static CreateDbSubnetGroupRequest createDbSubnetGroupRequest(final ResourceModel model,
                                                                 final Map<String, String> tags) {
        return CreateDbSubnetGroupRequest.builder()
                .dbSubnetGroupName(model.getDBSubnetGroupName())
                .dbSubnetGroupDescription(model.getDBSubnetGroupDescription())
                .subnetIds(model.getSubnetIds())
                .tags(Tagging.translateTagsToSdk(tags)).build();
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

    static Set<software.amazon.rds.dbsubnetgroup.Tag> translateTagsFromSdk(final Collection<Tag> tags) {
        return Optional.ofNullable(tags).orElse(Collections.emptySet())
                .stream()
                .map(tag -> software.amazon.rds.dbsubnetgroup.Tag.builder()
                        .key(tag.key())
                        .value(tag.value()).build())
                .collect(Collectors.toSet());
    }
}
