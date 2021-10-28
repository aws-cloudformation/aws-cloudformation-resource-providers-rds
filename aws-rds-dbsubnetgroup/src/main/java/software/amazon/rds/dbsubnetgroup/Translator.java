package software.amazon.rds.dbsubnetgroup;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.amazonaws.util.CollectionUtils;
import software.amazon.awssdk.services.rds.model.CreateDbSubnetGroupRequest;
import software.amazon.awssdk.services.rds.model.DBSubnetGroup;
import software.amazon.awssdk.services.rds.model.DeleteDbSubnetGroupRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbSubnetGroupsRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbSubnetGroupRequest;
import software.amazon.awssdk.services.rds.model.Subnet;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.rds.common.handler.Tagging;

public class Translator {
    static CreateDbSubnetGroupRequest createDbSubnetGroupRequest(
            final ResourceModel model,
            final Map<String, String> tags
    ) {
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

    static ProgressEvent<ResourceModel, CallbackContext> translateToModel(
            final DBSubnetGroup dbSubnetGroup,
            final Collection<software.amazon.awssdk.services.rds.model.Tag> rdsTags
    ) {
        List<Tag> tags = CollectionUtils.isNullOrEmpty(rdsTags) ? null
                : rdsTags.stream()
                .map(tag -> Tag.builder()
                        .key(tag.key())
                        .value(tag.value())
                        .build())
                .collect(Collectors.toList());

        return ProgressEvent.defaultSuccessHandler(ResourceModel.builder()
                .dBSubnetGroupName(dbSubnetGroup.dbSubnetGroupName())
                .dBSubnetGroupDescription(dbSubnetGroup.dbSubnetGroupDescription())
                .subnetIds(dbSubnetGroup.subnets().stream().map(Subnet::subnetIdentifier).collect(Collectors.toList()))
                .tags(tags)
                .build());
    }
}
