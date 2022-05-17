package software.amazon.rds.dbsubnetgroup;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.amazonaws.arn.Arn;
import com.amazonaws.util.CollectionUtils;
import software.amazon.awssdk.services.rds.model.CreateDbSubnetGroupRequest;
import software.amazon.awssdk.services.rds.model.DBSubnetGroup;
import software.amazon.awssdk.services.rds.model.DeleteDbSubnetGroupRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbSubnetGroupsRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbSubnetGroupRequest;
import software.amazon.awssdk.services.rds.model.Subnet;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Tagging;

public class Translator {
    static final String RDS = "rds";
    static final String RESOURCE_PREFIX = "subgrp:";

    static CreateDbSubnetGroupRequest createDbSubnetGroupRequest(
            final ResourceModel model,
            final Tagging.TagSet tags
    ) {
        return CreateDbSubnetGroupRequest.builder()
                .dbSubnetGroupName(model.getDBSubnetGroupName())
                .dbSubnetGroupDescription(model.getDBSubnetGroupDescription())
                .subnetIds(model.getSubnetIds().stream().sorted().collect(Collectors.toCollection(LinkedHashSet::new)))
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
                .subnetIds(model.getSubnetIds().stream().sorted().collect(Collectors.toCollection(LinkedHashSet::new)))
                .build();
    }

    static DeleteDbSubnetGroupRequest deleteDbSubnetGroupRequest(final ResourceModel model) {
        return DeleteDbSubnetGroupRequest.builder()
                .dbSubnetGroupName(model.getDBSubnetGroupName())
                .build();
    }

    static ResourceModel translateToModel(final DBSubnetGroup dbSubnetGroup) {
        return ResourceModel.builder()
                .dBSubnetGroupName(dbSubnetGroup.dbSubnetGroupName())
                .dBSubnetGroupDescription(dbSubnetGroup.dbSubnetGroupDescription())
                .subnetIds(dbSubnetGroup.subnets().stream().map(Subnet::subnetIdentifier).collect(Collectors.toList()))
                .build();
    }

    static Arn buildParameterGroupArn(final ResourceHandlerRequest<ResourceModel> request) {
        String resource = RESOURCE_PREFIX + request.getDesiredResourceState().getDBSubnetGroupName();
        return Arn.builder()
                .withPartition(request.getAwsPartition())
                .withRegion(request.getRegion())
                .withService(RDS)
                .withAccountId(request.getAwsAccountId())
                .withResource(resource)
                .build();
    }

    static List<software.amazon.awssdk.services.rds.model.Tag> translateTagsToSdk(final Collection<Tag> tags) {
        return Optional.ofNullable(tags).orElse(Collections.emptyList())
                .stream()
                .map(tag -> software.amazon.awssdk.services.rds.model.Tag.builder()
                        .key(tag.getKey())
                        .value(tag.getValue())
                        .build()
                )
                .collect(Collectors.toList());
    }

    static List<Tag> translateTags(final Collection<software.amazon.awssdk.services.rds.model.Tag> rdsTags) {
        return CollectionUtils.isNullOrEmpty(rdsTags) ? null
                : rdsTags.stream()
                .map(tag -> Tag.builder()
                        .key(tag.key())
                        .value(tag.value())
                        .build())
                .collect(Collectors.toList());
    }
}
