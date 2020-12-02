package software.amazon.rds.dbsecuritygroup;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.AuthorizeDbSecurityGroupIngressRequest;
import software.amazon.awssdk.services.rds.model.CreateDbSecurityGroupRequest;
import software.amazon.awssdk.services.rds.model.DeleteDbSecurityGroupRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbSecurityGroupsRequest;
import software.amazon.awssdk.services.rds.model.IPRange;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceRequest;
import software.amazon.awssdk.services.rds.model.RevokeDbSecurityGroupIngressRequest;

public class Translator {

    public static DescribeDbSecurityGroupsRequest describeDBSecurityGroupsRequest(final ResourceModel model) {
        return DescribeDbSecurityGroupsRequest.builder()
                .dbSecurityGroupName(model.getGroupName())
                .build();
    }

    public static DescribeDbSecurityGroupsRequest describeDBSecurityGroupsRequest(final String nextToken) {
        return DescribeDbSecurityGroupsRequest.builder()
                .marker(nextToken)
                .build();
    }

    public static ListTagsForResourceRequest listTagsForResourceRequest(final String arn) {
        return ListTagsForResourceRequest.builder()
                .resourceName(arn)
                .build();
    }

    public static AddTagsToResourceRequest addTagsToResourceRequest(final String arn, final Collection<Tag> tags) {
        return AddTagsToResourceRequest.builder()
                .resourceName(arn)
                .tags(translateTagsToSdk(tags))
                .build();
    }

    public static RemoveTagsFromResourceRequest removeTagsFromResourceRequest(final String arn, final Collection<Tag> tags) {
        return RemoveTagsFromResourceRequest.builder()
                .resourceName(arn)
                .tagKeys(tags.stream().map(Tag::getKey).collect(Collectors.toSet()))
                .build();
    }

    public static AuthorizeDbSecurityGroupIngressRequest authorizeDBSecurityGroupIngresRequest(ResourceModel model, Ingress ingres) {
        return AuthorizeDbSecurityGroupIngressRequest.builder()
                .dbSecurityGroupName(model.getGroupName())
                .ec2SecurityGroupId(ingres.getEC2SecurityGroupId())
                .ec2SecurityGroupName(ingres.getEC2SecurityGroupName())
                .ec2SecurityGroupOwnerId(ingres.getEC2SecurityGroupOwnerId())
                .cidrip(ingres.getCIDRIP())
                .build();
    }

    public static RevokeDbSecurityGroupIngressRequest revokeDBSecurityGroupIngresRequest(ResourceModel model, Ingress ingres) {
        return RevokeDbSecurityGroupIngressRequest.builder()
                .dbSecurityGroupName(model.getGroupName())
                .ec2SecurityGroupId(ingres.getEC2SecurityGroupId())
                .ec2SecurityGroupName(ingres.getEC2SecurityGroupName())
                .ec2SecurityGroupOwnerId(ingres.getEC2SecurityGroupOwnerId())
                .build();
    }

    public static DeleteDbSecurityGroupRequest deleteDBSecurityGroupRequest(final ResourceModel model) {
        return DeleteDbSecurityGroupRequest.builder()
                .dbSecurityGroupName(model.getGroupName())
                .build();
    }

    public static CreateDbSecurityGroupRequest createDBSecurityGroupRequest(final ResourceModel model, final Collection<Tag> tags) {
        return CreateDbSecurityGroupRequest.builder()
                .dbSecurityGroupName(model.getGroupName())
                .dbSecurityGroupDescription(model.getGroupDescription())
                .tags(Translator.translateTagsToSdk(tags))
                .build();
    }

    public static ResourceModel translateDBSecurityGroupFromSdk(
            final software.amazon.awssdk.services.rds.model.DBSecurityGroup dbSecurityGroup,
            final List<Tag> tags
    ) {
        final String ipRange = streamOfOrEmpty(dbSecurityGroup.ipRanges()).map(IPRange::cidrip).findFirst().orElse(null);
        return ResourceModel.builder()
                .dBSecurityGroupIngress(
                        streamOfOrEmpty(dbSecurityGroup.ec2SecurityGroups())
                                .map(Translator::translateDBSecurityGroupIngressFromSdk)
                                .peek(ingres -> ingres.setCIDRIP(ipRange))
                                .collect(Collectors.toList())
                )
                .eC2VpcId(dbSecurityGroup.vpcId())
                .groupDescription(dbSecurityGroup.dbSecurityGroupDescription())
                .groupName(dbSecurityGroup.dbSecurityGroupName())
                .tags(tags)
                .build();
    }

    public static List<Tag> translateTagsFromSdk(final Collection<software.amazon.awssdk.services.rds.model.Tag> tags) {
        return streamOfOrEmpty(tags)
                .map(tag -> Tag.builder()
                        .key(tag.key())
                        .value(tag.value())
                        .build())
                .collect(Collectors.toList());
    }

    public static Collection<Tag> translateTagsFromRequest(final Map<String, String> tags) {
        return Optional.ofNullable(tags).orElse(Collections.emptyMap())
                .entrySet()
                .stream()
                .map(entrySet -> Tag.builder()
                        .key(entrySet.getKey())
                        .value(entrySet.getValue())
                        .build()
                )
                .collect(Collectors.toList());
    }

    public static Map<String, String> translateTagsToRequest(Collection<Tag> tags) {
        return Optional.ofNullable(tags).orElse(Collections.emptySet())
                .stream()
                .collect(Collectors.toMap(Tag::getKey, Tag::getValue));
    }

    public static Collection<software.amazon.awssdk.services.rds.model.Tag> translateTagsToSdk(final Collection<Tag> tags) {
        return streamOfOrEmpty(tags)
                .map(tag -> software.amazon.awssdk.services.rds.model.Tag.builder()
                        .key(tag.getKey())
                        .value(tag.getValue())
                        .build()
                )
                .collect(Collectors.toList());
    }

    public static Ingress translateDBSecurityGroupIngressFromSdk(final software.amazon.awssdk.services.rds.model.EC2SecurityGroup ec2SecurityGroup) {
        return Ingress.builder()
                .eC2SecurityGroupId(ec2SecurityGroup.ec2SecurityGroupId())
                .eC2SecurityGroupName(ec2SecurityGroup.ec2SecurityGroupName())
                .eC2SecurityGroupOwnerId(ec2SecurityGroup.ec2SecurityGroupOwnerId())
                .build();
    }

    public static software.amazon.awssdk.services.rds.model.EC2SecurityGroup translateIngresToEc2SecurityGroup(final Ingress ingres) {
        return software.amazon.awssdk.services.rds.model.EC2SecurityGroup.builder()
                .ec2SecurityGroupId(ingres.getEC2SecurityGroupId())
                .ec2SecurityGroupName(ingres.getEC2SecurityGroupName())
                .ec2SecurityGroupOwnerId(ingres.getEC2SecurityGroupOwnerId())
                .build();
    }

    private static <T> Stream<T> streamOfOrEmpty(final Collection<T> collection) {
        return Optional.ofNullable(collection)
                .map(Collection::stream)
                .orElseGet(Stream::empty);
    }
}
