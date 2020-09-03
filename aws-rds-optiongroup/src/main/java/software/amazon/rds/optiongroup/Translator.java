package software.amazon.rds.optiongroup;

import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.CreateOptionGroupRequest;
import software.amazon.awssdk.services.rds.model.DeleteOptionGroupRequest;
import software.amazon.awssdk.services.rds.model.DescribeOptionGroupsRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ModifyOptionGroupRequest;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceRequest;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class is a centralized placeholder for
 * - api request construction
 * - object translation to/from aws sdk
 * - resource model construction for read/list handlers
 */

public class Translator {

    static CreateOptionGroupRequest createOptionGroupRequest(final ResourceModel model, final Map<String, String> tags) {
        return CreateOptionGroupRequest.builder()
                .optionGroupName(model.getOptionGroupName())
                .engineName(model.getEngineName())
                .majorEngineVersion(model.getMajorEngineVersion())
                .optionGroupDescription(model.getOptionGroupDescription())
                .tags(translateTagsToSdk(tags))
                .build();
    }

    static DescribeOptionGroupsRequest describeOptionGroupsRequest(final String nextToken) {
        return DescribeOptionGroupsRequest.builder()
                .marker(nextToken)
                .build();
    }

    static DescribeOptionGroupsRequest describeOptionGroupsRequest(final ResourceModel model) {
        return DescribeOptionGroupsRequest.builder()
                .optionGroupName(model.getOptionGroupName())
                .build();
    }

    static ModifyOptionGroupRequest modifyOptionGroupRequest(final ResourceModel model, Collection<OptionConfiguration> optionsToInclude, Collection<OptionConfiguration> optionsToRemove) {
        final Collection<software.amazon.awssdk.services.rds.model.OptionConfiguration> optionsToIncludeSdk = translateOptionConfigurationsToSdk(optionsToInclude);
        final Collection<String> optionNamesToRemove = optionsToRemove.stream().map(opt -> opt.getOptionName()).collect(Collectors.toList());

        return ModifyOptionGroupRequest.builder()
                .optionGroupName(model.getOptionGroupName())
                .optionsToInclude(optionsToIncludeSdk)
                .optionsToRemove(optionNamesToRemove)
                .applyImmediately(Boolean.TRUE)
                .build();
    }

    public static DeleteOptionGroupRequest deleteOptionGroupRequest(final ResourceModel model) {
        return DeleteOptionGroupRequest.builder()
                .optionGroupName(model.getOptionGroupName())
                .build();
    }

    static Set<OptionConfiguration> translateOptionConfigurationsFromSdk(final Collection<software.amazon.awssdk.services.rds.model.Option> options) {
        return Optional.ofNullable(options).orElse(Collections.emptyList())
                .stream()
                .map(option -> {
                    return OptionConfiguration.builder()
                            .dBSecurityGroupMemberships(translateDBSecurityMembershipsFromSdk(option.dbSecurityGroupMemberships()))
                            .optionName(option.optionName())
                            .optionSettings(translateOptionSettingsFromSdk(option.optionSettings()))
                            .optionVersion(option.optionVersion())
                            .port(option.port())
                            .vpcSecurityGroupMemberships(translateVpcSecurityGroupMembershipsFromSdk(option.vpcSecurityGroupMemberships()))
                            .build();
                })
                .collect(Collectors.toSet());
    }

    static List<software.amazon.awssdk.services.rds.model.OptionConfiguration> translateOptionConfigurationsToSdk(final Collection<OptionConfiguration> options) {
        return Optional.ofNullable(options).orElse(Collections.emptyList())
                .stream()
                .map(option -> {
                    return software.amazon.awssdk.services.rds.model.OptionConfiguration
                            .builder()
                            .optionName(option.getOptionName())
                            .dbSecurityGroupMemberships(translateDBSecurityMembershipsToSdk(option.getDBSecurityGroupMemberships()))
                            .optionSettings(translateOptionSettingsToSdk(option.getOptionSettings()))
                            .optionVersion(option.getOptionVersion())
                            .port(option.getPort())
                            .vpcSecurityGroupMemberships(translateVpcSecurityGroupMembershipsToSdk(option.getVpcSecurityGroupMemberships()))
                            .build();
                })
                .collect(Collectors.toList());
    }

    static Set<OptionSetting> translateOptionSettingsFromSdk(final Collection<software.amazon.awssdk.services.rds.model.OptionSetting> optionSettings) {
        return Optional.ofNullable(optionSettings).orElse(Collections.emptyList())
                .stream()
                .map(optionSetting -> {
                    return OptionSetting.builder()
                            .name(optionSetting.name())
                            .value(optionSetting.value())
                            .build();
                })
                .collect(Collectors.toSet());
    }

    static List<software.amazon.awssdk.services.rds.model.OptionSetting> translateOptionSettingsToSdk(final Collection<OptionSetting> optionSettings) {
        return Optional.ofNullable(optionSettings).orElse(Collections.emptyList())
                .stream()
                .map(optionSetting -> {
                    return software.amazon.awssdk.services.rds.model.OptionSetting
                            .builder()
                            .name(optionSetting.getName())
                            .value(optionSetting.getValue())
                            .build();
                })
                .collect(Collectors.toList());
    }

    static Set<DBSecurityGroupMembership> translateDBSecurityMembershipsFromSdk(final Collection<software.amazon.awssdk.services.rds.model.DBSecurityGroupMembership> dbSecurityGroupMemberships) {
        return Optional.ofNullable(dbSecurityGroupMemberships).orElse(Collections.emptyList())
                .stream()
                .map(dbSecurityGroupMembership -> {
                    return DBSecurityGroupMembership.builder()
                            .dBSecurityGroupName(dbSecurityGroupMembership.dbSecurityGroupName())
                            .status(dbSecurityGroupMembership.status())
                            .build();
                })
                .collect(Collectors.toSet());
    }

    static List<String> translateDBSecurityMembershipsToSdk(final Collection<DBSecurityGroupMembership> dbSecurityGroupMemberships) {
        return Optional.ofNullable(dbSecurityGroupMemberships).orElse(Collections.emptyList())
                .stream()
                .map(membership -> membership.getDBSecurityGroupName())
                .collect(Collectors.toList());
    }

    static Set<VpcSecurityGroupMembership> translateVpcSecurityGroupMembershipsFromSdk(final Collection<software.amazon.awssdk.services.rds.model.VpcSecurityGroupMembership> vpsSecurityGroupMemberships) {
        return Optional.ofNullable(vpsSecurityGroupMemberships).orElse(Collections.emptyList())
                .stream()
                .map(vpcSecurityGroupMembership -> {
                    return VpcSecurityGroupMembership.builder()
                            .vpcSecurityGroupId(vpcSecurityGroupMembership.vpcSecurityGroupId())
                            .status(vpcSecurityGroupMembership.status())
                            .build();
                })
                .collect(Collectors.toSet());
    }

    static List<String> translateVpcSecurityGroupMembershipsToSdk(final Collection<VpcSecurityGroupMembership> vpcSecurityGroupMemberships) {
        return Optional.ofNullable(vpcSecurityGroupMemberships).orElse(Collections.emptyList())
                .stream()
                .map(membership -> membership.getVpcSecurityGroupId())
                .collect(Collectors.toList());
    }

    static Set<software.amazon.awssdk.services.rds.model.Tag>translateTagsToSdk(final Collection<Tag> tags) {
        return Optional.ofNullable(tags).orElse(Collections.emptySet())
                .stream()
                .map(tag -> {
                    return software.amazon.awssdk.services.rds.model.Tag.builder()
                            .key(tag.getKey())
                            .value(tag.getValue())
                            .build();
                })
                .collect(Collectors.toSet());
    }

    static Set<software.amazon.awssdk.services.rds.model.Tag> translateTagsToSdk(final Map<String, String> tags) {
        return Optional.of(tags.entrySet()).orElse(Collections.emptySet())
                .stream()
                .map(tag -> {
                    return software.amazon.awssdk.services.rds.model.Tag.builder()
                            .key(tag.getKey())
                            .value(tag.getValue())
                            .build();
                })
                .collect(Collectors.toSet());
    }

    static Set<Tag> translateTagsFromSdk(final Collection<software.amazon.awssdk.services.rds.model.Tag> tags) {
        return Optional.ofNullable(tags).orElse(Collections.emptySet())
                .stream()
                .map(tag -> {
                    return Tag.builder()
                            .key(tag.key())
                            .value(tag.value())
                            .build();
                }).collect(Collectors.toSet());
    }

    static AddTagsToResourceRequest addTagsToResourceRequest(final String arn, final Set<Tag> tags) {
        return AddTagsToResourceRequest.builder()
                .resourceName(arn)
                .tags(translateTagsToSdk(tags))
                .build();
    }

    static RemoveTagsFromResourceRequest removeTagsFromResourceRequest(final String arn, final Set<Tag> tags) {
        return RemoveTagsFromResourceRequest.builder()
                .resourceName(arn)
                .tagKeys(tags.stream().map(Tag::getKey).collect(Collectors.toSet()))
                .build();
    }

    static ListTagsForResourceRequest listTagsForResourceRequest(final String arn) {
        return ListTagsForResourceRequest.builder()
                .resourceName(arn)
                .build();
    }
}
