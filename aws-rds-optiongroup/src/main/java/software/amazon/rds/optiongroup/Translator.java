package software.amazon.rds.optiongroup;

import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.CreateOptionGroupRequest;
import software.amazon.awssdk.services.rds.model.DBSecurityGroupMembership;
import software.amazon.awssdk.services.rds.model.DeleteOptionGroupRequest;
import software.amazon.awssdk.services.rds.model.DescribeOptionGroupsRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ModifyOptionGroupRequest;
import software.amazon.awssdk.services.rds.model.Option;
import software.amazon.awssdk.services.rds.model.OptionConfiguration;
import software.amazon.awssdk.services.rds.model.OptionSetting;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceRequest;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.awssdk.services.rds.model.VpcSecurityGroupMembership;

public class Translator {

  // request to create a resource
  static CreateOptionGroupRequest createOptionGroupRequest(final ResourceModel model, final Map<String, String> tags) {
    return CreateOptionGroupRequest.builder()
        .engineName(model.getEngineName())
        .majorEngineVersion(model.getMajorEngineVersion())
        .optionGroupDescription(model.getOptionGroupDescription())
        .optionGroupName(model.getId())
        .tags(translateTagsToSdk(tags))
        .build();
  }

  // request to read a resource
  static DescribeOptionGroupsRequest describeOptionGroupsRequest(final ResourceModel model) {
    return DescribeOptionGroupsRequest.builder()
        .optionGroupName(model.getId())
        .build();
  }

  static DescribeOptionGroupsRequest describeOptionGroupsRequest(final String nextToken) {
    return DescribeOptionGroupsRequest.builder()
        .marker(nextToken)
        .build();
  }

  // request to update properties of a previously created resource
  static ModifyOptionGroupRequest modifyOptionGroupRequest(final ResourceModel model) {
    return ModifyOptionGroupRequest.builder()
        .applyImmediately(true)
        .optionGroupName(model.getId())
        .optionsToInclude(translateOptionConfigurationToSdk(model.getOptionConfigurations()))
        .build();
  }

  // request to update properties of a previously created resource
  static ModifyOptionGroupRequest modifyOptionGroupRequest(
      final ResourceModel model,
      final Set<String> optionsToRemove) {
    final Set<String> optionsToAdd = getOptionNames(model.getOptionConfigurations());
    return ModifyOptionGroupRequest.builder()
        .applyImmediately(true)
        .optionGroupName(model.getId())
        .optionsToInclude(translateOptionConfigurationToSdk(model.getOptionConfigurations()))
        .optionsToRemove(Sets.difference(optionsToRemove, optionsToAdd))
        .build();
  }

  static Set<String> getOptionNames(final Set<software.amazon.rds.optiongroup.OptionConfiguration> optionConfigurations) {
    return Optional.ofNullable(optionConfigurations).orElse(Collections.emptySet()).stream()
        .map(software.amazon.rds.optiongroup.OptionConfiguration::getOptionName)
        .collect(Collectors.toSet());
  }

  // request to delete a resource
  static DeleteOptionGroupRequest deleteOptionGroupRequest(final ResourceModel model) {
    return DeleteOptionGroupRequest.builder()
        .optionGroupName(model.getId())
        .build();
  }

  static ListTagsForResourceRequest listTagsForResourceRequest(final String arn) {
    return ListTagsForResourceRequest.builder()
        .resourceName(arn)
        .build();
  }

  static AddTagsToResourceRequest addTagsToResourceRequest(
      final String optionGroupArn,
      final Set<software.amazon.rds.optiongroup.Tag> tags) {
    return AddTagsToResourceRequest.builder()
        .resourceName(optionGroupArn)
        .tags(translateTagsToSdk(tags))
        .build();
  }

  static RemoveTagsFromResourceRequest removeTagsFromResourceRequest(
      final String optionGroupArn,
      final Set<software.amazon.rds.optiongroup.Tag> tags) {
    return RemoveTagsFromResourceRequest.builder()
        .resourceName(optionGroupArn)
        .tagKeys(tags
            .stream()
            .map(software.amazon.rds.optiongroup.Tag::getKey)
            .collect(Collectors.toSet()))
        .build();
  }

  static Set<OptionConfiguration> translateOptionConfigurationToSdk(
      final Set<software.amazon.rds.optiongroup.OptionConfiguration> optionConfigurations) {
    if (optionConfigurations == null) return null;
    return optionConfigurations.stream()
        .map(optionConfiguration ->
            OptionConfiguration.builder()
            .dbSecurityGroupMemberships(optionConfiguration.getDBSecurityGroupMemberships())
            .optionName(optionConfiguration.getOptionName())
            .optionSettings(translateOptionSettingToSdk(optionConfiguration.getOptionSettings()))
            .optionVersion(optionConfiguration.getOptionVersion())
            .port(optionConfiguration.getPort())
            .build()
        ).collect(Collectors.toSet());
  }

  static Set<OptionSetting> translateOptionSettingToSdk(final Set<software.amazon.rds.optiongroup.OptionSetting> optionSettings) {
    if (optionSettings == null) return null;
    return optionSettings.stream()
        .map(optionSetting -> OptionSetting.builder()
            .name(optionSetting.getName())
            .value(optionSetting.getValue())
            .build())
        .collect(Collectors.toSet());
  }


  static Set<software.amazon.rds.optiongroup.OptionConfiguration> translateOptionConfigurationFromSdk(final Collection<Option> options) {
    return Optional.ofNullable(options).orElse(Collections.emptySet()).stream()
        .map(option ->
            software.amazon.rds.optiongroup.OptionConfiguration.builder()
                .dBSecurityGroupMemberships(
                    option.dbSecurityGroupMemberships()
                        .stream()
                        .map(DBSecurityGroupMembership::dbSecurityGroupName)
                        .collect(Collectors.toList()))
                .optionName(option.optionName())
                .optionVersion(option.optionVersion())
                .port(option.port())
                .vpcSecurityGroupMemberships(
                    option.vpcSecurityGroupMemberships()
                        .stream()
                        .map(VpcSecurityGroupMembership::vpcSecurityGroupId)
                        .collect(Collectors.toList()))
                .optionSettings(translateOptionSettingFromSdk(option.optionSettings()))
                .build())
        .collect(Collectors.toSet());
  }

  static Set<software.amazon.rds.optiongroup.OptionSetting> translateOptionSettingFromSdk(final Collection<OptionSetting> optionSettings) {
    return Optional.ofNullable(optionSettings).orElse(Collections.emptySet()).stream()
        .map(optionSetting ->
            software.amazon.rds.optiongroup.OptionSetting.builder()
                .name(optionSetting.name())
                .value(optionSetting.value())
                .build())
        .collect(Collectors.toSet());
  }

  // Translate tags
  static Set<Tag> translateTagsToSdk(final Collection<software.amazon.rds.optiongroup.Tag> tags) {
    return Optional.ofNullable(tags).orElse(Collections.emptySet())
        .stream()
        .map(tag -> Tag.builder().key(tag.getKey()).value(tag.getValue()).build())
        .collect(Collectors.toSet());
  }

  // Translate tags
  static Set<Tag> translateTagsToSdk(final Map<String, String> tags) {
    if (tags == null) return Collections.emptySet();
    return Optional.of(tags.entrySet()).orElse(Collections.emptySet())
        .stream()
        .map(tag -> Tag.builder().key(tag.getKey()).value(tag.getValue()).build())
        .collect(Collectors.toSet());
  }

  static Set<software.amazon.rds.optiongroup.Tag> mapToTags(final Map<String, String> tags) {
    if (tags == null) return Collections.emptySet();
    return Optional.of(tags.entrySet()).orElse(Collections.emptySet())
        .stream()
        .map(entry -> software.amazon.rds.optiongroup.Tag.builder().key(entry.getKey()).value(entry.getValue()).build())
        .collect(Collectors.toSet());
  }

  static Set<software.amazon.rds.optiongroup.Tag> translateTagsFromSdk(final Collection<Tag> tags) {
    return Optional.ofNullable(tags).orElse(Collections.emptySet())
        .stream()
        .map(tag -> software.amazon.rds.optiongroup.Tag.builder()
            .key(tag.key())
            .value(tag.value()).build())
        .collect(Collectors.toSet());
  }
}
