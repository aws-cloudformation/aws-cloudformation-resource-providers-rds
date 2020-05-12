package software.amazon.rds.eventsubscription;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import software.amazon.awssdk.services.rds.model.AddSourceIdentifierToSubscriptionRequest;
import software.amazon.awssdk.services.rds.model.DeleteEventSubscriptionRequest;
import software.amazon.awssdk.services.rds.model.DescribeEventSubscriptionsRequest;
import software.amazon.awssdk.services.rds.model.ModifyEventSubscriptionRequest;
import software.amazon.awssdk.services.rds.model.RemoveSourceIdentifierFromSubscriptionRequest;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceRequest;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.CreateEventSubscriptionRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;

public class Translator {
  static CreateEventSubscriptionRequest createEventSubscriptionRequest(
      final ResourceModel model,
      final Map<String, String> tags) {
    return CreateEventSubscriptionRequest.builder()
        .subscriptionName(model.getSubscriptionName())
        .snsTopicArn(model.getSnsTopicArn())
        .sourceType(model.getSourceType())
        .eventCategories(model.getEventCategories())
        .sourceIds(model.getSourceIds())
        .enabled(model.getEnabled())
        .tags(translateTagsToSdk(tags))
        .build();
  }

  static DescribeEventSubscriptionsRequest describeEventSubscriptionsRequest(final ResourceModel model) {
    return DescribeEventSubscriptionsRequest.builder()
        .subscriptionName(model.getSubscriptionName())
        .build();
  }

  static DescribeEventSubscriptionsRequest describeEventSubscriptionsRequest(final String nextToken) {
    return DescribeEventSubscriptionsRequest.builder()
        .marker(nextToken)
        .build();
  }

  static DeleteEventSubscriptionRequest deleteEventSubscriptionRequest(final ResourceModel model) {
    return DeleteEventSubscriptionRequest.builder()
        .subscriptionName(model.getSubscriptionName())
        .build();
  }

  static ModifyEventSubscriptionRequest modifyEventSubscriptionRequest(final ResourceModel model) {
    return ModifyEventSubscriptionRequest.builder()
        .subscriptionName(model.getSubscriptionName())
        .snsTopicArn(model.getSnsTopicArn())
        .sourceType(model.getSourceType())
        .eventCategories(model.getEventCategories())
        .enabled(model.getEnabled())
        .build();
  }

  static RemoveSourceIdentifierFromSubscriptionRequest removeSourceIdentifierFromSubscriptionRequest(
      final ResourceModel model,
      final String sourceId
  ) {
    return RemoveSourceIdentifierFromSubscriptionRequest.builder()
        .subscriptionName(model.getSubscriptionName())
        .sourceIdentifier(sourceId)
        .build();
  }

  static AddSourceIdentifierToSubscriptionRequest addSourceIdentifierToSubscriptionRequest(
      final ResourceModel model,
      final String sourceId
  ) {
    return AddSourceIdentifierToSubscriptionRequest.builder()
        .subscriptionName(model.getSubscriptionName())
        .sourceIdentifier(sourceId)
        .build();
  }

  static ListTagsForResourceRequest listTagsForResourceRequest(final String dbClusterParameterGroupArn) {
    return ListTagsForResourceRequest.builder()
        .resourceName(dbClusterParameterGroupArn)
        .build();
  }

  static AddTagsToResourceRequest addTagsToResourceRequest(final String dbClusterParameterGroupArn,
      final Set<software.amazon.rds.eventsubscription.Tag> tags) {
    return AddTagsToResourceRequest.builder()
        .resourceName(dbClusterParameterGroupArn)
        .tags(translateTagsToSdk(tags))
        .build();
  }

  static RemoveTagsFromResourceRequest removeTagsFromResourceRequest(final String dbClusterParameterGroupArn,
      final Set<software.amazon.rds.eventsubscription.Tag> tags) {
    return RemoveTagsFromResourceRequest.builder()
        .resourceName(dbClusterParameterGroupArn)
        .tagKeys(tags
            .stream()
            .map(software.amazon.rds.eventsubscription.Tag::getKey)
            .collect(Collectors.toSet()))
        .build();
  }

  // Translate tags
  static Set<Tag> translateTagsToSdk(final Collection<software.amazon.rds.eventsubscription.Tag> tags) {
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

  static Set<software.amazon.rds.eventsubscription.Tag> mapToTags(final Map<String, String> tags) {
    if (tags == null) return Collections.emptySet();
    return Optional.of(tags.entrySet()).orElse(Collections.emptySet())
        .stream()
        .map(entry -> software.amazon.rds.eventsubscription.Tag.builder().key(entry.getKey()).value(entry.getValue()).build())
        .collect(Collectors.toSet());
  }

  static Set<software.amazon.rds.eventsubscription.Tag> translateTagsFromSdk(final Collection<Tag> tags) {
    return Optional.ofNullable(tags).orElse(Collections.emptySet())
        .stream()
        .map(tag -> software.amazon.rds.eventsubscription.Tag.builder()
            .key(tag.key())
            .value(tag.value()).build())
        .collect(Collectors.toSet());
  }
}
