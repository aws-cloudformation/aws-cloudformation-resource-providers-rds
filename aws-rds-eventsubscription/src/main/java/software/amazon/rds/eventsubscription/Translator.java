package software.amazon.rds.eventsubscription;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.amazonaws.util.CollectionUtils;
import software.amazon.awssdk.services.rds.model.AddSourceIdentifierToSubscriptionRequest;
import software.amazon.awssdk.services.rds.model.CreateEventSubscriptionRequest;
import software.amazon.awssdk.services.rds.model.DeleteEventSubscriptionRequest;
import software.amazon.awssdk.services.rds.model.DescribeEventSubscriptionsRequest;
import software.amazon.awssdk.services.rds.model.EventSubscription;
import software.amazon.awssdk.services.rds.model.ModifyEventSubscriptionRequest;
import software.amazon.awssdk.services.rds.model.RemoveSourceIdentifierFromSubscriptionRequest;
import software.amazon.rds.common.handler.Tagging;

public class Translator {
    static CreateEventSubscriptionRequest createEventSubscriptionRequest(
            final ResourceModel model,
            final Tagging.TagSet tags
    ) {
        return CreateEventSubscriptionRequest.builder()
                .subscriptionName(model.getSubscriptionName())
                .snsTopicArn(model.getSnsTopicArn())
                .sourceType(model.getSourceType())
                .eventCategories(model.getEventCategories())
                .sourceIds(model.getSourceIds())
                .enabled(model.getEnabled())
                .tags(Tagging.translateTagsToSdk(tags))
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

    public static List<software.amazon.awssdk.services.rds.model.Tag> translateTagsToSdk(final Collection<Tag> tags) {
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

    static ResourceModel translateToModel(
            final String subscriptionName,
            final EventSubscription eventSubscription
    ) {
        return ResourceModel.builder()
                .enabled(eventSubscription.enabled())
                .eventCategories(eventSubscription.eventCategoriesList())
                .subscriptionName(subscriptionName)
                .snsTopicArn(eventSubscription.snsTopicArn())
                .sourceType(eventSubscription.sourceType())
                .sourceIds(new HashSet<>(eventSubscription.sourceIdsList()))
                .build();
    }
}
