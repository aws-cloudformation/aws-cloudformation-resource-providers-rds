package software.amazon.rds.eventsubscription;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import software.amazon.awssdk.services.rds.model.AddSourceIdentifierToSubscriptionRequest;
import software.amazon.awssdk.services.rds.model.CreateEventSubscriptionRequest;
import software.amazon.awssdk.services.rds.model.DeleteEventSubscriptionRequest;
import software.amazon.awssdk.services.rds.model.DescribeEventSubscriptionsRequest;
import software.amazon.awssdk.services.rds.model.ModifyEventSubscriptionRequest;
import software.amazon.awssdk.services.rds.model.RemoveSourceIdentifierFromSubscriptionRequest;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.rds.common.handler.Tagging;

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

    static Set<software.amazon.rds.eventsubscription.Tag> translateTagsFromSdk(final Collection<Tag> tags) {
        return Optional.ofNullable(tags).orElse(Collections.emptySet())
                .stream()
                .map(tag -> software.amazon.rds.eventsubscription.Tag.builder()
                        .key(tag.key())
                        .value(tag.value()).build())
                .collect(Collectors.toSet());
    }
}
