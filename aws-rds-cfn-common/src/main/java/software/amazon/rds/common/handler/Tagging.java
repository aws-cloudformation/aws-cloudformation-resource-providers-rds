package software.amazon.rds.common.handler;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.amazonaws.util.CollectionUtils;
import com.google.common.collect.Sets;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceRequest;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.error.ErrorRuleSet;
import software.amazon.rds.common.error.ErrorStatus;

public final class Tagging {
    public static final ErrorRuleSet SOFT_FAIL_IN_PROGRESS_TAGGING_ERROR_RULE_SET = ErrorRuleSet.builder()
            .withErrorCodes(ErrorStatus.ignore(OperationStatus.IN_PROGRESS),
                    ErrorCode.AccessDenied,
                    ErrorCode.AccessDeniedException)
            .build();

    public static final ErrorRuleSet SOFT_FAIL_TAG_ERROR_RULE_SET = ErrorRuleSet.builder()
            .withErrorCodes(ErrorStatus.ignore(),
                    ErrorCode.AccessDenied,
                    ErrorCode.AccessDeniedException
            ).build();

    public static final ErrorRuleSet HARD_FAIL_TAG_ERROR_RULE_SET = ErrorRuleSet.builder()
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.AccessDenied),
                    ErrorCode.AccessDenied,
                    ErrorCode.AccessDeniedException
            ).build();

    public static TagSet exclude(final TagSet from, final TagSet what) {
        final Set<Tag> systemTags = new LinkedHashSet<>(from.getSystemTags());
        systemTags.removeAll(what.getSystemTags());

        final Set<Tag> stackTags = new LinkedHashSet<>(from.getStackTags());
        stackTags.removeAll(what.getStackTags());

        final Set<Tag> resourceTags = new LinkedHashSet<>(from.getResourceTags());
        resourceTags.removeAll(what.getResourceTags());

        return TagSet.builder()
                .systemTags(systemTags)
                .stackTags(stackTags)
                .resourceTags(resourceTags)
                .build();
    }

    public static <K, V> Map<K, V> mergeTags(Map<K, V> tagsMap1, Map<K, V> tagsMap2) {
        final Map<K, V> result = new LinkedHashMap<>();
        result.putAll(Optional.ofNullable(tagsMap1).orElse(Collections.emptySortedMap()));
        result.putAll(Optional.ofNullable(tagsMap2).orElse(Collections.emptySortedMap()));
        return result;
    }

    public static Collection<Tag> translateTagsToSdk(final TagSet tagSet) {
        //For backward compatibility, We will resolve duplicates tags between stack level tags and resource tags.
        final Map<String, Tag> allTags = new LinkedHashMap<>();
        addToMapIfAbsent(allTags, tagSet.getResourceTags());
        addToMapIfAbsent(allTags, tagSet.getStackTags());
        addToMapIfAbsent(allTags, tagSet.getSystemTags());
        return allTags.values();
    }

    public static Set<Tag> translateTagsToSdk(final Map<String, String> tags) {
        return Optional.ofNullable(tags).orElse(Collections.emptySortedMap()).entrySet()
                .stream()
                .map(entry -> Tag.builder()
                        .key(entry.getKey())
                        .value(entry.getValue())
                        .build())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    public static Set<Tag> listTagsForResource(
            final ProxyClient<RdsClient> rdsProxyClient,
            final String arn
    ) {
        final ListTagsForResourceResponse listTagsForResourceResponse = rdsProxyClient.injectCredentialsAndInvokeV2(
                listTagsForResourceRequest(arn),
                rdsProxyClient.client()::listTagsForResource
        );

        return listTagsForResourceResponse.hasTagList() ?
                Sets.newLinkedHashSet(listTagsForResourceResponse.tagList()) : Sets.newLinkedHashSet();
    }

    public static <M, C> ProgressEvent<M, C> updateTags(
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<M, C> progress,
            final String resourceArn,
            final Map<String, String> previousTags,
            final Map<String, String> desiredTags,
            final ErrorRuleSet errorRuleSet
    ) {
        final Set<Tag> desiredTagSet = new LinkedHashSet<>(translateTagsToSdk(desiredTags));
        final Set<Tag> previousTagSet = new LinkedHashSet<>(translateTagsToSdk(previousTags));

        final Set<Tag> tagsToAdd = Sets.difference(desiredTagSet, previousTagSet);
        final Set<Tag> tagsToRemove = Sets.difference(previousTagSet, desiredTagSet);

        try {
            removeTags(rdsProxyClient, resourceArn, tagsToRemove);
            addTags(rdsProxyClient, resourceArn, tagsToAdd);
            return progress;
        } catch (Exception e) {
            return Commons.handleException(progress, e, errorRuleSet);
        }
    }

    public static void addTags(
            final ProxyClient<RdsClient> rdsProxyClient,
            final String arn,
            final Collection<Tag> tagsToAdd
    ) {
        if (CollectionUtils.isNullOrEmpty(tagsToAdd)) {
            return;
        }

        rdsProxyClient.injectCredentialsAndInvokeV2(
                addTagsToResourceRequest(arn, tagsToAdd),
                rdsProxyClient.client()::addTagsToResource
        );
    }

    public static void removeTags(
            final ProxyClient<RdsClient> rdsProxyClient,
            final String arn,
            final Collection<Tag> tagsToRemove
    ) {
        if (CollectionUtils.isNullOrEmpty(tagsToRemove)) {
            return;
        }

        rdsProxyClient.injectCredentialsAndInvokeV2(
                removeTagsFromResourceRequest(arn, tagsToRemove),
                rdsProxyClient.client()::removeTagsFromResource
        );
    }

    private static ListTagsForResourceRequest listTagsForResourceRequest(final String arn) {
        return ListTagsForResourceRequest.builder()
                .resourceName(arn)
                .build();
    }

    private static AddTagsToResourceRequest addTagsToResourceRequest(final String arn,
                                                                     final Collection<Tag> tagsToAdd) {
        return AddTagsToResourceRequest.builder()
                .resourceName(arn)
                .tags(tagsToAdd)
                .build();
    }

    private static RemoveTagsFromResourceRequest removeTagsFromResourceRequest(
            final String arn,
            final Collection<Tag> tagsToRemove
    ) {
        return RemoveTagsFromResourceRequest.builder()
                .resourceName(arn)
                .tagKeys(tagsToRemove.stream().map(Tag::key).collect(Collectors.toCollection(LinkedHashSet::new)))
                .build();
    }

    public static ErrorRuleSet bestEffortErrorRuleSet(
            final TagSet tagsToAdd,
            final TagSet tagsToRemove
    ) {
        return bestEffortErrorRuleSet(tagsToAdd, tagsToRemove, SOFT_FAIL_TAG_ERROR_RULE_SET, HARD_FAIL_TAG_ERROR_RULE_SET);
    }

    public static ErrorRuleSet bestEffortErrorRuleSet(
            final TagSet tagsToAdd,
            final TagSet tagsToRemove,
            final ErrorRuleSet softFailErrorRuleSet,
            final ErrorRuleSet hardFailErrorRuleSet
    ) {
        // Only soft fail if the customer provided no resource-level tags
        if (tagsToAdd.getResourceTags().isEmpty() && tagsToRemove.getResourceTags().isEmpty()) {
            return softFailErrorRuleSet;
        }
        return hardFailErrorRuleSet;
    }

    public static <M, C> ProgressEvent<M, C> softUpdateTags(
            ProxyClient<RdsClient> proxyClient,
            ProgressEvent<M, C> progress,
            Tagging.TagSet previousTags,
            Tagging.TagSet desiredTags,
            Supplier<Either<String, ProgressEvent<M, C>>> arnSupplier,
            ErrorRuleSet errorRuleSet) {
        final Tagging.TagSet tagsToAdd = Tagging.exclude(desiredTags, previousTags);
        final Tagging.TagSet tagsToRemove = Tagging.exclude(previousTags, desiredTags);

        if (tagsToAdd.isEmpty() && tagsToRemove.isEmpty()) {
            return progress;
        }

        final Either<String, ProgressEvent<M, C>> arnOrError = arnSupplier.get();
        if (!arnOrError.getLeft().isPresent()) {
            return arnOrError.getRight().get();
        }
        final String arn = arnOrError.getLeft().get();

        try {
            Tagging.removeTags(proxyClient, arn, Tagging.translateTagsToSdk(tagsToRemove));
            Tagging.addTags(proxyClient, arn, Tagging.translateTagsToSdk(tagsToAdd));
        } catch (Exception exception) {
            return Commons.handleException(
                    progress,
                    exception,
                    Tagging.bestEffortErrorRuleSet(tagsToAdd, tagsToRemove).orElse(errorRuleSet)
            );
        }
        return progress;
    }

    private static void addToMapIfAbsent(Map<String, Tag> allTags, Collection<Tag> tags) {
        for (Tag tag : tags) {
            allTags.putIfAbsent(tag.key(), tag);
        }
    }

    @Builder(toBuilder = true)
    @AllArgsConstructor
    @Data
    public static class TagSet {
        @Builder.Default
        private Set<Tag> systemTags = new LinkedHashSet<>();
        @Builder.Default
        private Set<Tag> stackTags = new LinkedHashSet<>();
        @Builder.Default
        private Set<Tag> resourceTags = new LinkedHashSet<>();

        public static TagSet emptySet() {
            return TagSet.builder().build();
        }

        public boolean isEmpty() {
            return systemTags.isEmpty() &&
                    stackTags.isEmpty() &&
                    resourceTags.isEmpty();
        }
    }
}
