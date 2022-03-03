package software.amazon.rds.common.handler;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.error.ErrorRuleSet;
import software.amazon.rds.common.error.ErrorStatus;

public final class Tagging {

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

    @Builder(toBuilder = true)
    @AllArgsConstructor
    @Data
    public static class TagSet {
        @Builder.Default
        private Set<Tag> systemTags = new HashSet<>();
        @Builder.Default
        private Set<Tag> stackTags = new HashSet<>();
        @Builder.Default
        private Set<Tag> resourceTags = new HashSet<>();

        public boolean isEmpty() {
            return systemTags.isEmpty() &&
                    stackTags.isEmpty() &&
                    resourceTags.isEmpty();
        }

        public static TagSet emptySet() {
            return TagSet.builder().build();
        }
    }

    public static TagSet exclude(final TagSet from, final TagSet what) {
        final Set<Tag> systemTags = new HashSet<>(from.getSystemTags());
        systemTags.removeAll(what.getSystemTags());

        final Set<Tag> stackTags = new HashSet<>(from.getStackTags());
        stackTags.removeAll(what.getStackTags());

        final Set<Tag> resourceTags = new HashSet<>(from.getResourceTags());
        resourceTags.removeAll(what.getResourceTags());

        return TagSet.builder()
                .systemTags(systemTags)
                .stackTags(stackTags)
                .resourceTags(resourceTags)
                .build();
    }

    public static <K, V> Map<K, V> mergeTags(Map<K, V> tagsMap1, Map<K, V> tagsMap2) {
        final Map<K, V> result = new HashMap<>();
        result.putAll(Optional.ofNullable(tagsMap1).orElse(Collections.emptyMap()));
        result.putAll(Optional.ofNullable(tagsMap2).orElse(Collections.emptyMap()));
        return result;
    }

    public static Set<Tag> translateTagsToSdk(final TagSet tagSet) {
        final Set<Tag> allTags = new HashSet<>();
        allTags.addAll(tagSet.getSystemTags());
        allTags.addAll(tagSet.getStackTags());
        allTags.addAll(tagSet.getResourceTags());
        return allTags;
    }

    public static Set<Tag> translateTagsToSdk(final Map<String, String> tags) {
        return Optional.ofNullable(tags).orElse(Collections.emptyMap()).entrySet()
                .stream()
                .map(entry -> Tag.builder()
                        .key(entry.getKey())
                        .value(entry.getValue())
                        .build())
                .collect(Collectors.toSet());
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
                Sets.newHashSet(listTagsForResourceResponse.tagList()) : Sets.newHashSet();
    }

    public static <M, C> ProgressEvent<M, C> updateTags(
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<M, C> progress,
            final String resourceArn,
            final Map<String, String> previousTags,
            final Map<String, String> desiredTags,
            final ErrorRuleSet errorRuleSet
    ) {
        final Set<Tag> desiredTagSet = new HashSet<>(translateTagsToSdk(desiredTags));
        final Set<Tag> previousTagSet = new HashSet<>(translateTagsToSdk(previousTags));

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

    private static AddTagsToResourceRequest addTagsToResourceRequest(final String arn, final Collection<Tag> tagsToAdd) {
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
                .tagKeys(tagsToRemove.stream().map(Tag::key).collect(Collectors.toSet()))
                .build();
    }

    public static ErrorRuleSet bestEffortErrorRuleSet(
            final TagSet tagsToAdd,
            final TagSet tagsToRemove
    ) {
        // Only soft fail if the customer provided no resource-level tags
        if (tagsToAdd.getResourceTags().isEmpty() && tagsToRemove.getResourceTags().isEmpty()) {
            return SOFT_FAIL_TAG_ERROR_RULE_SET;
        }
        return HARD_FAIL_TAG_ERROR_RULE_SET;
    }
}
