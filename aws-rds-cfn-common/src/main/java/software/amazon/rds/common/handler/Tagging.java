package software.amazon.rds.common.handler;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
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
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.error.ErrorRuleSet;
import software.amazon.rds.common.error.ErrorStatus;

public final class Tagging {
    public static final ErrorRuleSet IGNORE_LIST_TAGS_PERMISSION_DENIED_ERROR_RULE_SET = ErrorRuleSet
            .extend(ErrorRuleSet.EMPTY_RULE_SET)
            .withErrorCodes(ErrorStatus.ignore(),
                    ErrorCode.AccessDenied,
                    ErrorCode.AccessDeniedException)
            .build();

    public static final ErrorRuleSet STACK_TAGS_ERROR_RULE_SET = ErrorRuleSet
            .extend(ErrorRuleSet.EMPTY_RULE_SET)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.UnauthorizedTaggingOperation),
                    ErrorCode.AccessDenied,
                    ErrorCode.AccessDeniedException
            ).build();

    public static final ErrorRuleSet RESOURCE_TAG_ERROR_RULE_SET = ErrorRuleSet
            .extend(ErrorRuleSet.EMPTY_RULE_SET)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.AccessDenied),
                    ErrorCode.AccessDenied,
                    ErrorCode.AccessDeniedException
            ).build();
    public static final String RDS_ADD_TAGS_TO_RESOURCE_ACTION = "rds:AddTagsToResource";

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

    public static Collection<Tag> exclude(final Collection<Tag> from, final Collection<Tag> what) {
        final Set<Tag> result = new LinkedHashSet<>(from);
        result.removeAll(what);
        return result;
    }


    public static Set<Tag> translateTagsToSdk(final Collection<Tag> tags) {
        return Optional.ofNullable(tags).orElse(Collections.emptySet())
                .stream()
                .map(tag -> Tag.builder()
                        .key(tag.key())
                        .value(tag.value())
                        .build())
                .collect(Collectors.toCollection(LinkedHashSet::new));
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

    public static ErrorRuleSet getUpdateTagsAccessDeniedRuleSet(
            final TagSet tagsToAdd,
            final TagSet tagsToRemove
    ) {
        return getUpdateTagsAccessDeniedRuleSet(tagsToAdd, tagsToRemove, STACK_TAGS_ERROR_RULE_SET, RESOURCE_TAG_ERROR_RULE_SET);
    }

    public static ErrorRuleSet getUpdateTagsAccessDeniedRuleSet(
            final TagSet tagsToAdd,
            final TagSet tagsToRemove,
            final ErrorRuleSet stackTagsErrorRuleSet,
            final ErrorRuleSet resourceTagsErrorRuleSet
    ) {
        /* If the tagging operation comes across an AccessDenied error, we will throw an UnauthorizedTaggingOperation
        errorCode for stack level tags. For Resource tags, if they are included, we will throw an AccessDenied error.
        This is done to ensure backward compatibility. */
        if (tagsToAdd.getResourceTags().isEmpty() && tagsToRemove.getResourceTags().isEmpty()) {
            return stackTagsErrorRuleSet;
        }
        return resourceTagsErrorRuleSet;
    }

    public static <M, C extends TaggingContext.Provider> ProgressEvent<M, C> createWithTaggingFallback(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final HandlerMethod<M, C> handlerMethod,
            final ProgressEvent<M, C> progress,
            final Tagging.TagSet allTags
    ) {
        final ProgressEvent<M, C> allTagsResult = handlerMethod.invoke(proxy, rdsProxyClient, progress, allTags);
        if (allTagsResult.isFailed()) {
            if (isUnauthorizedTaggingFailure(allTagsResult, allTags)) {
                allTagsResult.setErrorCode(HandlerErrorCode.UnauthorizedTaggingOperation);
            }
            return allTagsResult;
        }
        allTagsResult.getCallbackContext().getTaggingContext().setAddTagsComplete(true);
        return allTagsResult;
    }

    private static <M, C extends TaggingContext.Provider> boolean isUnauthorizedTaggingFailure(final ProgressEvent<M, C> allTagsResult,
                                                                                               final TagSet allTags) {
        return HandlerErrorCode.AccessDenied.equals(allTagsResult.getErrorCode()) &&
                        allTags.getResourceTags().isEmpty() &&
                        allTagsResult.getMessage() != null &&
                        allTagsResult.getMessage().contains(RDS_ADD_TAGS_TO_RESOURCE_ACTION);
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
