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
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceRequest;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.common.error.ErrorRuleSet;

public final class Tagging {

    public static <K, V> Map<K, V> mergeTags(Map<K, V> tagsMap1, Map<K, V> tagsMap2) {
        final Map<K, V> result = new HashMap<>();
        result.putAll(Optional.ofNullable(tagsMap1).orElse(Collections.emptyMap()));
        result.putAll(Optional.ofNullable(tagsMap2).orElse(Collections.emptyMap()));
        return result;
    }

    public static Set<Tag> translateTagsToSdk(Map<String, String> tags) {
        return tags.entrySet()
                .stream()
                .map(entry -> Tag.builder()
                        .key(entry.getKey())
                        .value(entry.getValue())
                        .build())
                .collect(Collectors.toSet());
    }

    public static Set<Tag> listTagsForResource(
            final ProxyClient<RdsClient> rdsProxyClient,
            final String arn) {

        ListTagsForResourceResponse listTagsForResourceResponse = rdsProxyClient.injectCredentialsAndInvokeV2(
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

    private static void addTags(
            final ProxyClient<RdsClient> rdsProxyClient,
            final String arn,
            final Collection<Tag> tagsToAdd
    ) {
        if (CollectionUtils.isNullOrEmpty(tagsToAdd))
            return;

        rdsProxyClient.injectCredentialsAndInvokeV2(
                addTagsToResourceRequest(arn, tagsToAdd),
                rdsProxyClient.client()::addTagsToResource
        );
    }

    private static void removeTags(
            final ProxyClient<RdsClient> rdsProxyClient,
            final String arn,
            final Collection<Tag> tagsToRemove
    ) {
        if (CollectionUtils.isNullOrEmpty(tagsToRemove))
            return;

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

}
