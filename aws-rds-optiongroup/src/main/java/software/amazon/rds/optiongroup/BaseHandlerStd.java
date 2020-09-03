package software.amazon.rds.optiongroup;

import com.google.common.collect.Sets;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {
    protected static final Constant CONSTANT = Constant.of().timeout(Duration.ofMinutes(120L))
            .delay(Duration.ofSeconds(30L)).build();

    @Override
    public final ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger) {
        return handleRequest(
                proxy,
                request,
                callbackContext != null ? callbackContext : new CallbackContext(),
                proxy.newProxy(ClientBuilder::getClient),
                logger
        );
    }

    protected abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger);

    protected ProgressEvent<ResourceModel, CallbackContext> tagResource(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Map<String, String> tags
    ) {
        return proxy.initiate("rds::tag-option-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(Translator::describeOptionGroupsRequest)
                .makeServiceCall(((describeOptionGroupsRequest, proxyInvocation) -> {
                    return proxyInvocation.injectCredentialsAndInvokeV2(
                            describeOptionGroupsRequest,
                            proxyInvocation.client()::describeOptionGroups
                    );
                }))
                .handleError((req, exception, client, resourceModel, ctx) -> {
                    throw exception;
                })
                .done((request, response, invocation, model, context) -> {
                    final String arn = response.optionGroupsList().stream().findFirst().get().optionGroupArn();
                    final Set<Tag> currentTags = mapToTags(tags);
                    final Set<Tag> existingTags = listTags(proxyClient, arn);
                    final Set<Tag> tagsToRemove = Sets.difference(existingTags, currentTags);
                    final Set<Tag> tagsToAdd = Sets.difference(currentTags, existingTags);

                    proxyClient.injectCredentialsAndInvokeV2(
                            Translator.removeTagsFromResourceRequest(arn, tagsToRemove),
                            proxyClient.client()::removeTagsFromResource
                    );
                    proxyClient.injectCredentialsAndInvokeV2(
                            Translator.addTagsToResourceRequest(arn, tagsToAdd),
                            proxyClient.client()::addTagsToResource
                    );
                    return ProgressEvent.progress(model, context);
                });
    }

    protected Set<Tag> listTags(final ProxyClient<RdsClient> proxyClient, final String arn) {
        final ListTagsForResourceResponse listTagsForResourceResponse = proxyClient.injectCredentialsAndInvokeV2(
                Translator.listTagsForResourceRequest(arn),
                proxyClient.client()::listTagsForResource
        );
        return Translator.translateTagsFromSdk(listTagsForResourceResponse.tagList());
    }

    protected Set<Tag> mapToTags(final Map<String, String> tags) {
        return Optional.ofNullable(tags).orElse(Collections.emptyMap())
                .entrySet()
                .stream()
                .map(entry -> {
                    return Tag.builder()
                            .key(entry.getKey())
                            .value(entry.getValue())
                            .build();
                })
                .collect(Collectors.toSet());
    }
}
