package software.amazon.rds.optiongroup;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.OptionGroupAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.OptionGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.OptionGroupQuotaExceededException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {
    protected static final Constant BACKOFF_DELAY = Constant.of()
            .timeout(Duration.ofSeconds(150L))
            .delay(Duration.ofSeconds(5L))
            .build();

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

    protected ProgressEvent<ResourceModel, CallbackContext> updateOptionGroup(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress) {
        return proxy.initiate("rds::update-option-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(Translator::modifyOptionGroupRequest)
                .makeServiceCall((modifyRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        modifyRequest,
                        proxyInvocation.client()::modifyOptionGroup
                ))
                .handleError((describeRequest, exception, client, resourceModel, ctx) -> handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception
                ))
                .progress();
    }

    protected ProgressEvent<ResourceModel, CallbackContext> updateTags(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Map<String, String> previousTags,
            final Map<String, String> desiredTags
    ) {
        return proxy.initiate("rds::tag-option-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(Translator::describeOptionGroupsRequest)
                .makeServiceCall((describeRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        describeRequest,
                        proxyInvocation.client()::describeOptionGroups
                )).handleError((describeRequest, exception, client, resourceModel, ctx) -> handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception
                ))
                .done((describeRequest, describeResponse, invocation, resourceModel, ctx) -> {
                    final String arn = describeResponse.optionGroupsList().stream().findFirst().get().optionGroupArn();

                    final Set<Tag> previousTagSet = Translator.translateTagsToModelResource(previousTags);
                    final Set<Tag> desiredTagSet = Translator.translateTagsToModelResource(desiredTags);
                    final Set<Tag> tagsToRemove = Sets.difference(previousTagSet, desiredTagSet);
                    final Set<Tag> tagsToAdd = Sets.difference(desiredTagSet, previousTagSet);

                    proxyClient.injectCredentialsAndInvokeV2(
                            Translator.removeTagsFromResourceRequest(arn, tagsToRemove),
                            proxyClient.client()::removeTagsFromResource
                    );
                    proxyClient.injectCredentialsAndInvokeV2(
                            Translator.addTagsToResourceRequest(arn, tagsToAdd),
                            proxyClient.client()::addTagsToResource
                    );
                    return ProgressEvent.progress(resourceModel, ctx);
                });
    }

    protected List<Tag> listTags(final ProxyClient<RdsClient> proxyClient, final String arn) {
        final ListTagsForResourceResponse listTagsForResourceResponse = proxyClient.injectCredentialsAndInvokeV2(
                Translator.listTagsForResourceRequest(arn),
                proxyClient.client()::listTagsForResource
        );
        return Translator.translateTagsFromSdk(listTagsForResourceResponse.tagList());
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleException(
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Exception e
    ) {
        if (e instanceof OptionGroupAlreadyExistsException) {
            return ProgressEvent.progress(progress.getResourceModel(), progress.getCallbackContext());
        } else if (e instanceof OptionGroupNotFoundException) {
            return ProgressEvent.defaultFailureHandler(e, HandlerErrorCode.NotFound);
        } else if (e instanceof OptionGroupQuotaExceededException) {
            return ProgressEvent.defaultFailureHandler(e, HandlerErrorCode.ServiceLimitExceeded);
        }
        return ProgressEvent.defaultFailureHandler(e, HandlerErrorCode.InternalFailure);
    }

    protected <K, V> Map<K, V> mergeMaps(Map<K,V> m1, Map<K,V> m2) {
        final Map<K, V> result = new HashMap<>();
        if (m1 != null) {
            result.putAll(m1);
        }
        if (m2 != null) {
            result.putAll(m2);
        }
        return result;
    }
}
