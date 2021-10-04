package software.amazon.rds.dbparametergroup;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.Sets;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class UpdateHandler extends BaseHandlerStd {

    protected ProgressEvent<ResourceModel, CallbackContext> tagResource(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final ResourceModel model,
            final CallbackContext callbackContext,
            final Map<String, String> tags) {
        return softFailAccessDenied(
                () -> proxy.initiate("rds::tag-db-parameter-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                        .translateToServiceRequest(Translator::describeDbParameterGroupsRequest)
                        .makeServiceCall(((describeDbGroupsRequest, proxyInvocation) ->
                                proxyInvocation.injectCredentialsAndInvokeV2(describeDbGroupsRequest, proxyInvocation.client()::describeDBParameterGroups)))
                        .done((describeDbParameterGroupsRequest, describeDbParameterGroupsResponse, invocation, resourceModel, context) -> {
                            final String arn = describeDbParameterGroupsResponse.dbParameterGroups().stream().findFirst().get().dbParameterGroupArn();
                            final Set<Tag> currentTags = Sets.newHashSet(Translator.translateTagsToModelResource(tags));
                            final Set<Tag> existingTags = Sets.newHashSet(Translator.translateTagsFromSdk(proxyClient.injectCredentialsAndInvokeV2(Translator.listTagsForResourceRequest(arn), proxyClient.client()::listTagsForResource).tagList()));
                            final Set<Tag> tagsToRemove = Sets.difference(existingTags, currentTags);
                            final Set<Tag> tagsToAdd = Sets.difference(currentTags, existingTags);
                            invocation.injectCredentialsAndInvokeV2(
                                    Translator.removeTagsFromResourceRequest(arn, tagsToRemove),
                                    invocation.client()::removeTagsFromResource
                            );
                            invocation.injectCredentialsAndInvokeV2(
                                    Translator.addTagsToResourceRequest(arn, tagsToAdd),
                                    invocation.client()::addTagsToResource
                            );
                            return ProgressEvent.progress(resourceModel, context);
                        }), model, callbackContext);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();
        //Parameters are the same. No need to make reset and modify parameter requests. We need only to update tags
        final boolean skipUpdatingParameters = model.getParameters().equals(request.getPreviousResourceState().getParameters());
        return ProgressEvent.progress(model, callbackContext)
                .then(progress -> {
                    if (skipUpdatingParameters) return progress;
                    return proxy.initiate("rds::update-db-parameter-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                            .translateToServiceRequest(Translator::resetDbParameterGroupRequest)
                            .backoffDelay(CONSTANT)
                            .makeServiceCall((resetGroupRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(resetGroupRequest, proxyInvocation.client()::resetDBParameterGroup))
                            .handleError((awsRequest, exception, client, resourceModel, context) -> handleException(exception))
                            .done((resetGroupRequest, resetGroupResponse, proxyInvocation, resourceModel, context) -> ProgressEvent.progress(resourceModel, context))
                            .then(p -> applyParameters(proxy, proxyClient, p.getResourceModel(), p.getCallbackContext()));
                })
                .then(progress -> tagResource(proxy, proxyClient, progress, model, callbackContext, mergeMaps(request.getSystemTags(), request.getDesiredResourceTags())))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }
}
