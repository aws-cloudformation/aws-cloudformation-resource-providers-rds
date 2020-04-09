package software.amazon.rds.optiongroup;

import static software.amazon.rds.optiongroup.Translator.addTagsToResourceRequest;
import static software.amazon.rds.optiongroup.Translator.getOptionNames;
import static software.amazon.rds.optiongroup.Translator.listTagsForResourceRequest;
import static software.amazon.rds.optiongroup.Translator.mapToTags;
import static software.amazon.rds.optiongroup.Translator.removeTagsFromResourceRequest;

import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class UpdateHandler extends BaseHandlerStd {
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<RdsClient> proxyClient,
        final Logger logger) {
      final ResourceModel previousResourceModel = request.getPreviousResourceState();
      return proxy.initiate("rds::update-option-group", proxyClient, request.getDesiredResourceState(), callbackContext)
          .request((model) -> Translator.modifyOptionGroupRequest(model, getOptionNames(previousResourceModel.getOptionConfigurations())))
          .call((modifyOptionGroupRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(modifyOptionGroupRequest, proxyInvocation.client()::modifyOptionGroup))
          .progress()
          .then(progress -> tagResource(proxy, proxyClient, progress, request.getDesiredResourceTags()))
          .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    protected ProgressEvent<ResourceModel, CallbackContext> tagResource(
        final AmazonWebServicesClientProxy proxy,
        final ProxyClient<RdsClient> proxyClient,
        final ProgressEvent<ResourceModel, CallbackContext> progress,
        final Map<String, String> tags) {
        return proxy.initiate("rds::tag-event-subscription", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
            .request(Translator::describeOptionGroupsRequest)
            .call((describeOptionGroupsRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(describeOptionGroupsRequest, proxyInvocation.client()::describeOptionGroups))
            .done((describeOptionGroupsRequest, describeOptionGroupsResponse, proxyInvocation, resourceModel, context) -> {
                final String arn = describeOptionGroupsResponse.optionGroupsList()
                    .stream().findFirst().get().optionGroupArn();

                final Set<Tag> currentTags = new HashSet<>(Optional.ofNullable(mapToTags(tags)).orElse(Collections.emptySet()));

                final Set<Tag> existingTags = Translator.translateTagsFromSdk(
                    proxyInvocation.injectCredentialsAndInvokeV2(listTagsForResourceRequest(arn),
                        proxyInvocation.client()::listTagsForResource).tagList());

                final Set<Tag> tagsToRemove = Sets.difference(existingTags, currentTags);
                final Set<Tag> tagsToAdd = Sets.difference(currentTags, existingTags);

                proxyInvocation.injectCredentialsAndInvokeV2(
                    removeTagsFromResourceRequest(arn, tagsToRemove),
                    proxyInvocation.client()::removeTagsFromResource);
                proxyInvocation.injectCredentialsAndInvokeV2(
                    addTagsToResourceRequest(arn, tagsToAdd),
                    proxyInvocation.client()::addTagsToResource);
                return ProgressEvent.progress(resourceModel, context);
            });
    }
}
