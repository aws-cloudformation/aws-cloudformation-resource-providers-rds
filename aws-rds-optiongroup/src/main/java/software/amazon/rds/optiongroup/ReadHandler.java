package software.amazon.rds.optiongroup;

import static software.amazon.rds.optiongroup.Translator.translateOptionConfigurationFromSdk;
import static software.amazon.rds.optiongroup.Translator.translateTagsFromSdk;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.OptionGroup;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class ReadHandler extends BaseHandlerStd {
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<RdsClient> proxyClient,
        final Logger logger) {
        return proxy.initiate("rds::read-option-group", proxyClient, request.getDesiredResourceState(), callbackContext)
            .request(Translator::describeOptionGroupsRequest)
            .call((describeOptionGroupsRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(describeOptionGroupsRequest, proxyInvocation.client()::describeOptionGroups))
            .done((decribeOptionGroupsRequest, describeOptionGroupsResponse, proxyInvocation, model, context) -> {
                final OptionGroup optionGroup = describeOptionGroupsResponse.optionGroupsList().stream().findFirst().get();
                final ListTagsForResourceRequest listTagsForResourceRequest = Translator.listTagsForResourceRequest(optionGroup.optionGroupArn());
                final ListTagsForResourceResponse listTagsForResourceResponse = proxyInvocation.injectCredentialsAndInvokeV2(listTagsForResourceRequest, proxyInvocation.client()::listTagsForResource);
                return ProgressEvent.success(
                    ResourceModel.builder()
                        .id(optionGroup.optionGroupName())
                        .engineName(optionGroup.engineName())
                        .majorEngineVersion(optionGroup.majorEngineVersion())
                        .optionGroupDescription(optionGroup.optionGroupDescription())
                        .optionConfigurations(translateOptionConfigurationFromSdk(optionGroup.options()))
                        .tags(translateTagsFromSdk(listTagsForResourceResponse.tagList()))
                        .build(),
                    context
                );
            }); // gather all properties of the resource
    }
}
