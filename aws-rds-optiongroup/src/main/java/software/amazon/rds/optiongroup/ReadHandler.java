package software.amazon.rds.optiongroup;

import java.util.List;

import software.amazon.awssdk.services.rds.RdsClient;
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
                .translateToServiceRequest(Translator::describeOptionGroupsRequest)
                .backoffDelay(BACKOFF_DELAY)
                .makeServiceCall((describeRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        describeRequest,
                        proxyInvocation.client()::describeOptionGroups
                ))
                .handleError((describeRequest, exception, client, resourceModel, ctx) -> handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception
                ))
                .done((describeRequest, describeResponse, proxyInvocation, model, context) -> {
                    final OptionGroup optionGroup = describeResponse.optionGroupsList().stream().findFirst().get();
                    final List<OptionConfiguration> optionConfigurations = Translator.translateOptionConfigurationsFromSdk(optionGroup.options());
                    final List<Tag> tags = listTags(proxyInvocation, optionGroup.optionGroupArn());
                    return ProgressEvent.success(
                            ResourceModel.builder()
                                    .optionGroupName(optionGroup.optionGroupName())
                                    .engineName(optionGroup.engineName())
                                    .majorEngineVersion(optionGroup.majorEngineVersion())
                                    .optionGroupDescription(optionGroup.optionGroupDescription())
                                    .optionConfigurations(optionConfigurations)
                                    .tags(tags)
                                    .build(),
                            context
                    );
                });
    }
}
