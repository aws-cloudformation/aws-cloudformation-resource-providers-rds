package software.amazon.rds.optiongroup;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.OptionGroup;
import software.amazon.awssdk.services.rds.model.OptionGroupNotFoundException;
import software.amazon.cloudformation.proxy.*;

import java.util.Collection;
import java.util.Set;

public class ReadHandler extends BaseHandlerStd {
    private Logger logger;

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<RdsClient> proxyClient,
        final Logger logger) {

        this.logger = logger;

        return proxy.initiate("rds::read-option-group", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::describeOptionGroupsRequest)
                .backoffDelay(CONSTANT)
                .makeServiceCall((describeOptionGroupsRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(describeOptionGroupsRequest, proxyInvocation.client()::describeOptionGroups))
                .handleError((describeOptionGroupsRequest, exception, client, resourceModel, ctx) -> {
                    if (exception instanceof OptionGroupNotFoundException) {
                        return ProgressEvent.defaultFailureHandler(exception, HandlerErrorCode.NotFound);
                    }
                    throw exception;
                })
                .done((describeOptionGroupsRequest, describeOptionGroupsResponse, proxyInvocation, model, context) -> {
                    final OptionGroup optionGroup = describeOptionGroupsResponse.optionGroupsList().stream().findFirst().get();
                    final Set<OptionConfiguration> optionConfigurations = Translator.translateOptionConfigurationsFromSdk(optionGroup.options());
                    /*
                    final Set<Tag> tags = Translator.translateTagsFromSdk(
                            proxyInvocation.injectCredentialsAndInvokeV2(
                                    Translator.listTagsForResourceRequest(
                                            optionGroup.optionGroupArn()
                                    ),
                                    proxyInvocation.client()::listTagsForResource
                            ).tagList()
                    );

                     */
                    final String arn = optionGroup.optionGroupArn();
                    ListTagsForResourceRequest req = Translator.listTagsForResourceRequest(arn);
                    ListTagsForResourceResponse proxyResp = proxyInvocation.injectCredentialsAndInvokeV2(
                            req,
                            proxyInvocation.client()::listTagsForResource
                    );
                    Collection<software.amazon.awssdk.services.rds.model.Tag> sdkTags = proxyResp.tagList();
                    final Set<Tag> tags = Translator.translateTagsFromSdk(sdkTags);

                    return ProgressEvent.defaultSuccessHandler(ResourceModel.builder()
                            .optionGroupName(optionGroup.optionGroupName())
                            .optionGroupDescription(optionGroup.optionGroupDescription())
                            .engineName(optionGroup.engineName())
                            .majorEngineVersion(optionGroup.majorEngineVersion())
                            .optionConfigurations(optionConfigurations)
                            .tags(tags)
                            .build());
                });
    }
}
