package software.amazon.rds.dbparametergroup;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBParameterGroup;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class ReadHandler extends BaseHandlerStd {
    private Logger logger;

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger) {

        this.logger = logger;
        return proxy.initiate("rds::read-db-parameter-group", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::describeDbParameterGroupsRequest)
                .backoffDelay(CONSTANT)
                .makeServiceCall((describeDbParameterGroupsRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(describeDbParameterGroupsRequest, proxyInvocation.client()::describeDBParameterGroups))
                .handleError((describeDbParameterGroupsRequest, exception, client, resourceModel, ctx) -> handleException(exception))
                .done((describeDbParameterGroupsRequest, describeDbParameterGroupsResponse, proxyInvocation, model, context) -> {
                    final DBParameterGroup dBParameterGroup = describeDbParameterGroupsResponse.dbParameterGroups().stream().findFirst().get();
                    callbackContext.setDbParameterGroupArn(dBParameterGroup.dbParameterGroupArn());
                    return ProgressEvent.progress(Translator.translateFromDBParameterGroup(dBParameterGroup), callbackContext);
                })
                .then(progress -> softFailAccessDenied(() ->
                                proxy.initiate("rds::read-db-parameter-group-tags", proxyClient, request.getDesiredResourceState(), callbackContext)
                                        .translateToServiceRequest(resourceModel -> Translator.listTagsForResourceRequest(callbackContext.getDbParameterGroupArn()))
                                        .makeServiceCall((listTagsForResourceRequest, proxyInvocation) ->
                                                proxyInvocation.injectCredentialsAndInvokeV2(listTagsForResourceRequest, proxyInvocation.client()::listTagsForResource))
                                        .done((listTagsForResourceRequest, listTagsForResourceResponse, proxyInvocation, model, context) -> {
                                            progress.getResourceModel().setTags(Translator.translateTagsFromSdk(listTagsForResourceResponse.tagList()));
                                            return ProgressEvent.progress(progress.getResourceModel(), callbackContext);
                                        }),
                        progress.getResourceModel(), progress.getCallbackContext()))
                .then(progress -> ProgressEvent.success(progress.getResourceModel(), progress.getCallbackContext()));

    }
}
