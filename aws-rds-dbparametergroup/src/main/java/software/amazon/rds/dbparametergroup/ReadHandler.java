package software.amazon.rds.dbparametergroup;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBParameterGroup;
import software.amazon.awssdk.services.rds.model.DbParameterGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
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
                .makeServiceCall((describeDbParameterGroupsRequest, proxyInvocation) -> {
                    return proxyInvocation.injectCredentialsAndInvokeV2(describeDbParameterGroupsRequest, proxyInvocation.client()::describeDBParameterGroups);
                })
                .handleError((describeDbParameterGroupsRequest, exception, client, resourceModel, ctx) -> {
                    if (exception instanceof DbParameterGroupNotFoundException) {
                        return ProgressEvent.defaultFailureHandler(exception, HandlerErrorCode.NotFound);
                    }
                    throw exception;
                })
                .done((describeDbParameterGroupsRequest, describeDbParameterGroupsResponse, proxyInvocation, model, context) -> {
                    final DBParameterGroup dbParameterGroup = describeDbParameterGroupsResponse.dbParameterGroups().stream().findFirst().get();
                    final ListTagsForResourceRequest listTagsForResourceRequest = Translator.listTagsForResourceRequest(dbParameterGroup.dbParameterGroupArn());
                    final ListTagsForResourceResponse listTagsForResourceResponse = proxyInvocation.injectCredentialsAndInvokeV2(
                            listTagsForResourceRequest,
                            proxyInvocation.client()::listTagsForResource
                    );
                    return ProgressEvent.success(
                            ResourceModel.builder()
                                    .dBParameterGroupName(dbParameterGroup.dbParameterGroupName())
                                    .description(dbParameterGroup.description())
                                    .family(dbParameterGroup.dbParameterGroupFamily())
                                    .tags(Translator.translateTagsFromSdk(listTagsForResourceResponse.tagList()))
                                    .build(),
                            context
                    );
                });
    }
}
