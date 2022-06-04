package software.amazon.rds.dbclusterendpoint;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;


public class UpdateHandler extends BaseHandlerStd {


    public UpdateHandler() {
        this(HandlerConfig.builder()
                .backoff(BACKOFF_DELAY)
                .build());
    }

    public UpdateHandler(HandlerConfig config) {
        super(config);
    }
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger
    ) {
        final ResourceModel desiredModel = request.getDesiredResourceState();

        final Tagging.TagSet previousTags = Tagging.TagSet.builder()
                .systemTags(Tagging.translateTagsToSdk(request.getPreviousSystemTags()))
                .stackTags(Tagging.translateTagsToSdk(request.getPreviousResourceTags()))
                .resourceTags(Translator.translateTagsToSdk(request.getPreviousResourceState().getTags()))
                .build();

        final Tagging.TagSet desiredTags = Tagging.TagSet.builder()
                .systemTags(Tagging.translateTagsToSdk(request.getSystemTags()))
                .stackTags(Tagging.translateTagsToSdk(request.getDesiredResourceTags()))
                .resourceTags(Translator.translateTagsToSdk(request.getDesiredResourceState().getTags()))
                .build();

        return ProgressEvent.progress(desiredModel, callbackContext)
                .then(progress -> doPreCheck(proxy, request, callbackContext, proxyClient))
                // Since modifying any property of DBClusterEndpoint causes interruption,
                // we should only update tags. Updating any other properties should require replacement
                .then(progress -> updateTags(proxy, proxyClient, progress, previousTags, desiredTags))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    protected ProgressEvent<ResourceModel, CallbackContext> doPreCheck(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient
    ) {
        return proxy.initiate("rds::update-cluster-endpoint-precheck", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::describeDbClustersEndpointRequest)
                .makeServiceCall((describeRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        describeRequest,
                        proxyInvocation.client()::describeDBClusterEndpoints
                ))
                .handleError((describeRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DEFAULT_DB_CLUSTER_ENDPOINT_ERROR_RULE_SET))
                .done((describeRequest, describeResponse, proxyInvocation, model, context) -> {
                    if (!describeResponse.hasDbClusterEndpoints() || describeResponse.dbClusterEndpoints().isEmpty()) {
                        return ProgressEvent.failed(model, context, HandlerErrorCode.NotFound,
                                "DBClusterEndpoint " + model.getDBClusterEndpointIdentifier() + " not found");
                    }
                    return ProgressEvent.progress(model, context);
                });
    }
}
