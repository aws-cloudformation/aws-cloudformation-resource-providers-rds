package software.amazon.rds.dbsnapshot;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;

public class UpdateHandler extends BaseHandlerStd {

    public UpdateHandler() {
        this(HandlerConfig.builder().build());
    }

    public UpdateHandler(final HandlerConfig config) {
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
                .then(progress -> updateDBSnapshot(proxy, request, callbackContext, proxyClient))
                .then(progress -> updateTags(proxy, proxyClient, progress, previousTags, desiredTags))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    protected ProgressEvent<ResourceModel, CallbackContext> updateDBSnapshot(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient
    ) {
        return proxy.initiate("rds::update-db-snapshot", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::modifyDbDbSnapshotRequest)
                .backoffDelay(config.getBackoff())
                .makeServiceCall((modifyRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        modifyRequest,
                        proxyInvocation.client()::modifyDBSnapshot
                ))
                .stabilize((modifyRequest, modifyResponse, proxyInvocation, resourceModel, context) ->
                        isStabilized(resourceModel, proxyInvocation))
                .handleError((modifyRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DEFAULT_DB_SNAPSHOT_ERROR_RULE_SET))
                .done((modifyRequest, modifyResponse, proxyInvocation, model, context) ->
                {
                    model.setDBSnapshotArn(modifyResponse.dbSnapshot().dbSnapshotArn());
                    return ProgressEvent.progress(model, context);
                });
    }
}
