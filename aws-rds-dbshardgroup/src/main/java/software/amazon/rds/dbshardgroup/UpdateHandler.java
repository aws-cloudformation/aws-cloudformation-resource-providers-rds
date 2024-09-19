package software.amazon.rds.dbshardgroup;

import java.util.function.Function;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;

import java.util.HashSet;

public class UpdateHandler extends BaseHandlerStd {
    static final int POST_MODIFY_DELAY_SEC = 300;
    static final int CALLBACK_DELAY = 6;

    /** Default constructor w/ default backoff */
    public UpdateHandler() {
    }

    /** Default constructor w/ custom config */
    public UpdateHandler(HandlerConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                          final ProxyClient<RdsClient> proxyClient,
                                                                          final ResourceHandlerRequest<ResourceModel> request,
                                                                          final CallbackContext callbackContext) {
        final Tagging.TagSet previousTags = Tagging.TagSet.builder()
                .systemTags(Tagging.translateTagsToSdk(request.getPreviousSystemTags()))
                .stackTags(Tagging.translateTagsToSdk(request.getPreviousResourceTags()))
                .resourceTags(new HashSet<>(Translator.translateTagsToSdk(request.getPreviousResourceState().getTags())))
                .build();

        final Tagging.TagSet desiredTags = Tagging.TagSet.builder()
                .systemTags(Tagging.translateTagsToSdk(request.getSystemTags()))
                .stackTags(Tagging.translateTagsToSdk(request.getDesiredResourceTags()))
                .resourceTags(new HashSet<>(Translator.translateTagsToSdk(request.getDesiredResourceState().getTags())))
                .build();

        ResourceModel desiredModel = request.getDesiredResourceState();

        return ProgressEvent.progress(desiredModel, callbackContext)
                .then(progress -> Commons.execOnce(
                        progress,
                        () -> proxy.initiate("rds::modify-db-shard-group", proxyClient, request.getDesiredResourceState(), callbackContext)
                                .translateToServiceRequest(model -> Translator.modifyDbShardGroupRequest(desiredModel))
                                .backoffDelay(config.getBackoff())
                                .makeServiceCall((modifyDbShardGroupRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(modifyDbShardGroupRequest, proxyClient.client()::modifyDBShardGroup))
                                .handleError((describeRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                                        ProgressEvent.progress(resourceModel, ctx),
                                        exception,
                                    DEFAULT_DB_SHARD_GROUP_ERROR_RULE_SET,
                                    requestLogger))
                                .progress(), CallbackContext::isUpdated, CallbackContext::setUpdated))
                .then(progress -> Commons.execOnce(progress, () -> updateTags(proxyClient, request, progress, previousTags, desiredTags), CallbackContext::isTagged, CallbackContext::setTagged))
                // There is a lag between the modifyDbShardGroup request call and the shard group state moving to "modifying", so we introduce a fixed delay prior to stabilization
                .then((progress) -> delay(progress, POST_MODIFY_DELAY_SEC))
                .then(progress -> proxy.initiate("rds::update-db-shard-group-stabilize", proxyClient, request.getDesiredResourceState(), callbackContext)
                        .translateToServiceRequest(Function.identity())
                        .backoffDelay(config.getBackoff())
                        .makeServiceCall(NOOP_CALL)
                        .stabilize((noopRequest, noopResponse, proxyInvocation, model, context) -> isDBShardGroupStabilized(model, proxyInvocation))
                        .handleError((deleteRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                                ProgressEvent.progress(resourceModel, ctx),
                                exception,
                                DEFAULT_DB_SHARD_GROUP_ERROR_RULE_SET,
                                requestLogger
                        ))
                        .progress())
                // Stabilize cluster state to ensure shard group operations are fully available
                .then(progress -> proxy.initiate("rds::update-db-shard-group-stabilize-cluster", proxyClient, request.getDesiredResourceState(), callbackContext)
                        .translateToServiceRequest(Function.identity())
                        .backoffDelay(config.getBackoff())
                        .makeServiceCall(NOOP_CALL)
                        .stabilize((noopRequest, noopResponse, proxyInvocation, model, context) -> isDBClusterStabilized(model, proxyInvocation))
                        .handleError((noopRequest, exception, client, model, context) -> Commons.handleException(
                                ProgressEvent.progress(model, context),
                                exception,
                                DEFAULT_DB_SHARD_GROUP_ERROR_RULE_SET,
                                requestLogger
                        )).progress())
                .then(progress -> new ReadHandler().handleRequest(proxy, proxyClient, request, callbackContext));
    }

    /** Inserts an artificial delay */
    private ProgressEvent<ResourceModel, CallbackContext> delay(final ProgressEvent<ResourceModel, CallbackContext> evt, final int seconds) {
        CallbackContext callbackContext = evt.getCallbackContext();
        if (callbackContext.getWaitTime() <= seconds) {
            callbackContext.setWaitTime(callbackContext.getWaitTime() + CALLBACK_DELAY);
            return ProgressEvent.defaultInProgressHandler(callbackContext, CALLBACK_DELAY, evt.getResourceModel());
        } else {
            return ProgressEvent.progress(evt.getResourceModel(), callbackContext);
        }
    }
}
