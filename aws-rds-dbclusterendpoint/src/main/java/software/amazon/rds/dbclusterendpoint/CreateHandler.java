package software.amazon.rds.dbclusterendpoint;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.HandlerMethod;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.util.IdentifierFactory;

import java.util.HashSet;

public class CreateHandler extends BaseHandlerStd {
    private final IdentifierFactory dbClusterEndpointIdentifierFactory = new IdentifierFactory(
            DEFAULT_STACK_NAME,
            RESOURCE_IDENTIFIER,
            RESOURCE_ID_MAX_LENGTH
    );

    public CreateHandler() {
        this(HandlerConfig.builder()
                .backoff(BACKOFF_DELAY)
                .build());
    }

    public CreateHandler(HandlerConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();

        final Tagging.TagSet allTags = Tagging.TagSet.builder()
                .systemTags(Tagging.translateTagsToSdk(request.getSystemTags()))
                .stackTags(Tagging.translateTagsToSdk(request.getDesiredResourceTags()))
                .resourceTags(new HashSet<>(Translator.translateTagsToSdk(request.getDesiredResourceState().getTags())))
                .build();

        if (StringUtils.isNullOrEmpty(model.getDBClusterEndpointIdentifier())) {
            model.setDBClusterEndpointIdentifier(dbClusterEndpointIdentifierFactory.newIdentifier()
                    .withStackId(request.getStackId())
                    .withResourceId(request.getLogicalResourceIdentifier())
                    .withRequestToken(request.getClientRequestToken())
                    .toString());
        }
        return ProgressEvent.progress(model, callbackContext)
                .then(progress -> Tagging.safeCreate(proxy, proxyClient, this::createDbClusterEndpoint, progress, allTags))
                .then(progress -> Commons.execOnce(progress, () -> {
                            final Tagging.TagSet extraTags = Tagging.TagSet.builder()
                                    .stackTags(Tagging.translateTagsToSdk(request.getDesiredResourceTags()))
                                    .resourceTags(new HashSet<>(Translator.translateTagsToSdk(request.getDesiredResourceState().getTags())))
                                    .build();
                            return updateTags(proxy, proxyClient, progress, model.getDBClusterEndpointArn(), Tagging.TagSet.emptySet(), extraTags);
                        }, CallbackContext::isAddTagsComplete, CallbackContext::setAddTagsComplete
                ))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }


    private ProgressEvent<ResourceModel, CallbackContext> createDbClusterEndpoint(final AmazonWebServicesClientProxy proxy,
                                                                                  final ProxyClient<RdsClient> proxyClient,
                                                                                  final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                                  final Tagging.TagSet tags
    ) {
        return proxy.initiate("rds::create-db-cluster-endpoint", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest((resourceModel) -> Translator.createDbClusterEndpointRequest(resourceModel, tags))
                .backoffDelay(config.getBackoff())
                .makeServiceCall((createDbClusterEndpointRequest, proxyInvocation) ->
                        proxyInvocation.injectCredentialsAndInvokeV2(createDbClusterEndpointRequest, proxyInvocation.client()::createDBClusterEndpoint))
                .stabilize((createDbClusterEndpointRequest, createDbClusterEndpointResponse, proxyInvocation, resourceModel, context) ->
                        isStabilized(resourceModel, proxyInvocation))
                .handleError((createRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DEFAULT_DB_CLUSTER_ENDPOINT_ERROR_RULE_SET))
                .done((createRequest, createResponse, proxyInvocation, model, context) ->
                {
                    context.setDbClusterEndpointArn(createResponse.dbClusterEndpointArn());
                    return ProgressEvent.progress(model, context);
                });
    }
}
