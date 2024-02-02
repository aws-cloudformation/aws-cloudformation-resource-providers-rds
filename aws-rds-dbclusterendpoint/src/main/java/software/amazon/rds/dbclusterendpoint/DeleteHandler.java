package software.amazon.rds.dbclusterendpoint;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DbClusterEndpointNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;

public class DeleteHandler extends BaseHandlerStd {

    public DeleteHandler() {
        this(HandlerConfig.builder()
                .backoff(BACKOFF_DELAY)
                .build());
    }

    public DeleteHandler(HandlerConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext) {
        return proxy.initiate("rds::delete-db-cluster-endpoint", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::deleteDbClusterEndpointRequest)
                .backoffDelay(config.getBackoff())
                .makeServiceCall((deleteDbClusterEndpointRequest, proxyInvocation) ->
                        proxyInvocation.injectCredentialsAndInvokeV2(deleteDbClusterEndpointRequest, proxyInvocation.client()::deleteDBClusterEndpoint))
                .stabilize((deleteDbClusterEndpointRequest, deleteDbClusterEndpointResponse, proxyInvocation, model, context) ->
                        isDeleted(model, proxyInvocation))
                .handleError((deleteRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DEFAULT_DB_CLUSTER_ENDPOINT_ERROR_RULE_SET,
                        requestLogger))
                .done(progress -> ProgressEvent.defaultSuccessHandler(null));
    }

    protected boolean isDeleted(final ResourceModel model,
                                final ProxyClient<RdsClient> proxyClient) {
        try {
            fetchDBClusterEndpoint(model, proxyClient);
            return false;
        } catch (DbClusterEndpointNotFoundException e) {
            return true;
        }
    }
}
