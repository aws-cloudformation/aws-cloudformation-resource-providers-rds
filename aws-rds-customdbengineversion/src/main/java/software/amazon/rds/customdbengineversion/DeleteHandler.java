package software.amazon.rds.customdbengineversion;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CustomDbEngineVersionNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;

public class DeleteHandler extends BaseHandlerStd {


    public DeleteHandler() {
        this(CUSTOM_ENGINE_VERSION_HANDLER_CONFIG_10H);
    }

    public DeleteHandler(HandlerConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger) {
        return proxy.initiate("rds::delete-custom-db-engine-version", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::deleteCustomDbEngineVersion)
                .backoffDelay(config.getBackoff())
                .makeServiceCall((deleteDbClusterEndpointRequest, proxyInvocation) ->
                        proxyInvocation.injectCredentialsAndInvokeV2(deleteDbClusterEndpointRequest, proxyInvocation.client()::deleteCustomDBEngineVersion))
                .stabilize((deleteRequest, deleteResponse, proxyInvocation, model, context) ->
                        isDeleted(model, proxyInvocation))
                .handleError((deleteRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DEFAULT_CUSTOM_DB_ENGINE_VERSION_ERROR_RULE_SET))
                .done(progress -> ProgressEvent.defaultSuccessHandler(null));
    }

    protected boolean isDeleted(final ResourceModel model,
                                final ProxyClient<RdsClient> proxyClient) {
        try {
            fetchDBEngineVersion(model, proxyClient);
            return false;
        } catch (CustomDbEngineVersionNotFoundException e) {
            return true;
        }
    }

}
