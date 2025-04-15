package software.amazon.rds.customdbengineversion;

import java.util.function.Function;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
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

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient) {
        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                .then(progress -> proxy.initiate("rds::delete-custom-db-engine-version", proxyClient, request.getDesiredResourceState(), callbackContext)
                        .translateToServiceRequest(Translator::deleteCustomDbEngineVersion)
                        .backoffDelay(config.getBackoff())
                        .makeServiceCall((deleteDbEngineVersionRequest, proxyInvocation) ->
                                proxyInvocation.injectCredentialsAndInvokeV2(deleteDbEngineVersionRequest, proxyInvocation.client()::deleteCustomDBEngineVersion))
                        .handleError((deleteRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                                ProgressEvent.progress(resourceModel, ctx),
                                exception,
                                ACCESS_DENIED_TO_NOT_FOUND_ERROR_RULE_SET,
                                requestLogger))
                        .progress()
                )
                // Stabilization has been modified to handle a specific edge case during CEV deletion.
                // If call chain encounters an InvalidCustomDbEngineVersionStateException error with the message "CustomDBEngineVersion is already being deleted"
                // This will not call stabilize step. Hence we have separate stabilization chain step to handle the situation.
                // This ensures that the deletion process is completed properly.
                .then(progress -> proxy.initiate("rds::delete-custom-db-engine-version-stabilize", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                        .translateToServiceRequest(Function.identity())
                        .backoffDelay(config.getBackoff())
                        .makeServiceCall(NOOP_CALL)
                        .stabilize((noopRequest, noopResponse, proxyInvocation, model, context) -> isDeleted(model, proxyInvocation))
                        .handleError((noopRequest, exception, client, model, context) -> Commons.handleException(
                                ProgressEvent.progress(model, context),
                                exception,
                                DEFAULT_CUSTOM_DB_ENGINE_VERSION_ERROR_RULE_SET,
                                requestLogger
                        ))
                        .progress()
                )
                .then(progress -> ProgressEvent.defaultSuccessHandler(null));
    }

    protected boolean isDeleted(final ResourceModel model,
                                final ProxyClient<RdsClient> proxyClient) {
        try {
            fetchDBEngineVersion(model, proxyClient);
            return false;
        } catch (CfnNotFoundException e) {
            return true;
        }
    }

}
