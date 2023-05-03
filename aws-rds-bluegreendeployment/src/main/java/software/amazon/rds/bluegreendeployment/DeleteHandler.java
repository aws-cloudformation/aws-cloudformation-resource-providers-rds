package software.amazon.rds.bluegreendeployment;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.logging.RequestLogger;

public class DeleteHandler extends BaseHandlerStd {

    public DeleteHandler() {
        this(HandlerConfig.builder().build());
    }

    public DeleteHandler(final HandlerConfig config) {
        super(config);
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> client,
            final RequestLogger logger
    ) {
        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                .then(progress -> deleteBlueGreenDeployment(proxy, client, progress))
                .then(progress -> ProgressEvent.defaultSuccessHandler(null));
    }

    private ProgressEvent<ResourceModel, CallbackContext> deleteBlueGreenDeployment(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        return proxy.initiate("rds::delete-blue-green-deployment", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(Translator::deleteBlueGreenDeploymentRequest)
                .makeServiceCall((request, client) -> client.injectCredentialsAndInvokeV2(
                        request,
                        client.client()::deleteBlueGreenDeployment
                ))
                .stabilize((request, response, client, model, context) -> isBlueGreenDeploymentDeleted(client, model))
                .handleError((request, exception, client, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        DEFAULT_BLUE_GREEN_DEPLOYMENT_ERROR_RULE_SET
                ))
                .progress();
    }
}
