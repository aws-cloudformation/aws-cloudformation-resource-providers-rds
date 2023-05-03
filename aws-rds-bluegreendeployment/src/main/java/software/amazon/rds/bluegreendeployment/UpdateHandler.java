package software.amazon.rds.bluegreendeployment;


import static software.amazon.rds.common.util.DifferenceUtils.diff;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.logging.RequestLogger;

public class UpdateHandler extends BaseHandlerStd {

    public UpdateHandler() {
        this(HandlerConfig.builder().build());
    }

    public UpdateHandler(final HandlerConfig config) {
        super(config);
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final RequestLogger logger
    ) {

        final ResourceModel previousModel = request.getPreviousResourceState();
        final ResourceModel desiredModel = request.getDesiredResourceState();

        if (diff(previousModel.getStage(), desiredModel.getStage()) == null) {
            throw new CfnInvalidRequestException(new RuntimeException("Resource stage must be altered"));
        }

        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                .then(progress -> switchoverBlueGreenDeployment(proxy, proxyClient, progress))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private ProgressEvent<ResourceModel, CallbackContext> switchoverBlueGreenDeployment(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        return proxy.initiate("rds::switchower-blue-green-deployment", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(Translator::switchoverBlueGreenDeploymentRequest)
                .makeServiceCall((request, client) -> client.injectCredentialsAndInvokeV2(
                        request,
                        client.client()::switchoverBlueGreenDeployment
                ))
                .stabilize((request, response, client, model, context) -> isBlueGreenDeploymentStabilized(client, model))
                .handleError((request, exception, client, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        DEFAULT_BLUE_GREEN_DEPLOYMENT_ERROR_RULE_SET
                ))
                .progress();
    }
}
