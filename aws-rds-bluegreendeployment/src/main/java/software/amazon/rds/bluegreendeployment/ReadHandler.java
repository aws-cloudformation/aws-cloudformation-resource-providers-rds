package software.amazon.rds.bluegreendeployment;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.BlueGreenDeployment;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.logging.RequestLogger;

public class ReadHandler extends BaseHandlerStd {

    public ReadHandler() {
        this(HandlerConfig.builder().build());
    }

    public ReadHandler(final HandlerConfig config) {
        super(config);
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> req,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final RequestLogger logger
    ) {
        return proxy.initiate("rds::describe-blue-green-deployment", proxyClient, req.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::describeBlueGreenDeploymentsRequest)
                .makeServiceCall((request, client) -> client.injectCredentialsAndInvokeV2(
                        request,
                        client.client()::describeBlueGreenDeployments
                ))
                .handleError((request, exception, client, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        DEFAULT_BLUE_GREEN_DEPLOYMENT_ERROR_RULE_SET
                ))
                .done((request, response, client, model, context) -> {
                    final BlueGreenDeployment blueGreenDeployment = response.blueGreenDeployments().get(0);
                    return ProgressEvent.success(Translator.translateBlueGreenDeploymentFromSdk(blueGreenDeployment), context);
                });
    }
}
