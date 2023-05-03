package software.amazon.rds.bluegreendeployment;

import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.logging.RequestLogger;

public class ListHandler extends BaseHandlerStd {

    public ListHandler() {
        this(HandlerConfig.builder().build());
    }

    public ListHandler(final HandlerConfig config) {
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
        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                .then(progress -> listBlueGreenDeployments(proxy, proxyClient, progress, request.getNextToken()));
    }

    private ProgressEvent<ResourceModel, CallbackContext> listBlueGreenDeployments(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final String nextToken
    ) {
        return proxy.initiate("rds::list-blue-green-deployments", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(model -> Translator.describeBlueGreenDeploymentsRequest(nextToken))
                .backoffDelay(config.getBackoff())
                .makeServiceCall((request, client) -> client.injectCredentialsAndInvokeV2(
                        request,
                        client.client()::describeBlueGreenDeployments
                ))
                .handleError((request, exception, client, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        DEFAULT_BLUE_GREEN_DEPLOYMENT_ERROR_RULE_SET
                ))
                .done((request, response, client, model, context) -> ProgressEvent.<ResourceModel, CallbackContext>builder()
                        .resourceModels(
                                Optional.ofNullable(response.blueGreenDeployments()).orElse(Collections.emptyList())
                                        .stream()
                                        .map(Translator::translateBlueGreenDeploymentFromSdk)
                                        .collect(Collectors.toList())
                        )
                        .nextToken(response.marker())
                        .status(OperationStatus.SUCCESS)
                        .build());
    }
}
