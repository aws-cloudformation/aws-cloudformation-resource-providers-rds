package software.amazon.rds.integration;


import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.Integration;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;

public class ReadHandler extends BaseHandlerStd {
    /** Default constructor w/ default backoff */
    public ReadHandler() {}

    /** Default constructor w/ custom config */
    public ReadHandler(HandlerConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext
    ) {
        return proxy.initiate("rds::describe-integration", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::describeIntegrationsRequest)
                .backoffDelay(config.getBackoff())
                .makeServiceCall((describeIntegrationsRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(describeIntegrationsRequest, proxyInvocation.client()::describeIntegrations))
                .handleError((describeRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DEFAULT_INTEGRATION_ERROR_RULE_SET, requestLogger))
                .done((describeIntegrationsRequest, describeIntegrationsResponse, proxyInvocation, model, context) -> {
                    final Integration integration = describeIntegrationsResponse.integrations().stream().findFirst().get();
                    // it's possible the model does not have the ARN populated yet,
                    // so we can just be conservative and populate it at all times.
                    model.setIntegrationArn(integration.integrationArn());
                    return ProgressEvent.success(Translator.translateToModel(integration), context);
                });
    }

}
