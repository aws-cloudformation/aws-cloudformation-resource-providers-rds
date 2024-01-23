package software.amazon.rds.integration;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeIntegrationsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import javax.annotation.Nullable;

import java.util.stream.Collectors;

public class ListHandler extends BaseHandlerStd {

    /** Default constructor w/ default backoff */
    public ListHandler() {}

    /** Default constructor w/ custom config */
    public ListHandler(HandlerConfig config) {
        super(config);
    }

    @Override
    @Nullable
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger) {

        DescribeIntegrationsResponse describeIntegrationsResponse;
        try {
            describeIntegrationsResponse = proxy.injectCredentialsAndInvokeV2(
                    Translator.describeIntegrationsRequest(request.getNextToken()),
                    proxyClient.client()::describeIntegrations);
        } catch (Exception e) {
            return Commons.handleException(
                    ProgressEvent.progress(request.getDesiredResourceState(), callbackContext),
                    e,
                    DEFAULT_INTEGRATION_ERROR_RULE_SET,
                    requestLogger
            );
        }

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                .resourceModels(
                        describeIntegrationsResponse.integrations()
                                .stream()
                                .map(Translator::translateToModel).collect(Collectors.toList())
                ).nextToken(describeIntegrationsResponse.marker())
                .status(OperationStatus.SUCCESS)
                .build();
    }
}
