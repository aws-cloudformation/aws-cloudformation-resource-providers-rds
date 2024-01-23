package software.amazon.rds.eventsubscription;

import java.util.stream.Collectors;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeEventSubscriptionsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;

public class ListHandler extends BaseHandlerStd {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger) {


        DescribeEventSubscriptionsResponse describeEventSubscriptionsResponse;
        try {
            describeEventSubscriptionsResponse = proxy.injectCredentialsAndInvokeV2(
                    Translator.describeEventSubscriptionsRequest(request.getNextToken()),
                    proxyClient.client()::describeEventSubscriptions);
        } catch (Exception e) {
            return Commons.handleException(
                    ProgressEvent.progress(request.getDesiredResourceState(), callbackContext),
                    e,
                    DEFAULT_EVENT_SUBSCRIPTION_ERROR_RULE_SET,
                    requestLogger
            );
        }

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                .resourceModels(
                        describeEventSubscriptionsResponse.eventSubscriptionsList()
                                .stream()
                                .map(eventSubscription -> ResourceModel.builder()
                                        .subscriptionName(eventSubscription.custSubscriptionId())
                                        .build()
                                ).collect(Collectors.toList())
                ).nextToken(describeEventSubscriptionsResponse.marker())
                .status(OperationStatus.SUCCESS)
                .build();
    }
}
