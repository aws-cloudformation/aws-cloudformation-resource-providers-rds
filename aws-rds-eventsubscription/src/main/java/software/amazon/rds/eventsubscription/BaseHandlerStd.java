package software.amazon.rds.eventsubscription;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.EventSubscriptionQuotaExceededException;
import software.amazon.awssdk.services.rds.model.InvalidEventSubscriptionStateException;
import software.amazon.awssdk.services.rds.model.SubscriptionAlreadyExistException;
import software.amazon.awssdk.services.rds.model.SubscriptionNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.error.ErrorRuleSet;
import software.amazon.rds.common.error.ErrorStatus;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.logging.LoggingProxyClient;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.common.printer.FilteredJsonPrinter;

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {
    protected static final BiFunction<ResourceModel, ProxyClient<RdsClient>, ResourceModel> EMPTY_CALL = (model, proxyClient) -> model;

    protected static final ErrorRuleSet DEFAULT_EVENT_SUBSCRIPTION_ERROR_RULE_SET = ErrorRuleSet.builder()
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    SubscriptionAlreadyExistException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    SubscriptionNotFoundException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ServiceLimitExceeded),
                    EventSubscriptionQuotaExceededException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
                    InvalidEventSubscriptionStateException.class)
            .build()
            .orElse(Commons.DEFAULT_ERROR_RULE_SET);

    private final FilteredJsonPrinter PARAMETERS_FILTER = new FilteredJsonPrinter();

    @Override
    public final ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger) {
        return RequestLogger.handleRequest(
                logger,
                request,
                PARAMETERS_FILTER,
                requestLogger -> handleRequest(
                        proxy,
                        request,
                        callbackContext != null ? callbackContext : new CallbackContext(),
                        new LoggingProxyClient<>(requestLogger, proxy.newProxy(ClientBuilder::getClient)),
                        logger
                ));
    }

    protected abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger);


    protected boolean isStabilized(final ResourceModel model, final ProxyClient<RdsClient> proxyClient) {
        final String status = proxyClient.injectCredentialsAndInvokeV2(
                Translator.describeEventSubscriptionsRequest(model),
                proxyClient.client()::describeEventSubscriptions)
                .eventSubscriptionsList().stream().findFirst().get().status();
        return status.equals("active");
    }

    protected ProgressEvent<ResourceModel, CallbackContext> waitForEventSubscription(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress) {
        return proxy.initiate("rds::stabilize-event-subscription" + getClass().getSimpleName(), proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                // only stabilization is necessary so this is a dummy call
                // Function.identity() takes ResourceModel as an input and returns (the same) ResourceModel
                // Function.identity() is roughly similar to `model -> model`
                .translateToServiceRequest(Function.identity())
                // this skips the call and goes directly to stabilization
                .makeServiceCall(EMPTY_CALL)
                .stabilize((resourceModel, response, proxyInvocation, model, callbackContext) -> isStabilized(resourceModel, proxyInvocation)).progress();
    }

    protected ProgressEvent<ResourceModel, CallbackContext> tagResource(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Map<String, String> previousTags,
            final Map<String, String> desiredTags) {
        return proxy.initiate("rds::tag-event-subscription", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(Translator::describeEventSubscriptionsRequest)
                .makeServiceCall((describeEventSubscriptionsRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(describeEventSubscriptionsRequest, proxyInvocation.client()::describeEventSubscriptions))
                .done((describeEventSubscriptionsRequest, describeEventSubscriptionsResponse, proxyInvocation, resourceModel, context) -> {
                    final String arn = describeEventSubscriptionsResponse.eventSubscriptionsList().stream().findFirst().get().eventSubscriptionArn();
                    return Tagging.updateTags(
                            proxyInvocation,
                            arn,
                            ProgressEvent.progress(resourceModel, context),
                            previousTags,
                            desiredTags,
                            DEFAULT_EVENT_SUBSCRIPTION_ERROR_RULE_SET
                    );
                });
    }

}
