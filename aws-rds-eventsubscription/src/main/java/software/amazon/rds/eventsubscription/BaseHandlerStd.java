package software.amazon.rds.eventsubscription;

import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Function;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.EventSubscriptionQuotaExceededException;
import software.amazon.awssdk.services.rds.model.InvalidEventSubscriptionStateException;
import software.amazon.awssdk.services.rds.model.SnsTopicArnNotFoundException;
import software.amazon.awssdk.services.rds.model.SourceNotFoundException;
import software.amazon.awssdk.services.rds.model.SubscriptionAlreadyExistException;
import software.amazon.awssdk.services.rds.model.SubscriptionNotFoundException;
import software.amazon.awssdk.services.rds.model.Tag;
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

    protected static final String STACK_NAME = "rds";
    protected static final String RESOURCE_IDENTIFIER = "eventsubscription";
    protected static final int MAX_LENGTH_EVENT_SUBSCRIPTION = 255;
    protected RequestLogger requestLogger;

    protected static final ErrorRuleSet DEFAULT_EVENT_SUBSCRIPTION_ERROR_RULE_SET = ErrorRuleSet
            .extend(Commons.DEFAULT_ERROR_RULE_SET)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    SubscriptionAlreadyExistException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    SubscriptionNotFoundException.class,
                    SnsTopicArnNotFoundException.class,
                    SourceNotFoundException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ServiceLimitExceeded),
                    EventSubscriptionQuotaExceededException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
                    InvalidEventSubscriptionStateException.class)
            .build();

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
                        new LoggingProxyClient<>(requestLogger, proxy.newProxy(new ClientProvider()::getClient)), request,
                        callbackContext != null ? callbackContext : new CallbackContext()
                ));
    }

    protected abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext);


    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final RequestLogger requestLogger
    )
    {
        this.requestLogger = requestLogger;
        return handleRequest(proxy, proxyClient, request, callbackContext);
    }


    protected boolean isStabilized(final ResourceModel model, final ProxyClient<RdsClient> proxyClient) {
        final String status = proxyClient.injectCredentialsAndInvokeV2(
                    Translator.describeEventSubscriptionsRequest(model),
                    proxyClient.client()::describeEventSubscriptions)
                .eventSubscriptionsList().stream().findFirst().get().status();
        return status.equals("active");
    }

    protected ProgressEvent<ResourceModel, CallbackContext> setEnabledDefaultValue(
            final ProgressEvent<ResourceModel, CallbackContext> progress) {
        ResourceModel model = progress.getResourceModel();
        if (model.getEnabled() == null) {
            model.setEnabled(true);
        }
        return progress;
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

    protected ProgressEvent<ResourceModel, CallbackContext> updateTags(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet previousTags,
            final Tagging.TagSet desiredTags) {
        final Collection<Tag> effectivePreviousTags = Tagging.translateTagsToSdk(previousTags);
        final Collection<Tag> effectiveDesiredTags = Tagging.translateTagsToSdk(desiredTags);

        final Collection<Tag> tagsToRemove = Tagging.exclude(effectivePreviousTags, effectiveDesiredTags);
        final Collection<Tag> tagsToAdd = Tagging.exclude(effectiveDesiredTags, effectivePreviousTags);

        if (tagsToAdd.isEmpty() && tagsToRemove.isEmpty()) {
            return progress;
        }

        final Tagging.TagSet rulesetTagsToAdd = Tagging.exclude(desiredTags, previousTags);
        final Tagging.TagSet rulesetTagsToRemove = Tagging.exclude(previousTags, desiredTags);

        String arn = progress.getCallbackContext().getEventSubscriptionArn();
        if (arn == null) {
            ProgressEvent<ResourceModel, CallbackContext> progressEvent = fetchEventSubscriptionArn(proxy, rdsProxyClient, progress);
            if (progressEvent.isFailed()) {
                return progressEvent;
            }
            arn = progressEvent.getCallbackContext().getEventSubscriptionArn();
        }

        try {
            Tagging.removeTags(rdsProxyClient, arn, Tagging.translateTagsToSdk(tagsToRemove));
            Tagging.addTags(rdsProxyClient, arn, Tagging.translateTagsToSdk(tagsToAdd));
        } catch (Exception exception) {
            return Commons.handleException(
                    progress,
                    exception,
                    DEFAULT_EVENT_SUBSCRIPTION_ERROR_RULE_SET.extendWith(
                                    Tagging.getUpdateTagsAccessDeniedRuleSet(
                                            rulesetTagsToAdd,
                                            rulesetTagsToRemove
                                    )
                            ), requestLogger

            );
        }

        return progress;
    }

    protected ProgressEvent<ResourceModel, CallbackContext> fetchEventSubscriptionArn(final AmazonWebServicesClientProxy proxy,
                                                                                      final ProxyClient<RdsClient> proxyClient,
                                                                                      final ProgressEvent<ResourceModel, CallbackContext> progress) {
        return proxy.initiate("rds::read-db-parameter-group-arn", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(Translator::describeEventSubscriptionsRequest)
                .makeServiceCall((describeEventSubscriptionsRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(describeEventSubscriptionsRequest, proxyInvocation.client()::describeEventSubscriptions))
                .handleError((describeDbParameterGroupsRequest, exception, client, resourceModel, ctx) ->
                        Commons.handleException(
                                ProgressEvent.progress(resourceModel, ctx),
                                exception,
                                DEFAULT_EVENT_SUBSCRIPTION_ERROR_RULE_SET,
                                requestLogger
                        ))
                .done((describeEventSubscriptionsRequest, describeEventSubscriptionsResponse, proxyInvocation, resourceModel, context) -> {
                    final String arn = describeEventSubscriptionsResponse.eventSubscriptionsList().stream().findFirst().get().eventSubscriptionArn();
                    context.setEventSubscriptionArn(arn);
                    return ProgressEvent.progress(resourceModel, context);
                });
    }
}
