package software.amazon.rds.optiongroup;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.OptionGroupAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.OptionGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.OptionGroupQuotaExceededException;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.error.ErrorRuleSet;
import software.amazon.rds.common.error.ErrorStatus;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.logging.LoggingProxyClient;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.common.printer.FilteredJsonPrinter;

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {

    protected static final String STACK_NAME = "rds";
    protected static final String RESOURCE_IDENTIFIER = "optiongroup";
    protected static final String OPTION_GROUP_REQUEST_STARTED_AT = "optiongroup-request-started-at";
    protected static final String OPTION_GROUP_REQUEST_IN_PROGRESS_AT = "optiongroup-request-in-progress-at";
    protected static final String OPTION_GROUP_STABILIZATION_TIME = "optiongroup-stabilization-time";
    protected static final int RESOURCE_ID_MAX_LENGTH = 255;

    protected static final Constant BACKOFF_DELAY = Constant.of()
            .timeout(Duration.ofSeconds(150L))
            .delay(Duration.ofSeconds(5L))
            .build();

    protected static final ErrorRuleSet DEFAULT_OPTION_GROUP_ERROR_RULE_SET = ErrorRuleSet
            .extend(Commons.DEFAULT_ERROR_RULE_SET)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
                    ErrorCode.InvalidOptionGroupStateFault)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    OptionGroupAlreadyExistsException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    OptionGroupNotFoundException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ServiceLimitExceeded),
                    OptionGroupQuotaExceededException.class)
            .build();


    private static final FilteredJsonPrinter PARAMETERS_FILTER = new FilteredJsonPrinter();
    protected HandlerConfig config;
    protected RequestLogger requestLogger;

    public BaseHandlerStd(final HandlerConfig config) {
        super();
        this.config = config;
    }

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
                        new LoggingProxyClient<>(requestLogger, proxy.newProxy(new ClientBuilder()::getClient)), request,
                        callbackContext != null ? callbackContext : new CallbackContext(), requestLogger
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
    ) {
        this.requestLogger = requestLogger;
        resourceStabilizationTime(callbackContext);
        return handleRequest(proxy, proxyClient, request, callbackContext);
    };

    protected ProgressEvent<ResourceModel, CallbackContext> updateOptionGroupConfigurations(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress) {
        return proxy.initiate("rds::update-option-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(Translator::modifyOptionGroupRequest)
                .makeServiceCall((modifyRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        modifyRequest,
                        proxyInvocation.client()::modifyOptionGroup
                ))
                .handleError((describeRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DEFAULT_OPTION_GROUP_ERROR_RULE_SET,
                        requestLogger
                ))
                .progress();
    }

    private void resourceStabilizationTime(final CallbackContext callbackContext) {
        callbackContext.timestampOnce(OPTION_GROUP_REQUEST_STARTED_AT, Instant.now());
        callbackContext.timestamp(OPTION_GROUP_REQUEST_IN_PROGRESS_AT, Instant.now());
        callbackContext.calculateTimeDeltaInMinutes(OPTION_GROUP_STABILIZATION_TIME,
                callbackContext.getTimestamp(OPTION_GROUP_REQUEST_IN_PROGRESS_AT),
                callbackContext.getTimestamp(OPTION_GROUP_REQUEST_STARTED_AT));
    }

    protected ProgressEvent<ResourceModel, CallbackContext> updateTags(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet previousTags,
            final Tagging.TagSet desiredTags
    ) {
        return proxy.initiate("rds::tag-option-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(Translator::describeOptionGroupsRequest)
                .makeServiceCall((describeRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        describeRequest,
                        proxyInvocation.client()::describeOptionGroups
                )).handleError((describeRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DEFAULT_OPTION_GROUP_ERROR_RULE_SET,
                        requestLogger
                ))
                .done((describeRequest, describeResponse, invocation, resourceModel, ctx) -> {
                    final Collection<Tag> effectivePreviousTags = Tagging.translateTagsToSdk(previousTags);
                    final Collection<Tag> effectiveDesiredTags = Tagging.translateTagsToSdk(desiredTags);

                    final Collection<Tag> tagsToRemove = Tagging.exclude(effectivePreviousTags, effectiveDesiredTags);
                    final Collection<Tag> tagsToAdd = Tagging.exclude(effectiveDesiredTags, effectivePreviousTags);

                    if (tagsToAdd.isEmpty() && tagsToRemove.isEmpty()) {
                        return progress;
                    }

                    final Tagging.TagSet rulesetTagsToAdd = Tagging.exclude(desiredTags, previousTags);
                    final Tagging.TagSet rulesetTagsToRemove = Tagging.exclude(previousTags, desiredTags);


                    final String arn = describeResponse.optionGroupsList().stream().findFirst().get().optionGroupArn();
                    try {
                        Tagging.removeTags(proxyClient, arn, Tagging.translateTagsToSdk(tagsToRemove));
                        Tagging.addTags(proxyClient, arn, Tagging.translateTagsToSdk(tagsToAdd));
                    } catch (Exception exception) {
                        return Commons.handleException(
                                progress,
                                exception,
                                DEFAULT_OPTION_GROUP_ERROR_RULE_SET.extendWith(
                                        Tagging.getUpdateTagsAccessDeniedRuleSet(
                                                rulesetTagsToAdd,
                                                rulesetTagsToRemove
                                        )
                                ),
                                requestLogger
                        );
                    }
                    return ProgressEvent.progress(resourceModel, ctx);
                });
    }
}
