package software.amazon.rds.customdbengineversion;

import java.time.Duration;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CustomDbEngineVersionAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.CustomDbEngineVersionNotFoundException;
import software.amazon.awssdk.services.rds.model.CustomDbEngineVersionQuotaExceededException;
import software.amazon.awssdk.services.rds.model.DBEngineVersion;
import software.amazon.awssdk.services.rds.model.DescribeDbEngineVersionsResponse;
import software.amazon.awssdk.services.rds.model.InvalidCustomDbEngineVersionStateException;
import software.amazon.awssdk.services.rds.model.InvalidS3BucketException;
import software.amazon.awssdk.services.rds.model.KmsKeyNotAccessibleException;
import software.amazon.awssdk.utils.StringUtils;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
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
    protected static final BiFunction<ResourceModel, ProxyClient<RdsClient>, ResourceModel> EMPTY_CALL = (model, proxyClient) -> model;

    protected static final String STACK_NAME = "rds";
    protected static final String RESOURCE_IDENTIFIER = "customdbengineversion";
    protected static final int RESOURCE_ID_MAX_LENGTH = 50;
    protected static final String IS_ALREADY_BEING_DELETED_ERROR_FRAGMENT = "is already being deleted";
    protected static final BiFunction<ResourceModel, ProxyClient<RdsClient>, ResourceModel> NOOP_CALL = (model, proxyClient) -> model;

    protected static final Function<Exception, ErrorStatus> ignoreCEVBeingDeletedConditionalErrorStatus = exception -> {
        if (isCEVBeingDeletedException(exception)) {
            return ErrorStatus.ignore(OperationStatus.IN_PROGRESS);
        }
        return ErrorStatus.failWith(HandlerErrorCode.ResourceConflict);
    };

    protected final static HandlerConfig CUSTOM_ENGINE_VERSION_HANDLER_CONFIG_10H = HandlerConfig.builder()
            .backoff(Constant.of()
                    .delay(Duration.ofSeconds(30))
                    .timeout(Duration.ofHours(10))
                    .build())
            .build();

    protected static final ErrorRuleSet DEFAULT_CUSTOM_DB_ENGINE_VERSION_ERROR_RULE_SET = ErrorRuleSet
            .extend(Commons.DEFAULT_ERROR_RULE_SET)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    CustomDbEngineVersionAlreadyExistsException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    CustomDbEngineVersionNotFoundException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
                    KmsKeyNotAccessibleException.class,
                    InvalidS3BucketException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ServiceLimitExceeded),
                    CustomDbEngineVersionQuotaExceededException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
                    InvalidCustomDbEngineVersionStateException.class)
            .build();

    protected static final ErrorRuleSet ACCESS_DENIED_TO_NOT_FOUND_ERROR_RULE_SET = ErrorRuleSet
            .extend(DEFAULT_CUSTOM_DB_ENGINE_VERSION_ERROR_RULE_SET)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    ErrorCode.AccessDenied)
            .withErrorClasses(ErrorStatus.conditional(ignoreCEVBeingDeletedConditionalErrorStatus),
                    InvalidCustomDbEngineVersionStateException.class)
            .build();

    private final FilteredJsonPrinter EMPTY_FILTER = new FilteredJsonPrinter();

    protected final HandlerConfig config;
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
                EMPTY_FILTER,
                requestLogger -> handleRequest(
                        proxy,
                        request,
                        callbackContext != null ? callbackContext : new CallbackContext(),
                        new LoggingProxyClient<>(requestLogger, proxy.newProxy(new ClientProvider()::getClient))
                ));
    }

    protected abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient);

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final RequestLogger requestLogger)
    {
        this.requestLogger = requestLogger;
        return handleRequest(proxy, request, callbackContext, proxyClient, requestLogger);
    }


    protected boolean isStabilized(final ResourceModel model, final ProxyClient<RdsClient> proxyClient) {
        try {
            final String status = fetchDBEngineVersion(model, proxyClient).status();
            assertNoCustomDbEngineVersionTerminalStatus(status);
            return status != null && CustomDBEngineVersionStatus.fromString(status).isStable();
        } catch (CustomDbEngineVersionNotFoundException exception) {
            return false;
        }
    }

    private void assertNoCustomDbEngineVersionTerminalStatus(final String source) throws CfnNotStabilizedException {
        CustomDBEngineVersionStatus status = CustomDBEngineVersionStatus.fromString(source);
        if (status != null && status.isTerminal()) {
            throw new CfnNotStabilizedException(new Exception("Custom DB Engine Version is in state: " + source + ""));
        }
    }

    protected DBEngineVersion fetchDBEngineVersion(final ResourceModel model,
                                                   final ProxyClient<RdsClient> proxyClient) {
        DescribeDbEngineVersionsResponse response = proxyClient.injectCredentialsAndInvokeV2(
                Translator.describeDbEngineVersionsRequest(model),
                proxyClient.client()::describeDBEngineVersions);

        final Optional<DBEngineVersion> engineVersion = response
                .dbEngineVersions().stream().findFirst();

        return engineVersion.orElseThrow(() -> CustomDbEngineVersionNotFoundException.builder().message(
                "CustomDBEngineVersion " + model.getEngineVersion() + " not found").build());
    }


    protected ProgressEvent<ResourceModel, CallbackContext> updateTags(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet previousTags,
            final Tagging.TagSet desiredTags) {
        final Tagging.TagSet tagsToAdd = Tagging.exclude(desiredTags, previousTags);
        final Tagging.TagSet tagsToRemove = Tagging.exclude(previousTags, desiredTags);

        if (tagsToAdd.isEmpty() && tagsToRemove.isEmpty()) {
            return progress;
        }

        ResourceModel model = progress.getResourceModel();
        if (StringUtils.isBlank(model.getDBEngineVersionArn())) {
            model.setDBEngineVersionArn(fetchDBEngineVersion(model, rdsProxyClient).dbEngineVersionArn());
        }

        final String arn = model.getDBEngineVersionArn();

        try {
            Tagging.removeTags(rdsProxyClient, arn, Tagging.translateTagsToSdk(tagsToRemove));
            Tagging.addTags(rdsProxyClient, arn, Tagging.translateTagsToSdk(tagsToAdd));
        } catch (Exception exception) {
            return getTaggingErrorRuleSet(progress, tagsToAdd, tagsToRemove, exception);
        }

        return progress;
    }

    private ProgressEvent<ResourceModel, CallbackContext> getTaggingErrorRuleSet(final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                                 final Tagging.TagSet tagsToAdd,
                                                                                 final Tagging.TagSet tagsToRemove,
                                                                                 final Exception exception) {
        return Commons.handleException(
                progress,
                exception,
                DEFAULT_CUSTOM_DB_ENGINE_VERSION_ERROR_RULE_SET.extendWith(
                        Tagging.getUpdateTagsAccessDeniedRuleSet(
                                tagsToAdd,
                                tagsToRemove
                        )
                ),
                requestLogger
        );
    }

    protected ProgressEvent<ResourceModel, CallbackContext> modifyCustomEngineVersion(final AmazonWebServicesClientProxy proxy,
                                                                                      final ProxyClient<RdsClient> proxyClient,
                                                                                      final ResourceModel previousModel,
                                                                                      final ProgressEvent<ResourceModel, CallbackContext> progress) {
        return proxy.initiate("rds::modify-custom-db-engine-version", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(model -> Translator.modifyCustomDbEngineVersionRequest(previousModel, model))
                .backoffDelay(config.getBackoff())
                .makeServiceCall((modifyCustomDbEngineVersionRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(modifyCustomDbEngineVersionRequest, proxyInvocation.client()::modifyCustomDBEngineVersion))
                .stabilize((modifyEventSubscriptionRequest, modifyEventSubscriptionResponse, proxyInvocation, resourceModel, context) ->
                        isStabilized(resourceModel, proxyInvocation))
                .handleError((modifyRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        ACCESS_DENIED_TO_NOT_FOUND_ERROR_RULE_SET,
                        requestLogger))
                .progress();
    }

    private static boolean looksLikeCEVBeingDeletedMessage(final String message) {
        if (StringUtils.isBlank(message)) {
            return false;
        }
        return message.contains(IS_ALREADY_BEING_DELETED_ERROR_FRAGMENT);
    }

    private static boolean isCEVBeingDeletedException(final Exception e) {
        if (e instanceof InvalidCustomDbEngineVersionStateException) {
            return looksLikeCEVBeingDeletedMessage(e.getMessage());
        }
        return false;
    }
}
