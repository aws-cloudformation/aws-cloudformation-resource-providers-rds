package software.amazon.rds.customdbengineversion;

import java.time.Duration;
import java.util.function.BiFunction;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CustomDbEngineVersionAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.CustomDbEngineVersionNotFoundException;
import software.amazon.awssdk.services.rds.model.CustomDbEngineVersionQuotaExceededException;
import software.amazon.awssdk.services.rds.model.CustomEngineVersionStatus;
import software.amazon.awssdk.services.rds.model.InvalidCustomDbEngineVersionStateException;
import software.amazon.awssdk.services.rds.model.InvalidS3BucketException;
import software.amazon.awssdk.services.rds.model.KmsKeyNotAccessibleException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;
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
    protected static final String DEFAULT_ENGINE_NAME_PREFIX = "19.";
    protected static final int RESOURCE_ID_MAX_LENGTH = 50;

    protected static final Constant BACKOFF_DELAY = Constant.of()
            .timeout(Duration.ofSeconds(180L))
            .delay(Duration.ofSeconds(15L))
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

    private final FilteredJsonPrinter PARAMETERS_FILTER = new FilteredJsonPrinter();

    protected final HandlerConfig config;

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
                        request,
                        callbackContext != null ? callbackContext : new CallbackContext(),
                        new LoggingProxyClient<>(requestLogger, proxy.newProxy(new ClientProvider()::getClient)),
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
                Translator.describeDbEngineVersionsRequest(model),
                proxyClient.client()::describeDBEngineVersions)
                .dbEngineVersions().stream().findFirst().get().status();
        return !CustomEngineVersionStatus.UNKNOWN_TO_SDK_VERSION.equals(CustomEngineVersionStatus.fromValue(status));
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

        String arn = progress.getResourceModel().getDBEngineVersionArn();

        try {
            Tagging.removeTags(rdsProxyClient, arn, Tagging.translateTagsToSdk(tagsToRemove));
            Tagging.addTags(rdsProxyClient, arn, Tagging.translateTagsToSdk(tagsToAdd));
        } catch (Exception exception) {
            return Commons.handleException(
                    progress,
                    exception,
                    DEFAULT_CUSTOM_DB_ENGINE_VERSION_ERROR_RULE_SET.extendWith(
                            Tagging.bestEffortErrorRuleSet(
                                    tagsToAdd,
                                    tagsToRemove,
                                    Tagging.SOFT_FAIL_IN_PROGRESS_TAGGING_ERROR_RULE_SET,
                                    Tagging.HARD_FAIL_TAG_ERROR_RULE_SET
                            )
                    )
            );
        }

        return progress;
    }

    protected ProgressEvent<ResourceModel, CallbackContext> updateCustomEngineVersion(final AmazonWebServicesClientProxy proxy,
                                                                                      final ProxyClient<RdsClient> proxyClient,
                                                                                      final ProgressEvent<ResourceModel, CallbackContext> progress) {
        return proxy.initiate("rds::update-custom-db-engine-version", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(Translator::modifyCustomDbEngineVersionRequest)
                .backoffDelay(config.getBackoff())
                .makeServiceCall((modifyCustomDbEngineVersionRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(modifyCustomDbEngineVersionRequest, proxyInvocation.client()::modifyCustomDBEngineVersion))
                .stabilize((modifyEventSubscriptionRequest, modifyEventSubscriptionResponse, proxyInvocation, resourceModel, context) ->
                        isStabilized(resourceModel, proxyInvocation))
                .handleError((modifyRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DEFAULT_CUSTOM_DB_ENGINE_VERSION_ERROR_RULE_SET))
                .progress();
    }
}
