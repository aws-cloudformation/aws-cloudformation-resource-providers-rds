package software.amazon.rds.integration;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.IntegrationAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.IntegrationConflictOperationException;
import software.amazon.awssdk.services.rds.model.IntegrationNotFoundException;
import software.amazon.awssdk.services.rds.model.IntegrationQuotaExceededException;
import software.amazon.awssdk.services.rds.model.IntegrationStatus;
import software.amazon.awssdk.services.rds.model.KmsKeyNotAccessibleException;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.error.ErrorRuleSet;
import software.amazon.rds.common.error.ErrorStatus;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.logging.LoggingProxyClient;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.common.printer.FilteredJsonPrinter;

import java.util.Collection;

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {
    protected static final String STACK_NAME = "rds";
    protected static final String RESOURCE_IDENTIFIER = "integration";
    protected static final int MAX_LENGTH_INTEGRATION = 63;

    protected static final ErrorRuleSet DEFAULT_INTEGRATION_ERROR_RULE_SET = ErrorRuleSet
            .extend(Commons.DEFAULT_ERROR_RULE_SET)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    IntegrationAlreadyExistsException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    IntegrationNotFoundException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
                    IntegrationConflictOperationException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ServiceLimitExceeded),
                    IntegrationQuotaExceededException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.AccessDenied),
                    KmsKeyNotAccessibleException.class)
            .build();

    private final FilteredJsonPrinter PARAMETERS_FILTER = new FilteredJsonPrinter();
    private final IntegrationStatusUtil integrationStatusUtil;

    /** Custom handler config, mostly to facilitate faster unit test */
    final HandlerConfig config;
    protected RequestLogger requestLogger;

    public BaseHandlerStd() {
        this(HandlerConfig.builder().build());
    }

    BaseHandlerStd(HandlerConfig config) {
         this.config = config;
         this.integrationStatusUtil = new IntegrationStatusUtil();
    }

    @Override
    public final ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final   ResourceHandlerRequest<ResourceModel> request,
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


    /**
     * Integration is stablized when it's in active state.
     * @param model
     * @param proxyClient
     * @return
     */
    protected boolean isStabilized(final ResourceModel model, final ProxyClient<RdsClient> proxyClient) {
        final IntegrationStatus status = proxyClient.injectCredentialsAndInvokeV2(
                    Translator.describeIntegrationsRequest(model),
                    proxyClient.client()::describeIntegrations)
                .integrations().stream().findFirst().get().status();

        assertIntegrationInValidCreatingState(status);
        return integrationStatusUtil.isStabilizedState(status);
    }

    /**
     * Assert that the integration is in a valid state that can continue creating.
     *
     * @throws CfnNotStabilizedException if the integration is in a state that cannot continue creating.
     * @param integrationStatus
     */
    void assertIntegrationInValidCreatingState(IntegrationStatus integrationStatus) {
        if (!integrationStatusUtil.isValidCreatingStatus(integrationStatus)) {
            throw new CfnNotStabilizedException(
                    new Exception("Integration is in state a state that cannot complete creation: " + integrationStatus));
        }
    }


    protected ProgressEvent<ResourceModel, CallbackContext> updateTags(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet previousTags,
            final Tagging.TagSet desiredTags) {
        final Collection<software.amazon.awssdk.services.rds.model.Tag> effectivePreviousTags = Tagging.translateTagsToSdk(previousTags);
        final Collection<software.amazon.awssdk.services.rds.model.Tag> effectiveDesiredTags = Tagging.translateTagsToSdk(desiredTags);

        final Collection<software.amazon.awssdk.services.rds.model.Tag> tagsToRemove = Tagging.exclude(effectivePreviousTags, effectiveDesiredTags);
        final Collection<Tag> tagsToAdd = Tagging.exclude(effectiveDesiredTags, effectivePreviousTags);

        if (tagsToAdd.isEmpty() && tagsToRemove.isEmpty()) {
            return progress;
        }

        // TODO - we should call "add" on updated tags, not "remove and then add".
        final Tagging.TagSet rulesetTagsToAdd = Tagging.exclude(desiredTags, previousTags);
        final Tagging.TagSet rulesetTagsToRemove = Tagging.exclude(previousTags, desiredTags);

        String arn = progress.getCallbackContext().getIntegrationArn();
        if (arn == null) {
            ProgressEvent<ResourceModel, CallbackContext> progressEvent = fetchIntegrationArn(proxy, rdsProxyClient, progress);
            if (progressEvent.isFailed()) {
                return progressEvent;
            }
            arn = progressEvent.getCallbackContext().getIntegrationArn();
        }

        try {
            Tagging.removeTags(rdsProxyClient, arn, Tagging.translateTagsToSdk(tagsToRemove));
            Tagging.addTags(rdsProxyClient, arn, Tagging.translateTagsToSdk(tagsToAdd));
        } catch (Exception exception) {
            return Commons.handleException(
                    progress,
                    exception,
                    // Integration resource will NOT allow soft fail on tag updates.
                    DEFAULT_INTEGRATION_ERROR_RULE_SET,
                    requestLogger
            );
        }

        return progress;
    }

    protected ProgressEvent<ResourceModel, CallbackContext> fetchIntegrationArn(final AmazonWebServicesClientProxy proxy,
                                                                                      final ProxyClient<RdsClient> proxyClient,
                                                                                      final ProgressEvent<ResourceModel, CallbackContext> progress) {
        return proxy.initiate("rds::read-integration-arn", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(Translator::describeIntegrationsRequest)
                .makeServiceCall((describeIntegrationsRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(describeIntegrationsRequest, proxyInvocation.client()::describeIntegrations))
                .handleError((describeIntegrationsRequest, exception, client, resourceModel, ctx) ->
                        Commons.handleException(
                                ProgressEvent.progress(resourceModel, ctx),
                                exception,
                                DEFAULT_INTEGRATION_ERROR_RULE_SET,
                                requestLogger
                        ))
                .done((describeIntegrationsRequest, describeIntegrationsResponse, proxyInvocation, resourceModel, context) -> {
                    final String arn = describeIntegrationsResponse.integrations().stream().findFirst().get().integrationArn();
                    context.setIntegrationArn(arn);
                    return ProgressEvent.progress(resourceModel, context);
                });
    }
}
