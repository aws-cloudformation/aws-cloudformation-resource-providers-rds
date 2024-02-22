package software.amazon.rds.integration;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.IntegrationConflictOperationException;
import software.amazon.awssdk.services.rds.model.IntegrationNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.error.ErrorRuleSet;
import software.amazon.rds.common.error.ErrorStatus;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;

import java.util.Optional;

public class DeleteHandler extends BaseHandlerStd {
    // Currently, if you re-create an Integration within 500 seconds of deletion against the same cluster,
    // The Integration may fail to create. Remove when the issue no longer exists.
    static final int POST_DELETION_DELAY_SEC = 500;
    static final int CALLBACK_DELAY = 6;

    /** Default constructor w/ default backoff */
    public DeleteHandler() {}

    /** Default constructor w/ custom config */
    public DeleteHandler(HandlerConfig config) {
        super(config);
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext) {
        return checkIfIntegrationExists(proxy, request, callbackContext, proxyClient)
                .then((evt) -> proxy.initiate("rds::delete-integration", proxyClient, request.getDesiredResourceState(), callbackContext)
                        .translateToServiceRequest(Translator::deleteIntegrationRequest)
                        .backoffDelay(config.getBackoff())
                        .makeServiceCall((deleteIntegrationRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(deleteIntegrationRequest, proxyInvocation.client()::deleteIntegration))
                        .stabilize((deleteIntegrationRequest, deleteIntegrationResponse, proxyInvocation, model, context) ->
                                isDeleted(model, proxyInvocation))
                        .handleError((deleteRequest, exception, client, resourceModel, ctx) -> {
                            if (IntegrationConflictOperationException.class.isAssignableFrom(exception.getClass())) {
                                String nonNullErrorMessage = Optional.ofNullable(exception.getMessage()).orElse("");
                                if (nonNullErrorMessage.contains(INTEGRATION_RETRIABLE_CONFLICT_MESSAGE)) {
                                    // this tells the cfn framework to come back in a bit.
                                    return ProgressEvent.defaultInProgressHandler(ctx, CALLBACK_DELAY, resourceModel);
                                }
                            }
                            return Commons.handleException(
                                    ProgressEvent.progress(resourceModel, ctx),
                                    exception,
                                    // if the integration is already deleted, this should be ignored,
                                    // but only once we started the deletion process
                                    ErrorRuleSet.extend(DEFAULT_INTEGRATION_ERROR_RULE_SET)
                                            .withErrorClasses(ErrorStatus.ignore(), IntegrationNotFoundException.class)
                                            .build(),
                                    requestLogger
                            );
                        }
                        )
                        .progress()
                        .then((e) -> delay(e, POST_DELETION_DELAY_SEC))
                        .then((e) -> ProgressEvent.defaultSuccessHandler(null)));
    }

    private ProgressEvent<ResourceModel, CallbackContext> checkIfIntegrationExists(final AmazonWebServicesClientProxy proxy,
                                                                                   final ResourceHandlerRequest<ResourceModel> request,
                                                                                   final CallbackContext callbackContext,
                                                                                   final ProxyClient<RdsClient> proxyClient) {
        // it is part of the CFN contract that we return NotFound on DELETE.
        return proxy.initiate("rds::delete-integration-check-exists", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::describeIntegrationsRequest)
                .backoffDelay(config.getBackoff())
                .makeServiceCall(((describeIntegrationsRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(describeIntegrationsRequest, proxyInvocation.client()::describeIntegrations)))
                .handleError((deleteRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DEFAULT_INTEGRATION_ERROR_RULE_SET,
                        requestLogger
                ))
                .progress();
    }

    /** Inserts an artificial delay */
    private ProgressEvent<ResourceModel, CallbackContext> delay(final ProgressEvent<ResourceModel, CallbackContext> evt, final int seconds) {
        CallbackContext callbackContext = evt.getCallbackContext();
        if (callbackContext.getDeleteWaitTime() <= seconds) {
            callbackContext.setDeleteWaitTime(callbackContext.getDeleteWaitTime() + CALLBACK_DELAY);
            return ProgressEvent.defaultInProgressHandler(callbackContext, CALLBACK_DELAY, evt.getResourceModel());
        } else {
            return ProgressEvent.progress(evt.getResourceModel(), callbackContext);
        }
    }

    protected boolean isDeleted(final ResourceModel model,
                                final ProxyClient<RdsClient> proxyClient) {
        try {
            proxyClient.injectCredentialsAndInvokeV2(Translator.describeIntegrationsRequest(model), proxyClient.client()::describeIntegrations);
            return false;
        } catch (IntegrationNotFoundException e) {
            return true;
        }
    }
}
