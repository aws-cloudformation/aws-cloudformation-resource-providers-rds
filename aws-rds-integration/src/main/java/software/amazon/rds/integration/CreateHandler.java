package software.amazon.rds.integration;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.IntegrationConflictOperationException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.util.IdentifierFactory;

import java.util.HashSet;
import java.util.Optional;

public class CreateHandler extends BaseHandlerStd {

    private final static String INTEGRATION_NAME_CONFLICT_ERROR_MESSAGE = "Integration names must be unique within an account";
    private final static IdentifierFactory integrationNameFactory = new IdentifierFactory(
            STACK_NAME,
            RESOURCE_IDENTIFIER,
            MAX_LENGTH_INTEGRATION
    );

    /** Default constructor w/ default backoff */
    public CreateHandler() {
    }

    /** Default constructor w/ custom config */
    public CreateHandler(HandlerConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();

        final Tagging.TagSet allTags = Tagging.TagSet.builder()
                .systemTags(Tagging.translateTagsToSdk(request.getSystemTags()))
                .stackTags(Tagging.translateTagsToSdk(request.getDesiredResourceTags()))
                .resourceTags(new HashSet<>(Translator.translateTagsToSdk(request.getDesiredResourceState().getTags())))
                .build();

        return ProgressEvent.progress(model, callbackContext)
                .then(progress -> setIntegrationNameIfEmpty(request, progress))
                .then(progress -> createIntegration(proxy, proxyClient, progress, allTags))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private ProgressEvent<ResourceModel, CallbackContext> createIntegration(final AmazonWebServicesClientProxy proxy,
                                                                                  final ProxyClient<RdsClient> proxyClient,
                                                                                  final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                                  final Tagging.TagSet tags) {
        return proxy.initiate("rds::create-integration", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest((resourceModel) -> Translator.createIntegrationRequest(resourceModel, tags))
                .backoffDelay(config.getBackoff())
                .makeServiceCall((createIntegrationRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(createIntegrationRequest, proxyInvocation.client()::createIntegration))
                .stabilize((createIntegrationRequest, createIntegrationResponse, proxyInvocation, resourceModel, context) -> {
                    // with the response, now we'd know what the ARN is.
                    resourceModel.setIntegrationArn(
                            Optional.ofNullable(resourceModel.getIntegrationArn()).orElse(createIntegrationResponse.integrationArn())
                    );
                    return isStabilized(resourceModel, proxyInvocation);
                })
                .handleError((createRequest, exception, client, resourceModel, ctx) -> {
                    // it's a little strange that IntegrationConflictOperationException is thrown instead of AlreadyExists exception
                    // we need to override the default error handling because in this case we need to tell CFN that it's an AlreadyExists.
                    if (IntegrationConflictOperationException.class.isAssignableFrom(exception.getClass())) {
                        if (Optional.ofNullable(exception.getMessage()).orElse("").contains(INTEGRATION_NAME_CONFLICT_ERROR_MESSAGE)) {
                            return ProgressEvent.failed(null, null, HandlerErrorCode.AlreadyExists, exception.getMessage());
                        }
                    }
                    return Commons.handleException(
                            ProgressEvent.progress(resourceModel, ctx),
                            exception,
                            DEFAULT_INTEGRATION_ERROR_RULE_SET,
                            requestLogger);
                })
                .progress();
    }

    private ProgressEvent<ResourceModel, CallbackContext> setIntegrationNameIfEmpty(final ResourceHandlerRequest<ResourceModel> request,
                                                                                    final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        ResourceModel model = progress.getResourceModel();
        if (StringUtils.isNullOrEmpty(model.getIntegrationName())) {
            model.setIntegrationName(integrationNameFactory.newIdentifier()
                    .withStackId(request.getStackId())
                    .withResourceId(request.getLogicalResourceIdentifier())
                    .withRequestToken(request.getClientRequestToken())
                    .toString());
        }
        return progress;
    }
}
