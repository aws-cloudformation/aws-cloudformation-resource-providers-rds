package software.amazon.rds.integration;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.IntegrationConflictOperationException;
import software.amazon.awssdk.services.rds.model.InvalidIntegrationStateException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.error.ErrorRuleSet;
import software.amazon.rds.common.error.ErrorStatus;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.util.IdempotencyHelper;
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

    /** We cannot "retry" create API call if we do not know the primaryIdentifier
     * (in this case, the integrationArn is the primaryIdentifier).
     * The integration ARN is only vended on a successful create.
     * A CFN createHandler MUST return a valid primaryIdentifier on the first lambda invocation.
     * Since this is not possible on a failed create-integration call,
     * we will just fail on a certain set of retriable conditions.
     */
    protected static final ErrorRuleSet CREATE_INTEGRATION_ERROR_RULE_SET = ErrorRuleSet
            .extend(DEFAULT_INTEGRATION_ERROR_RULE_SET)
            .withErrorClasses(ErrorStatus.conditional((e) -> {
                String nonNullErrorMessage = Optional.ofNullable(e.getMessage()).orElse("");
                // this condition happens on create-integration.
                // it's a little strange that IntegrationConflictOperationException is thrown instead of AlreadyExists exception
                // we need to override the default error handling because in this case we need to tell CFN that it's an AlreadyExists.
                if (nonNullErrorMessage.contains(INTEGRATION_NAME_CONFLICT_ERROR_MESSAGE)) {
                    return ErrorStatus.failWith(HandlerErrorCode.AlreadyExists);
                } else {
                    // we cannot retry for create
                    return ErrorStatus.failWith(HandlerErrorCode.ResourceConflict);
                }
            }), IntegrationConflictOperationException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict), InvalidIntegrationStateException.class)
            .build();

    /** Default constructor w/ default backoff */
    public CreateHandler() {
        this(HandlerConfig.builder()
                .backoff(CREATE_UPDATE_DELAY)
                .build());
    }

    /** Default constructor w/ custom config */
    public CreateHandler(HandlerConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext) {

        final ResourceModel model = request.getDesiredResourceState();

        final Tagging.TagSet allTags = Tagging.TagSet.builder()
                .systemTags(Tagging.translateTagsToSdk(request.getSystemTags()))
                .stackTags(Tagging.translateTagsToSdk(request.getDesiredResourceTags()))
                .resourceTags(new HashSet<>(Translator.translateTagsToSdk(request.getDesiredResourceState().getTags())))
                .build();

        return ProgressEvent.progress(model, callbackContext)
                .then(progress -> setIntegrationNameIfEmpty(request, progress))
                .then(progress -> IdempotencyHelper.safeCreate(
                    m -> fetchIntegration(proxyClient, m),
                    p -> createIntegration(proxy, proxyClient, p, allTags),
                    ResourceModel.TYPE_NAME, model.getIntegrationName(), progress, requestLogger))
                .then(progress -> new ReadHandler().handleRequest(proxy, proxyClient, request, callbackContext));
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
                    resourceModel.setIntegrationArn(createIntegrationResponse.integrationArn());
                    return isStabilized(resourceModel, proxyInvocation);
                })
                .handleError((createRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        CREATE_INTEGRATION_ERROR_RULE_SET,
                        requestLogger))
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
