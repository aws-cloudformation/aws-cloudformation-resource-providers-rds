package software.amazon.rds.bluegreendeployment;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.common.util.IdentifierFactory;

public class CreateHandler extends BaseHandlerStd {

    private final static IdentifierFactory dbClusterIdentifierFactory = new IdentifierFactory(
            STACK_NAME,
            RESOURCE_IDENTIFIER,
            RESOURCE_ID_MAX_LENGTH
    );

    public CreateHandler() {
        this(DEFAULT_HANDLER_CONFIG);
    }

    public CreateHandler(final HandlerConfig config) {
        super(config);
    }

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final RequestLogger logger
    ) {
        final ResourceModel model = request.getDesiredResourceState();

        if (StringUtils.isNullOrEmpty(model.getBlueGreenDeploymentName())) {
            model.setBlueGreenDeploymentName(dbClusterIdentifierFactory.newIdentifier()
                    .withStackId(request.getStackId())
                    .withResourceId(request.getLogicalResourceIdentifier())
                    .withRequestToken(request.getClientRequestToken())
                    .toString());
        }

        final Tagging.TagSet tags = Tagging.TagSet.builder()
                //TODO
                //.systemTags(Tagging.translateTagsToSdk(request.getSystemTags()))
                //.stackTags(Tagging.translateTagsToSdk(request.getDesiredResourceTags()))
                .resourceTags(Translator.translateTagsToSdk(request.getDesiredResourceState().getTags()))
                .build();

        return ProgressEvent.progress(model, callbackContext)
                .then(progress -> createBlueGreenDeployment(proxy, proxyClient, progress, tags))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private ProgressEvent<ResourceModel, CallbackContext> createBlueGreenDeployment(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet tags
    ) {
        return proxy.initiate("rds::create-blue-green-deployment", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(model -> Translator.createBlueGreenDeploymentRequest(model, tags))
                .backoffDelay(config.getBackoff())
                .makeServiceCall((request, client) -> client.injectCredentialsAndInvokeV2(
                        request, client.client()::createBlueGreenDeployment
                ))
                .stabilize((request, response, client, model, context) -> {
                    if (StringUtils.isNullOrEmpty(model.getBlueGreenDeploymentIdentifier())) {
                        model.setBlueGreenDeploymentIdentifier(response.blueGreenDeployment().blueGreenDeploymentIdentifier());
                    }
                    return isBlueGreenDeploymentStabilized(client, model);
                })
                .handleError((request, exception, client, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        DEFAULT_BLUE_GREEN_DEPLOYMENT_ERROR_RULE_SET
                ))
                .done((request, response, client, model, context) -> {
                    if (StringUtils.isNullOrEmpty(model.getBlueGreenDeploymentIdentifier())) {
                        model.setBlueGreenDeploymentIdentifier(response.blueGreenDeployment().blueGreenDeploymentIdentifier());
                    }
                    return ProgressEvent.progress(model, context);
                });
    }
}
