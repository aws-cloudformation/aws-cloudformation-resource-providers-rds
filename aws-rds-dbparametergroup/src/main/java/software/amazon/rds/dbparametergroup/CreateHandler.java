package software.amazon.rds.dbparametergroup;

import java.util.HashSet;
import java.util.Optional;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.resource.IdentifierUtils;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.logging.RequestLogger;


public class CreateHandler extends BaseHandlerStd {

    public static final String DEFAULT_LOGICAL_IDENTIFIER = "dbparametergroup";

    public CreateHandler() {
        this(HandlerConfig.builder().build());
    }

    public CreateHandler(final HandlerConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final RequestLogger requestLogger) {

        final ResourceModel desiredModel = request.getDesiredResourceState();

        final Tagging.TagSet systemTags = Tagging.TagSet.builder()
                .systemTags(Tagging.translateTagsToSdk(request.getSystemTags()))
                .build();

        final Tagging.TagSet extraTags = Tagging.TagSet.builder()
                .stackTags(Tagging.translateTagsToSdk(request.getDesiredResourceTags()))
                .resourceTags(new HashSet<>(Translator.translateTagsToSdk(request.getDesiredResourceState().getTags())))
                .build();

        return ProgressEvent.progress(desiredModel, callbackContext)
                .then(progress -> setDBParameterGroupNameIfEmpty(request, desiredModel, progress))
                .then(progress -> createDBParameterGroup(proxy, proxyClient, progress, systemTags, requestLogger))
                .then(progress -> updateTags(proxy, proxyClient, progress, Tagging.TagSet.emptySet(), extraTags, requestLogger))
                .then(progress -> applyParameters(proxy, proxyClient, progress, requestLogger))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, requestLogger));
    }

    private ProgressEvent<ResourceModel, CallbackContext> createDBParameterGroup(final AmazonWebServicesClientProxy proxy,
                                                                                 final ProxyClient<RdsClient> proxyClient,
                                                                                 final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                                 final Tagging.TagSet systemTags,
                                                                                 final RequestLogger requestLogger) {
        return proxy.initiate("rds::create-db-parameter-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(resourceModel -> Translator.createDbParameterGroupRequest(resourceModel, systemTags))
                .backoffDelay(config.getBackoff())
                .makeServiceCall((createDBParameterGroupRequest, proxyInvocation) ->
                        proxyInvocation.injectCredentialsAndInvokeV2(createDBParameterGroupRequest, proxyInvocation.client()::createDBParameterGroup))
                .handleError((createDBParameterGroupRequest, exception, client, resourceModel, ctx) ->
                        Commons.handleException(
                                ProgressEvent.progress(resourceModel, ctx),
                                exception,
                                DEFAULT_DB_PARAMETER_GROUP_ERROR_RULE_SET))
                .done((paramGroupRequest, paramGroupResponse, proxyInvocation, resourceModel, context) -> {
                    context.setDbParameterGroupArn(paramGroupResponse.dbParameterGroup().dbParameterGroupArn());
                    return ProgressEvent.progress(resourceModel, context);
                });
    }

    private ProgressEvent<ResourceModel, CallbackContext> setDBParameterGroupNameIfEmpty(final ResourceHandlerRequest<ResourceModel> request,
                                                                                         final ResourceModel model,
                                                                                         final ProgressEvent<ResourceModel, CallbackContext> progress) {
        if (StringUtils.isNullOrEmpty(model.getDBParameterGroupName()))
            model.setDBParameterGroupName(IdentifierUtils.generateResourceIdentifier(
                    request.getStackId(),
                    Optional.ofNullable(request.getLogicalResourceIdentifier()).orElse(DEFAULT_LOGICAL_IDENTIFIER),
                    request.getClientRequestToken(),
                    MAX_LENGTH_GROUP_NAME
            ).toLowerCase());
        return ProgressEvent.progress(model, progress.getCallbackContext());
    }
}
