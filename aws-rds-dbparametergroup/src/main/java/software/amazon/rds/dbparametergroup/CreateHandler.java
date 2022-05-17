package software.amazon.rds.dbparametergroup;

import java.util.Collections;
import java.util.HashSet;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.common.util.IdentifierFactory;


public class CreateHandler extends BaseHandlerStd {

    private static final IdentifierFactory groupIdentifierFactory = new IdentifierFactory(
            STACK_NAME,
            RESOURCE_IDENTIFIER,
            MAX_LENGTH_GROUP_NAME
    );

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

        final Tagging.TagSet allTags = Tagging.TagSet.builder()
                .systemTags(Tagging.translateTagsToSdk(request.getSystemTags()))
                .stackTags(Tagging.translateTagsToSdk(request.getDesiredResourceTags()))
                .resourceTags(new HashSet<>(Translator.translateTagsToSdk(request.getDesiredResourceState().getTags())))
                .build();

        return ProgressEvent.progress(desiredModel, callbackContext)
                .then(progress -> setDBParameterGroupNameIfEmpty(request, desiredModel, progress))
                .then(progress -> safeCreateDBParameterGroup(proxy, proxyClient, progress, allTags, requestLogger))
                .then(progress -> applyParameters(proxy, proxyClient, progress, requestLogger))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, requestLogger));
    }

    private ProgressEvent<ResourceModel, CallbackContext> safeCreateDBParameterGroup(final AmazonWebServicesClientProxy proxy,
                                                                                     final ProxyClient<RdsClient> proxyClient,
                                                                                     final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                                     final Tagging.TagSet allTags,
                                                                                     final RequestLogger requestLogger) {
        ProgressEvent<ResourceModel, CallbackContext> progressEvent = createDBParameterGroup(proxy, proxyClient, progress, allTags, requestLogger);
        if (HandlerErrorCode.AccessDenied.equals(progressEvent.getErrorCode())) { //Resource is subject to soft fail on stack level tags.
            Tagging.TagSet systemTags = Tagging.TagSet.builder().systemTags(allTags.getSystemTags()).build();
            Tagging.TagSet extraTags = allTags.toBuilder().systemTags(Collections.emptySet()).build();
            return createDBParameterGroup(proxy, proxyClient, progress, systemTags, requestLogger)
                    .then(prog -> updateTags(proxy, proxyClient, prog, Tagging.TagSet.emptySet(), extraTags, requestLogger));
        }
        return progressEvent;
    }

    private ProgressEvent<ResourceModel, CallbackContext> createDBParameterGroup(final AmazonWebServicesClientProxy proxy,
                                                                                 final ProxyClient<RdsClient> proxyClient,
                                                                                 final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                                 final Tagging.TagSet tags,
                                                                                 final RequestLogger requestLogger) {
        return proxy.initiate("rds::create-db-parameter-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(resourceModel -> Translator.createDbParameterGroupRequest(resourceModel, tags))
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
            model.setDBParameterGroupName(groupIdentifierFactory.newIdentifier()
                    .withStackId(request.getStackId())
                    .withResourceId(request.getLogicalResourceIdentifier())
                    .withRequestToken(request.getClientRequestToken())
                    .toString());
        return ProgressEvent.progress(model, progress.getCallbackContext());
    }
}
