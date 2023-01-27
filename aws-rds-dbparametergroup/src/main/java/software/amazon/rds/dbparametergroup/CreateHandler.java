package software.amazon.rds.dbparametergroup;

import java.util.HashSet;
import java.util.Map;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.HandlerMethod;
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
            final ProxyClient<RdsClient> proxyClient,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final RequestLogger logger
    ) {
        final ResourceModel desiredModel = request.getDesiredResourceState();

        final Tagging.TagSet allTags = Tagging.TagSet.builder()
                .systemTags(Tagging.translateTagsToSdk(request.getSystemTags()))
                .stackTags(Tagging.translateTagsToSdk(request.getDesiredResourceTags()))
                .resourceTags(new HashSet<>(Translator.translateTagsToSdk(request.getDesiredResourceState().getTags())))
                .build();

        final Map<String, Object> desiredParams = request.getDesiredResourceState().getParameters();

        return ProgressEvent.progress(desiredModel, callbackContext)
                .then(progress -> setDBParameterGroupNameIfEmpty(request, progress))
                .then(progress -> safeCreateDBParameterGroup(proxy, proxyClient, progress, allTags, logger))
                .then(progress -> applyParameters(proxy, proxyClient, progress, desiredParams, logger))
                .then(progress -> new ReadHandler().handleRequest(proxy, proxyClient, request, callbackContext, logger));
    }

    private ProgressEvent<ResourceModel, CallbackContext> safeCreateDBParameterGroup(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet allTags,
            final RequestLogger logger
    ) {
        final HandlerMethod<ResourceModel, CallbackContext> createMethod = (pxy, pcl, prg, tgs) -> createDBParameterGroup(pxy, pcl, prg, tgs, logger);
        return Tagging.safeCreate(proxy, proxyClient, createMethod, progress, allTags)
                .then(p -> Commons.execOnce(p, () -> {
                    final Tagging.TagSet extraTags = Tagging.TagSet.builder()
                            .stackTags(allTags.getStackTags())
                            .resourceTags(allTags.getResourceTags())
                            .build();
                    return updateTags(proxy, proxyClient, p, Tagging.TagSet.emptySet(), extraTags, logger);
                }, CallbackContext::isAddTagsComplete, CallbackContext::setAddTagsComplete));
    }

    private ProgressEvent<ResourceModel, CallbackContext> createDBParameterGroup(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet tags,
            final RequestLogger logger
    ) {
        return proxy.initiate("rds::create-db-parameter-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(resourceModel -> Translator.createDbParameterGroupRequest(resourceModel, tags))
                .backoffDelay(config.getBackoff())
                .makeServiceCall((request, proxyInvocation) ->
                        proxyInvocation.injectCredentialsAndInvokeV2(request, proxyInvocation.client()::createDBParameterGroup))
                .handleError((request, exception, client, resourceModel, ctx) ->
                        Commons.handleException(
                                ProgressEvent.progress(resourceModel, ctx),
                                exception,
                                DEFAULT_DB_PARAMETER_GROUP_ERROR_RULE_SET))
                .done((request, response, proxyInvocation, resourceModel, context) -> {
                    context.setDbParameterGroupArn(response.dbParameterGroup().dbParameterGroupArn());
                    return ProgressEvent.progress(resourceModel, context);
                });
    }

    private ProgressEvent<ResourceModel, CallbackContext> setDBParameterGroupNameIfEmpty(
            final ResourceHandlerRequest<ResourceModel> request,
            final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        if (StringUtils.isNullOrEmpty(progress.getResourceModel().getDBParameterGroupName())) {
            progress.getResourceModel().setDBParameterGroupName(groupIdentifierFactory.newIdentifier()
                    .withStackId(request.getStackId())
                    .withResourceId(request.getLogicalResourceIdentifier())
                    .withRequestToken(request.getClientRequestToken())
                    .toString());
        }
        return progress;
    }
}
