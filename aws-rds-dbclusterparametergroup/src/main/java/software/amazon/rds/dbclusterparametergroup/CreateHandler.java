package software.amazon.rds.dbclusterparametergroup;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.util.IdentifierFactory;

public class CreateHandler extends BaseHandlerStd {

    private final static IdentifierFactory groupIdentifierFactory = new IdentifierFactory(
            STACK_NAME,
            RESOURCE_IDENTIFIER,
            MAX_LENGTH_GROUP_NAME
    );

    public CreateHandler() {
        this(DEFAULT_HANDLER_CONFIG);
    }

    public CreateHandler(final DefaultHandlerConfig config) {
        super(config);
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                          final ResourceHandlerRequest<ResourceModel> request,
                                                                          final CallbackContext callbackContext,
                                                                          final ProxyClient<RdsClient> proxyClient,
                                                                          final Logger logger) {
        final Tagging.TagSet allTags = Tagging.TagSet.builder()
                .systemTags(Tagging.translateTagsToSdk(request.getSystemTags()))
                .stackTags(Tagging.translateTagsToSdk(request.getDesiredResourceTags()))
                .resourceTags(Translator.translateTagsToSdk(request.getDesiredResourceState().getTags()))
                .build();

        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                .then(progress -> setDbClusterParameterGroupNameIfMissing(request, progress))
                .then(progress -> Tagging.safeCreate(proxy, proxyClient, this::createDbClusterParameterGroup, progress, allTags))
                .then(progress -> Commons.execOnce(progress, () -> {
                    final Tagging.TagSet extraTags = Tagging.TagSet.builder()
                            .stackTags(allTags.getStackTags())
                            .resourceTags(allTags.getResourceTags())
                            .build();
                    return updateTags(proxy, proxyClient, progress, Tagging.TagSet.emptySet(), extraTags);
                }, CallbackContext::isAddTagsComplete, CallbackContext::setAddTagsComplete))
                .then(progress -> Commons.execOnce(progress, () -> applyParameters(proxy, proxyClient, progress.getResourceModel(), progress.getCallbackContext()),
                        CallbackContext::isParametersApplied, CallbackContext::setParametersApplied))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private ProgressEvent<ResourceModel, CallbackContext> createDbClusterParameterGroup(final AmazonWebServicesClientProxy proxy,
                                                                                        final ProxyClient<RdsClient> proxyClient,
                                                                                        final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                                        final Tagging.TagSet tags) {
        return proxy
                .initiate("rds::create-db-cluster-parameter-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest((resourceModel) -> Translator
                        .createDbClusterParameterGroupRequest(resourceModel, tags))
                .makeServiceCall((paramGroupRequest, proxyInvocation) -> proxyInvocation
                        .injectCredentialsAndInvokeV2(paramGroupRequest, proxyInvocation.client()::createDBClusterParameterGroup))
                .handleError((createDBClusterParameterGroupRequest, exception, client, resourceModel, ctx) ->
                        Commons.handleException(
                                ProgressEvent.progress(resourceModel, ctx),
                                exception,
                                DEFAULT_DB_CLUSTER_PARAMETER_GROUP_ERROR_RULE_SET))
                .done((paramGroupRequest, paramGroupResponse, proxyInvocation, resourceModel, context) -> {
                    context.setDbClusterParameterGroupArn(paramGroupResponse.dbClusterParameterGroup().dbClusterParameterGroupArn());
                    return ProgressEvent.progress(resourceModel, context);
                });
    }

    private ProgressEvent<ResourceModel, CallbackContext> setDbClusterParameterGroupNameIfMissing(final ResourceHandlerRequest<ResourceModel> request,
                                                                                                  final ProgressEvent<ResourceModel, CallbackContext> progress) {
        final ResourceModel model = progress.getResourceModel();
        if (StringUtils.isNullOrEmpty(model.getDBClusterParameterGroupName()))
            model.setDBClusterParameterGroupName(groupIdentifierFactory.newIdentifier()
                    .withStackId(request.getStackId())
                    .withResourceId(request.getLogicalResourceIdentifier())
                    .withRequestToken(request.getClientRequestToken())
                    .toString());
        return ProgressEvent.progress(model, progress.getCallbackContext());
    }
}
