package software.amazon.rds.dbclusterparametergroup;

import java.util.ArrayList;
import java.util.Map;

import com.amazonaws.util.StringUtils;
import com.google.common.collect.Maps;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.Parameter;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
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

    public CreateHandler(final HandlerConfig config) {
        super(config);
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext
    ) {
        final Tagging.TagSet allTags = Tagging.TagSet.builder()
                .systemTags(Tagging.translateTagsToSdk(request.getSystemTags()))
                .stackTags(Tagging.translateTagsToSdk(request.getDesiredResourceTags()))
                .resourceTags(Translator.translateTagsToSdk(request.getDesiredResourceState().getTags()))
                .build();

        final Map<String, Object> desiredParams = request.getDesiredResourceState().getParameters();
        final Map<String, Parameter> currentClusterParameters = Maps.newHashMap();

        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                .then(progress -> setDbClusterParameterGroupNameIfMissing(request, progress))
                .then(progress -> Tagging.createWithTaggingFallback(proxy, proxyClient, this::createDbClusterParameterGroup, progress, allTags))
                .then(progress -> Commons.execOnce(progress, () -> {
                    final Tagging.TagSet extraTags = Tagging.TagSet.builder()
                            .stackTags(allTags.getStackTags())
                            .resourceTags(allTags.getResourceTags())
                            .build();
                    return updateTags(proxy, proxyClient, progress, Tagging.TagSet.emptySet(), extraTags);
                }, CallbackContext::isAddTagsComplete, CallbackContext::setAddTagsComplete))
                .then(progress -> Commons.execOnce(progress, () ->
                                describeCurrentDBClusterParameters(proxy, proxyClient, progress, new ArrayList<>(desiredParams.keySet()), currentClusterParameters)
                                        .then(p -> applyParameters(proxy, proxyClient, progress, currentClusterParameters, requestLogger)
                        ),
                        CallbackContext::isParametersApplied, CallbackContext::setParametersApplied))
                .then(progress -> new ReadHandler().handleRequest(proxy, proxyClient, request, callbackContext));
    }

    private ProgressEvent<ResourceModel, CallbackContext> createDbClusterParameterGroup(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet tags
    ) {
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
                                DEFAULT_DB_CLUSTER_PARAMETER_GROUP_ERROR_RULE_SET,
                                requestLogger))
                .done((paramGroupRequest, paramGroupResponse, proxyInvocation, resourceModel, context) -> {
                    context.setDbClusterParameterGroupArn(paramGroupResponse.dbClusterParameterGroup().dbClusterParameterGroupArn());
                    return ProgressEvent.progress(resourceModel, context);
                });
    }

    private ProgressEvent<ResourceModel, CallbackContext> setDbClusterParameterGroupNameIfMissing(
            final ResourceHandlerRequest<ResourceModel> request,
            final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
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
