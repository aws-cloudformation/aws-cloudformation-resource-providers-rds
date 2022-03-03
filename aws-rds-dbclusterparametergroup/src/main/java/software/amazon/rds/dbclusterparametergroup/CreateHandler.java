package software.amazon.rds.dbclusterparametergroup;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.resource.IdentifierUtils;
import software.amazon.rds.common.handler.Commons;


public class CreateHandler extends BaseHandlerStd {
    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                          final ResourceHandlerRequest<ResourceModel> request,
                                                                          final CallbackContext callbackContext,
                                                                          final ProxyClient<RdsClient> proxyClient,
                                                                          final Logger logger) {

        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                .then(progress -> setDbClusterParameterGroupNameIfMissing(request, progress))
                .then(progress -> createDbClusterParameterGroup(proxy, request, proxyClient, progress))
                .then(progress -> applyParameters(proxy, proxyClient, progress.getResourceModel(), progress.getCallbackContext()))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private ProgressEvent<ResourceModel, CallbackContext> createDbClusterParameterGroup(final AmazonWebServicesClientProxy proxy,
                                                                                        final ResourceHandlerRequest<ResourceModel> request,
                                                                                        final ProxyClient<RdsClient> proxyClient,
                                                                                        final ProgressEvent<ResourceModel, CallbackContext> progress) {
        return proxy
                .initiate("rds::create-db-cluster-parameter-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest((resourceModel) -> Translator
                        .createDbClusterParameterGroupRequest(resourceModel, request.getDesiredResourceTags()))
                .makeServiceCall((paramGroupRequest, proxyInvocation) -> proxyInvocation
                        .injectCredentialsAndInvokeV2(paramGroupRequest, proxyInvocation.client()::createDBClusterParameterGroup))
                .handleError((createDBClusterParameterGroupRequest, exception, client, resourceModel, ctx) ->
                        Commons.handleException(
                                ProgressEvent.progress(resourceModel, ctx),
                                exception,
                                DEFAULT_DB_CLUSTER_PARAMETER_GROUP_ERROR_RULE_SET))
                .progress();
    }

    private ProgressEvent<ResourceModel, CallbackContext> setDbClusterParameterGroupNameIfMissing(final ResourceHandlerRequest<ResourceModel> request,
                                                                                                  final ProgressEvent<ResourceModel, CallbackContext> progress) {
        final ResourceModel model = progress.getResourceModel();
        if (StringUtils.isNullOrEmpty(model.getDBClusterParameterGroupName()))
            model.setDBClusterParameterGroupName(IdentifierUtils
                    .generateResourceIdentifier(request.getStackId(),
                            request.getLogicalResourceIdentifier(),
                            request.getClientRequestToken(),
                            MAX_LENGTH_GROUP_NAME)
                    .toLowerCase());
        return ProgressEvent.progress(model, progress.getCallbackContext());
    }
}
