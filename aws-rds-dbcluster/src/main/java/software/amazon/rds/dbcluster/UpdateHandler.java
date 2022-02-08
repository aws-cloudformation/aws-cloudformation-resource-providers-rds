package software.amazon.rds.dbcluster;

import static software.amazon.rds.dbcluster.ModelAdapter.setDefaults;
import static software.amazon.rds.dbcluster.Translator.cloudwatchLogsExportConfiguration;

import java.util.Collection;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CloudwatchLogsExportConfiguration;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.dbcluster.util.ImmutabilityHelper;

public class UpdateHandler extends BaseHandlerStd {

    public UpdateHandler() {
        this(HandlerConfig.builder().build());
    }

    public UpdateHandler(final HandlerConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger
    ) {
        if (!ImmutabilityHelper.isChangeMutable(request.getPreviousResourceState(), request.getDesiredResourceState())) {
            return ProgressEvent.failed(
                    request.getDesiredResourceState(),
                    callbackContext,
                    HandlerErrorCode.NotUpdatable,
                    "Resource is immutable"
            );
        }

        final Collection<Tag> previousTags = Translator.translateTagsFromRequest(
                Tagging.mergeTags(
                        request.getPreviousSystemTags(),
                        request.getPreviousResourceTags()
                )
        );
        final Collection<Tag> desiredTags = Translator.translateTagsFromRequest(
                Tagging.mergeTags(
                        request.getSystemTags(),
                        request.getDesiredResourceTags()
                )
        );

        return ProgressEvent.progress(setDefaults(request.getDesiredResourceState()), callbackContext)
                .then(progress -> {
                    if (shouldRemoveFromGlobalCluster(request.getPreviousResourceState(), request.getDesiredResourceState())) {
                        return removeFromGlobalCluster(proxy, proxyClient, progress, request.getPreviousResourceState().getGlobalClusterIdentifier());
                    }
                    return progress;
                })
                .then(progress -> Commons.execOnce(
                        progress,
                        () -> modifyDBCluster(proxy, proxyClient, progress, cloudwatchLogsExportConfiguration(request)),
                        CallbackContext::isModified,
                        CallbackContext::setModified)
                )
                .then(progress -> removeAssociatedRoles(proxy, proxyClient, progress, setDefaults(request.getPreviousResourceState()).getAssociatedRoles()))
                .then(progress -> addAssociatedRoles(proxy, proxyClient, progress, progress.getResourceModel().getAssociatedRoles()))
                .then(progress -> tagResource(proxy, proxyClient, progress, previousTags, desiredTags))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    protected ProgressEvent<ResourceModel, CallbackContext> modifyDBCluster(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final CloudwatchLogsExportConfiguration cloudwatchLogsExportConfiguration
    ) {
        return proxy.initiate("rds::modify-dbcluster", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(model -> Translator.modifyDbClusterRequest(model, cloudwatchLogsExportConfiguration))
                .backoffDelay(config.getBackoff())
                .makeServiceCall((dbClusterModifyRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        dbClusterModifyRequest,
                        proxyInvocation.client()::modifyDBCluster
                ))
                .stabilize((modifyRequest, modifyResponse, proxyInvocation, model, context) -> {
                    return isDBClusterStabilized(proxyInvocation, model, DBClusterStatus.Available);
                })
                .handleError((createRequest, exception, client, resourceModel, callbackCtxt) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, callbackCtxt),
                        exception,
                        DEFAULT_DB_CLUSTER_ERROR_RULE_SET
                ))
                .progress();
    }

    private boolean shouldRemoveFromGlobalCluster(
            final ResourceModel previousResourceState,
            final ResourceModel desiredResourceState
    ) {
        return StringUtils.hasValue(previousResourceState.getGlobalClusterIdentifier()) &&
                StringUtils.isNullOrEmpty(desiredResourceState.getGlobalClusterIdentifier());
    }
}
