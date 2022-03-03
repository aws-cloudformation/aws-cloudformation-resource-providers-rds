package software.amazon.rds.dbcluster;

import java.util.Optional;

import org.apache.commons.lang3.BooleanUtils;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.resource.IdentifierUtils;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;

public class DeleteHandler extends BaseHandlerStd {

    private static final String SNAPSHOT_PREFIX = "Snapshot-";
    private static final int SNAPSHOT_MAX_LENGTH = 255;
    private static final String DELETION_PROTECTION_ENABLED_ERROR = "Cannot delete protected Cluster %s, please disable deletion protection and try again";

    public DeleteHandler() {
        this(HandlerConfig.builder().build());
    }

    public DeleteHandler(final HandlerConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger
    ) {
        final ResourceModel model = request.getDesiredResourceState();

        if (!callbackContext.isDeleting()) {
            boolean deletionProtectionEnabled;
            try {
                deletionProtectionEnabled = isDeletionProtectionEnabled(proxyClient, model);
            } catch (Exception exception) {
                return Commons.handleException(
                        ProgressEvent.progress(model, callbackContext),
                        exception,
                        DEFAULT_DB_CLUSTER_ERROR_RULE_SET
                );
            }
            if (deletionProtectionEnabled) {
                return ProgressEvent.failed(
                        model,
                        callbackContext,
                        HandlerErrorCode.NotUpdatable,
                        String.format(DELETION_PROTECTION_ENABLED_ERROR, model.getDBClusterIdentifier())
                );
            }
        }

        return ProgressEvent.progress(model, callbackContext)
                .then(progress -> {
                    if (isGlobalClusterMember(model)) {
                        return removeFromGlobalCluster(proxy, proxyClient, progress, model.getGlobalClusterIdentifier());
                    }
                    return progress;
                })
                .then(progress -> deleteDbCluster(proxy, request, proxyClient, progress));
    }

    private ProgressEvent<ResourceModel, CallbackContext> deleteDbCluster(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        final ResourceModel resourceModel = request.getDesiredResourceState();

        String snapshotIdentifier = null;
        if (BooleanUtils.isTrue(request.getSnapshotRequested())) {
            snapshotIdentifier = resourceModel.getSnapshotIdentifier();
            if (StringUtils.isNullOrEmpty(snapshotIdentifier)) {
                snapshotIdentifier = IdentifierUtils.generateResourceIdentifier(
                        Optional.ofNullable(request.getStackId()).orElse(STACK_NAME),
                        SNAPSHOT_PREFIX + Optional.ofNullable(request.getLogicalResourceIdentifier()).orElse(RESOURCE_IDENTIFIER),
                        request.getClientRequestToken(),
                        SNAPSHOT_MAX_LENGTH
                );
            }
        }
        final String finalSnapshotIdentifier = snapshotIdentifier;

        progress.getCallbackContext().setDeleting(true);

        return proxy.initiate("rds::delete-dbcluster", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(model -> Translator.deleteDbClusterRequest(model, finalSnapshotIdentifier))
                .backoffDelay(config.getBackoff())
                .makeServiceCall((deleteRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        deleteRequest,
                        proxyInvocation.client()::deleteDBCluster
                ))
                .stabilize((deleteRequest, deleteResponse, proxyInvocation, model, context) -> isDBClusterDeleted(proxyInvocation, model))
                .handleError((deleteRequest, exception, client, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        DEFAULT_DB_CLUSTER_ERROR_RULE_SET
                ))
                .done((deleteRequest, deleteResponse, proxyInvocation, model, context) -> ProgressEvent.defaultSuccessHandler(null));
    }

    protected boolean isDeletionProtectionEnabled(
            final ProxyClient<RdsClient> proxyClient,
            final ResourceModel model
    ) {
        final DBCluster dbCluster = fetchDBCluster(proxyClient, model);
        return dbCluster.deletionProtection();
    }
}
