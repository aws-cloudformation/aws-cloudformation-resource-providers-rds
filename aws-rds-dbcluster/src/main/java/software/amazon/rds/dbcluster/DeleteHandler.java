package software.amazon.rds.dbcluster;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.util.IdentifierFactory;

public class DeleteHandler extends BaseHandlerStd {

    private static final String SNAPSHOT_PREFIX = "Snapshot-";
    private static final int SNAPSHOT_MAX_LENGTH = 255;
    private static final String DELETION_PROTECTION_ENABLED_ERROR = "Cannot delete protected Cluster %s, please disable deletion protection and try again";

    private static final IdentifierFactory snapshotIdentifierFactory = new IdentifierFactory(
            STACK_NAME,
            SNAPSHOT_PREFIX + RESOURCE_IDENTIFIER,
            SNAPSHOT_MAX_LENGTH
    );

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
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProxyClient<Ec2Client> ec2ProxyClient,
            final Logger logger) {
        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                .then(progress -> Commons.execOnce(progress, () ->
                                ensureDeletionProtectionDisabled(rdsProxyClient, progress),
                        CallbackContext::isDeleting,
                        CallbackContext::setDeleting)
                )
                .then(progress -> {
                    final ResourceModel model = progress.getResourceModel();
                    if (isGlobalClusterMember(model)) {
                        return removeFromGlobalCluster(proxy, rdsProxyClient, progress, model.getGlobalClusterIdentifier());
                    }
                    return progress;
                })
                .then(progress -> deleteDbCluster(proxy, request, rdsProxyClient, progress));
    }

    private ProgressEvent<ResourceModel, CallbackContext> ensureDeletionProtectionDisabled(
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        try {
            final ResourceModel model = progress.getResourceModel();
            final CallbackContext context = progress.getCallbackContext();
            if (isDeletionProtectionEnabled(proxyClient, model)) {
                final String errorMessage = String.format(DELETION_PROTECTION_ENABLED_ERROR, model.getDBClusterIdentifier());
                return ProgressEvent.failed(model, context, HandlerErrorCode.NotUpdatable, errorMessage);
            }
        } catch (Exception exception) {
            return Commons.handleException(progress, exception, DEFAULT_DB_CLUSTER_ERROR_RULE_SET);
        }
        return progress;
    }

    private ProgressEvent<ResourceModel, CallbackContext> deleteDbCluster(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        String snapshotIdentifier = null;

        if (BooleanUtils.isTrue(request.getSnapshotRequested())) {
            snapshotIdentifier = snapshotIdentifierFactory.newIdentifier()
                    .withStackId(request.getStackId())
                    .withResourceId(StringUtils.prependIfMissing(request.getLogicalResourceIdentifier(), SNAPSHOT_PREFIX))
                    .withRequestToken(request.getClientRequestToken())
                    .toString();
        }
        final String finalSnapshotIdentifier = snapshotIdentifier;

        return proxy.initiate("rds::delete-db-cluster", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
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

    private boolean isDeletionProtectionEnabled(
            final ProxyClient<RdsClient> proxyClient,
            final ResourceModel model
    ) {
        final DBCluster dbCluster = fetchDBCluster(proxyClient, model);
        return BooleanUtils.isTrue(dbCluster.deletionProtection());
    }
}
