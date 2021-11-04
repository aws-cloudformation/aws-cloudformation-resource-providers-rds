package software.amazon.rds.dbcluster;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBClusterSnapshot;
import software.amazon.awssdk.services.rds.model.DbClusterNotFoundException;
import software.amazon.awssdk.services.rds.model.DbClusterSnapshotNotFoundException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import org.apache.commons.lang3.BooleanUtils;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.cloudformation.resource.IdentifierUtils;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.Logger;

import java.util.Optional;

public class DeleteHandler extends BaseHandlerStd {
    private static final String SNAPSHOT_PREFIX = "Snapshot-";
    private static final int SNAPSHOT_MAX_LENGTH = 255;
    private static final String SNAPSHOT_AVAILABLE = "available";
    private static final String SNAPSHOT_CREATING = "creating";
    private static final String MESSAGE_FORMAT_FAILED_TO_STABILIZE_SNAPSHOT = "DBClusterSnapshot %s failed to stabilize";
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                          final ResourceHandlerRequest<ResourceModel> request,
                                                                          final CallbackContext callbackContext,
                                                                          final ProxyClient<RdsClient> proxyClient,
                                                                          final Logger logger) {

        final String DELETION_PROTECTION_ENABLED_ERROR = "Cannot delete protected %s %s, please disable deletion protection and try again";

        ResourceModel model = request.getDesiredResourceState();
        return ProgressEvent.progress(model, callbackContext)
                .then(progress -> {
                    // If DeletionProtection is enabled, we will fail immediately. We must describe the instance because sometimes deletionProtection is enabled by default and sometimes it is not
                    // We must fail immediately so that a snapshot is not created unnecessarily
                    if( !callbackContext.isDeleting() && isDeletionProtectionEnabled(proxyClient,model)) {
                        throw new CfnGeneralServiceException(String.format(DELETION_PROTECTION_ENABLED_ERROR, "Cluster", model.getDBClusterIdentifier()));
                    }
                    return progress;
                })
                .then(progress -> {

                    String snapshotIdentifier = null;
                    // https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-attribute-deletionpolicy.html
                    // For AWS::RDS::DBCluster resources that don't specify the DBClusterIdentifier property, the default policy is Snapshot.
                    if (request.getSnapshotRequested()!=null && request.getSnapshotRequested()) {
                        snapshotIdentifier = model.getSnapshotIdentifier();
                        if (StringUtils.isNullOrEmpty(snapshotIdentifier)) {
                            snapshotIdentifier = IdentifierUtils.generateResourceIdentifier(
                                    Optional.ofNullable(request.getStackId()).orElse(STACK_NAME),
                                    SNAPSHOT_PREFIX + Optional.ofNullable(request.getLogicalResourceIdentifier()).orElse(RESOURCE_IDENTIFIER),
                                    request.getClientRequestToken(),
                                    SNAPSHOT_MAX_LENGTH
                            );
                        }

                        final String finalSnapshotIdentifier = snapshotIdentifier;
                        return proxy.initiate("rds::remove-from-global-cluster", proxyClient, model, callbackContext)
                                // request to create the snapshot cluster
                                .translateToServiceRequest(clusterModel -> Translator.createDbClusterSnapshotRequest(clusterModel, finalSnapshotIdentifier))
                                .backoffDelay(BACKOFF_STRATEGY)
                                .makeServiceCall((createDbClusterSnapshotRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(createDbClusterSnapshotRequest, proxyInvocation.client()::createDBClusterSnapshot))
                                // wait until creating the snapshot
                                .stabilize((createDbClusterSnapshotRequest, createDbClusterSnapshotResponse, proxyInvocation, resourceModel, context) -> isCreateSnapShotStabilized(proxyInvocation, model, finalSnapshotIdentifier))
                                .progress();
                    }
                    return progress;
                })
                .then(progress ->  {

                    if (!StringUtils.isNullOrEmpty(model.getGlobalClusterIdentifier())){

                    return proxy.initiate("rds::remove-from-global-cluster", proxyClient, model, callbackContext)
                            // request to remove from global cluster
                            .translateToServiceRequest(Translator::removeFromGlobalClusterRequest)
                            .backoffDelay(BACKOFF_STRATEGY)
                            .makeServiceCall((removeFromGlobalClusterRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(removeFromGlobalClusterRequest, proxyInvocation.client()::removeFromGlobalCluster))
                            // wait until deleted
                            .stabilize((removeFromGlobalClusterRequest, removeFromGlobalClusterResponse, proxyInvocation, resourceModel, context) -> isDBClusterStabilized(proxyInvocation, model, DBClusterStatus.Available))
                            .progress();
                    }
                    return progress;
                })
                .then(progress -> {
                    callbackContext.setDeleting(true);
                   return  proxy.initiate("rds::delete-dbcluster", proxyClient, request.getDesiredResourceState(), callbackContext)
                    // request to delete db cluster
                    .translateToServiceRequest(Translator::deleteDbClusterRequest)
                    .backoffDelay(BACKOFF_STRATEGY)
                    .makeServiceCall((deleteDbClusterRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(deleteDbClusterRequest, proxyInvocation.client()::deleteDBCluster))
                    // wait until deleted
                    .stabilize((deleteDbClusterRequest, deleteDbClusterResponse, proxyInvocation, resourceModel, context) -> isDBClusterDeleted(proxyInvocation, model, DBClusterStatus.Deleted))
                    .done((deleteDbClusterRequest, deleteDbClusterResponse, proxyInvocation, resourceModel, context) -> ProgressEvent.defaultSuccessHandler(null));}
        );


    }

    protected boolean isCreateSnapShotStabilized(final ProxyClient<RdsClient> proxyClient,
                                              final ResourceModel model,
                                              final String finalDBSnapshotIdentifier
                                              ) {
        try {
            final Optional<DBClusterSnapshot> dbClusterSnapshot =
                    proxyClient.injectCredentialsAndInvokeV2(
                            Translator.describeDbClusterSnapshotsRequest(model,finalDBSnapshotIdentifier ),
                            proxyClient.client()::describeDBClusterSnapshots).dbClusterSnapshots().stream().findFirst();
            if(!dbClusterSnapshot.isPresent()) {
                throw new CfnNotStabilizedException(MESSAGE_FORMAT_FAILED_TO_STABILIZE_SNAPSHOT, finalDBSnapshotIdentifier);
            }

            switch (dbClusterSnapshot.get().status()){
                case  SNAPSHOT_AVAILABLE:
                    return true;
                case SNAPSHOT_CREATING:
                    return false;
                default:
                    throw new CfnNotStabilizedException(MESSAGE_FORMAT_FAILED_TO_STABILIZE_SNAPSHOT, finalDBSnapshotIdentifier);
            }

        } catch (DbClusterSnapshotNotFoundException e) {
            throw new CfnNotFoundException(ResourceModel.TYPE_NAME, e.getMessage());
        }
    }
    protected boolean isDeletionProtectionEnabled(final ProxyClient<RdsClient> proxyClient,
                                         final ResourceModel model) {
        // describe  db cluster to fetch if deletion protection is enabled
        try {
            final Optional<DBCluster> dbCluster =
                    proxyClient.injectCredentialsAndInvokeV2(
                            Translator.describeDbClustersRequest(model),
                            proxyClient.client()::describeDBClusters).dbClusters().stream().findFirst();
            if(!dbCluster.isPresent()){
                throw new CfnNotFoundException(ResourceModel.TYPE_NAME, model.getDBClusterIdentifier());
            }
            return dbCluster.get().deletionProtection();
        } catch (DbClusterNotFoundException e) {
            throw new CfnNotFoundException(ResourceModel.TYPE_NAME, e.getMessage());
        }
    }
}
