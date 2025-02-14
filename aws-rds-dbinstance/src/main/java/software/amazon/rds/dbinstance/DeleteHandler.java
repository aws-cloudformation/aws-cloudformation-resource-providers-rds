package software.amazon.rds.dbinstance;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.request.ValidatedRequest;
import software.amazon.rds.common.util.IdentifierFactory;
import software.amazon.rds.dbinstance.client.VersionedProxyClient;
import software.amazon.rds.dbinstance.common.ErrorRuleSets;

import java.util.function.Function;

public class DeleteHandler extends BaseHandlerStd {

    private static final String SNAPSHOT_PREFIX = "Snapshot-";
    private static final int SNAPSHOT_MAX_LENGTH = 255;

    private static final IdentifierFactory snapshotIdentifierFactory = new IdentifierFactory(
            STACK_NAME,
            SNAPSHOT_PREFIX + RESOURCE_IDENTIFIER,
            SNAPSHOT_MAX_LENGTH
    );

    public DeleteHandler() {
        this(DB_INSTANCE_HANDLER_CONFIG_36H);
    }

    public DeleteHandler(final HandlerConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ValidatedRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final VersionedProxyClient<RdsClient> rdsProxyClient,
            final VersionedProxyClient<Ec2Client> ec2ProxyClient
    ) {
        // !!!: For delete handlers, the only input guaranteed to exist in the model is the DB instance identifier.
        // This is why we fetch the DB instance instead of poking into the model.
        // https://docs.aws.amazon.com/cloudformation-cli/latest/userguide/resource-type-test-contract.html#resource-type-test-contract-delete
        final ResourceModel resourceModel = request.getDesiredResourceState();

        return ProgressEvent.progress(resourceModel, callbackContext)
                .then(progress -> Commons.execOnce(progress, () -> {
                        try {
                            final var dbInstance = fetchDBInstance(rdsProxyClient.defaultClient(), progress.getResourceModel());
                            callbackContext.setSnapshotIdentifier(decideSnapshotIdentifier(request, dbInstance));
                        } catch (Exception exception) {
                            return Commons.handleException(progress, exception, software.amazon.rds.dbinstance.common.ErrorRuleSets.DEFAULT_DB_INSTANCE, requestLogger);
                        }
                        return progress;
                }, CallbackContext::isDescribed, CallbackContext::setDescribed))
                .then(progress -> proxy.initiate("rds::delete-db-instance", rdsProxyClient.defaultClient(), resourceModel, callbackContext)
                        .translateToServiceRequest(model -> Translator.deleteDbInstanceRequest(model, callbackContext.getSnapshotIdentifier()))
                        .backoffDelay(config.getBackoff())
                        .makeServiceCall((deleteRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                                deleteRequest,
                                proxyInvocation.client()::deleteDBInstance
                        ))
                        .handleError((deleteRequest, exception, client, model, context) -> Commons.handleException(
                                ProgressEvent.progress(model, context),
                                exception,
                                ErrorRuleSets.DELETE_DB_INSTANCE,
                                requestLogger
                        )).progress()
                )
                // We split the delete and stabilize into two call chains because any error interrupts the entire chain.
                // The delete operation can return certain errors that we wish to ignore. To ensure that stabilization
                // happens when these errors occur, stabilization needs to be in a separate chain.
                .then(progress -> proxy.initiate("rds::delete-db-instance-stabilize", rdsProxyClient.defaultClient(), progress.getResourceModel(), progress.getCallbackContext())
                        .translateToServiceRequest(Function.identity())
                        .backoffDelay(config.getBackoff())
                        .makeServiceCall(NOOP_CALL)
                        .stabilize((noopRequest, noopResponse, proxyInvocation, model, context) -> isDbInstanceDeleted(proxyInvocation, model))
                        .handleError((noopRequest, exception, client, model, context) -> Commons.handleException(
                                ProgressEvent.progress(model, context),
                                exception,
                                software.amazon.rds.dbinstance.common.ErrorRuleSets.DEFAULT_DB_INSTANCE,
                                requestLogger
                        ))
                        .progress()
                )
                .then(progress -> ProgressEvent.defaultSuccessHandler(null));
    }

    @VisibleForTesting
    static String decideSnapshotIdentifier(
            final ResourceHandlerRequest<ResourceModel> request,
            final DBInstance dbInstance
    ) {
        // CFN sends true or false for snapshotRequested based on the template's DeletionPolicy.
        // However, CCAPI leaves it null. In that case, we take a snapshot, since that is CFN's default.
        // Exception is that final snapshots are not allowed for read replicas and cluster instances.
        boolean shouldAttemptToTakeSnapshot = BooleanUtils.isNotFalse(request.getSnapshotRequested());
        boolean isReadReplica = StringUtils.isNotEmpty(dbInstance.readReplicaSourceDBInstanceIdentifier()) ||
                StringUtils.isNotEmpty(dbInstance.readReplicaSourceDBClusterIdentifier());
        boolean isSnapshotPossible = !isReadReplica && StringUtils.isEmpty(dbInstance.dbClusterIdentifier());

        if (shouldAttemptToTakeSnapshot && isSnapshotPossible) {
            return snapshotIdentifierFactory.newIdentifier()
                    .withStackId(request.getStackId())
                    .withResourceId(StringUtils.prependIfMissing(request.getLogicalResourceIdentifier(), SNAPSHOT_PREFIX))
                    .withRequestToken(request.getClientRequestToken())
                    .toString();
        } else {
            return null;
        }
    }
}
