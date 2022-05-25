package software.amazon.rds.dbinstance;

import java.util.function.Function;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.util.IdentifierFactory;
import software.amazon.rds.dbinstance.client.VersionedProxyClient;

public class DeleteHandler extends BaseHandlerStd {

    private static final String SNAPSHOT_PREFIX = "Snapshot-";
    private static final int SNAPSHOT_MAX_LENGTH = 255;

    private static final IdentifierFactory snapshotIdentifierFactory = new IdentifierFactory(
            STACK_NAME,
            SNAPSHOT_PREFIX + RESOURCE_IDENTIFIER,
            SNAPSHOT_MAX_LENGTH
    );

    public DeleteHandler() {
        this(DEFAULT_DB_INSTANCE_HANDLER_CONFIG);
    }

    public DeleteHandler(final HandlerConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final VersionedProxyClient<RdsClient> rdsProxyClient,
            final VersionedProxyClient<Ec2Client> ec2ProxyClient,
            final Logger logger
    ) {
        final ResourceModel resourceModel = request.getDesiredResourceState();
        String snapshotIdentifier = null;
        // https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-attribute-deletionpolicy.html
        // For AWS::RDS::DBInstance resources that don't specify the DBClusterIdentifier property, the default policy is Snapshot.
        // Final snapshots are not allowed for read replicas and cluster instances.
        if (BooleanUtils.isTrue(request.getSnapshotRequested()) &&
                !isReadReplica(resourceModel) &&
                !isDBClusterMember(resourceModel)) {
            snapshotIdentifier = snapshotIdentifierFactory.newIdentifier()
                    .withStackId(request.getStackId())
                    .withResourceId(StringUtils.prependIfMissing(request.getLogicalResourceIdentifier(), SNAPSHOT_PREFIX))
                    .withRequestToken(request.getClientRequestToken())
                    .toString();
        }
        final String finalSnapshotIdentifier = snapshotIdentifier;

        return ProgressEvent.progress(resourceModel, callbackContext)
                .then(progress -> proxy.initiate("rds::delete-db-instance", rdsProxyClient.defaultClient(), resourceModel, callbackContext)
                        .translateToServiceRequest(model -> Translator.deleteDbInstanceRequest(model, finalSnapshotIdentifier))
                        .backoffDelay(config.getBackoff())
                        .makeServiceCall((deleteRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                                deleteRequest,
                                proxyInvocation.client()::deleteDBInstance
                        ))
                        .handleError((deleteRequest, exception, client, model, context) -> Commons.handleException(
                                ProgressEvent.progress(model, context),
                                exception,
                                DELETE_DB_INSTANCE_ERROR_RULE_SET
                        )).progress()
                )
                // The reason we split a pretty trivial execution chain in 2 is because of the error handling.
                // Delete handler should ignore some exceptions and go straight to the stabilization step.
                // The execution chain interrupts immediately once handleError is called. This eliminates
                // the stabilization step. For the sake of enforcing the stabilization, we spin up a separate
                // execution chain with a no-op service call. Note that it is only supposed to handle exceptions
                // thrown by isDbInstanceDeleted, hence the default ruleset is put in place instead.
                .then(progress -> proxy.initiate("rds::delete-db-instance-stabilize", rdsProxyClient.defaultClient(), progress.getResourceModel(), progress.getCallbackContext())
                        .translateToServiceRequest(Function.identity())
                        .backoffDelay(config.getBackoff())
                        .makeServiceCall(NOOP_CALL)
                        .stabilize((noopRequest, noopResponse, proxyInvocation, model, context) -> isDbInstanceDeleted(proxyInvocation, model))
                        .handleError((noopRequest, exception, client, model, context) -> Commons.handleException(
                                ProgressEvent.progress(model, context),
                                exception,
                                DEFAULT_DB_INSTANCE_ERROR_RULE_SET
                        ))
                        .progress()
                )
                .then(progress -> ProgressEvent.defaultSuccessHandler(null));
    }
}
