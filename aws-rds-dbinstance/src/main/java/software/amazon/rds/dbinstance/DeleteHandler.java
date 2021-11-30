package software.amazon.rds.dbinstance;

import java.util.Optional;

import org.apache.commons.lang3.BooleanUtils;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DeleteDbInstanceResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;

public class DeleteHandler extends BaseHandlerStd {

    private static final String SNAPSHOT_PREFIX = "Snapshot-";
    private static final int SNAPSHOT_MAX_LENGTH = 255;
    private static final String DB_INSTANCE_IS_BEING_DELETED_ERR = "is already being deleted";

    public DeleteHandler() {
        this(HandlerConfig.builder().probingEnabled(true).build());
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
            final Logger logger
    ) {
        final ResourceModel resourceModel = request.getDesiredResourceState();
        String snapshotIdentifier = null;
        // https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-attribute-deletionpolicy.html
        // For AWS::RDS::DBInstance resources that don't specify the DBClusterIdentifier property, the default policy is Snapshot.
        if (BooleanUtils.isNotFalse(request.getSnapshotRequested())) {
            snapshotIdentifier = resourceModel.getDBSnapshotIdentifier();
            if (StringUtils.isNullOrEmpty(snapshotIdentifier)) {
                snapshotIdentifier = generateResourceIdentifier(
                        Optional.ofNullable(request.getStackId()).orElse(STACK_NAME),
                        SNAPSHOT_PREFIX + Optional.ofNullable(request.getLogicalResourceIdentifier()).orElse(RESOURCE_IDENTIFIER),
                        request.getClientRequestToken(),
                        SNAPSHOT_MAX_LENGTH
                );
            }
        }
        final String finalSnapshotIdentifier = snapshotIdentifier;

        return proxy.initiate("rds::delete-db-instance", rdsProxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(model -> Translator.deleteDbInstanceRequest(model, finalSnapshotIdentifier))
                .backoffDelay(config.getBackoff())
                .makeServiceCall((deleteRequest, proxyInvocation) -> {
                    if (callbackContext.isDeleted()) {
                        return callbackContext.response("rds::delete-db-instance");
                    }
                    DeleteDbInstanceResponse response = null;
                    try {
                        response = proxyInvocation.injectCredentialsAndInvokeV2(
                                deleteRequest,
                                proxyInvocation.client()::deleteDBInstance
                        );
                    } catch (Exception exception) {
                        if (!isDbInstanceDeletingException(exception)) {
                            throw exception;
                        }
                    }
                    callbackContext.setDeleted(true);
                    return response;
                })
                .stabilize((deleteRequest, deleteResponse, proxyInvocation, model, context) -> isDbInstanceDeleted(proxyInvocation, model))
                .handleError((deleteRequest, exception, client, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        DELETE_DB_INSTANCE_ERROR_RULE_SET
                ))
                .done((deleteRequest, deleteResponse, proxyInvocation, model, context) -> ProgressEvent.defaultSuccessHandler(null));
    }

    private boolean isDbInstanceDeletingException(final Exception exception) {
        if (exception instanceof AwsServiceException) {
            return Optional.ofNullable(exception.getMessage()).orElse("").contains(DB_INSTANCE_IS_BEING_DELETED_ERR);
        }
        return false;
    }
}
