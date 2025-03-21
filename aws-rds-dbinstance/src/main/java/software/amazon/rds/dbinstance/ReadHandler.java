package software.amazon.rds.dbinstance;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DBInstanceAutomatedBackup;
import software.amazon.awssdk.services.rds.model.DBInstanceAutomatedBackupsReplication;
import software.amazon.awssdk.services.rds.model.RdsException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.common.error.ErrorStatus;
import software.amazon.rds.common.error.HandlerErrorStatus;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.request.ValidatedRequest;
import software.amazon.rds.dbinstance.util.ResourceModelHelper;
import software.amazon.rds.dbinstance.client.RdsClientProvider;
import software.amazon.rds.dbinstance.client.VersionedProxyClient;

import java.util.List;

public class ReadHandler extends BaseHandlerStd {

    public ReadHandler() {
        this(DEFAULT_DB_INSTANCE_HANDLER_CONFIG);
    }

    public ReadHandler(final HandlerConfig config) {
        super(config);
    }



    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ValidatedRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final VersionedProxyClient<RdsClient> rdsProxyClient,
            final VersionedProxyClient<Ec2Client> ec2ProxyClient
    ) {
        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
            .then(progress -> describeDbInstance(progress, proxy, rdsProxyClient))
            .then(progress -> {
                // If replication found for DB instance, we describe it
                if (!StringUtils.isNullOrEmpty(progress.getCallbackContext().getAutomaticBackupReplicationArn())) {
                    return describeAutomatedBackupsReplication(progress, proxy);
                }
                return progress;
            })
            .then(progress -> ProgressEvent.success(progress.getResourceModel(), progress.getCallbackContext()));
    }

    protected ProgressEvent<ResourceModel, CallbackContext> describeDbInstance(
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final AmazonWebServicesClientProxy proxy,
            final VersionedProxyClient<RdsClient> rdsProxyClient
    ) {
        final CallbackContext callbackContext = progress.getCallbackContext();
        final ResourceModel resourceModel = progress.getResourceModel();

        return proxy.initiate("rds::describe-db-instance", rdsProxyClient.defaultClient(), resourceModel, callbackContext)
            .translateToServiceRequest(Translator::describeDbInstancesRequest)
            .makeServiceCall((describeRequest, proxyInvocation) -> {
                return proxyInvocation.injectCredentialsAndInvokeV2(
                    describeRequest,
                    proxyInvocation.client()::describeDBInstances
                );
            })
            .handleError((describeRequest, exception, client, model, context) -> Commons.handleException(
                ProgressEvent.progress(model, context),
                exception,
                DEFAULT_DB_INSTANCE_ERROR_RULE_SET,
                requestLogger
            ))
            .done((describeRequest, describeResponse, automatedBackupProxyInvocation, model, context) -> {
                final DBInstance dbInstance = describeResponse.dbInstances().get(0);
                final ResourceModel currentModel = Translator.translateDbInstanceFromSdk(dbInstance);
                final List<DBInstanceAutomatedBackupsReplication> replications = dbInstance.dbInstanceAutomatedBackupsReplications();
                if (replications.isEmpty()) {
                    context.setAutomaticBackupReplicationStopped(true);
                    return ProgressEvent.progress(currentModel, context);
                }
                context.setAutomaticBackupReplicationArn(replications.get(0).dbInstanceAutomatedBackupsArn());

                return ProgressEvent.progress(currentModel, context);
            });
    }

    protected ProgressEvent<ResourceModel, CallbackContext> describeAutomatedBackupsReplication(
        final ProgressEvent<ResourceModel, CallbackContext> progress,
        final AmazonWebServicesClientProxy proxy
    ) {
        final CallbackContext callbackContext = progress.getCallbackContext();
        final ResourceModel resourceModel = progress.getResourceModel();

        if (StringUtils.isNullOrEmpty(callbackContext.getAutomaticBackupReplicationArn())) {
            return ProgressEvent.progress(resourceModel, callbackContext);
        }

        final String replicationRegion = ResourceModelHelper.getRegionFromArn(callbackContext.getAutomaticBackupReplicationArn());
        final ProxyClient<RdsClient> replicationRegionProxyClient =
            proxy.newProxy(() -> new RdsClientProvider().getClientForRegion(replicationRegion));

        return proxy.initiate("rds::describe-db-instance-automated-backups", replicationRegionProxyClient, resourceModel, callbackContext)
            .translateToServiceRequest(model -> Translator.describeDBInstanceAutomaticBackupRequest(callbackContext.getAutomaticBackupReplicationArn()))
            .backoffDelay(config.getBackoff())
            .makeServiceCall((describeRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                describeRequest,
                proxyInvocation.client()::describeDBInstanceAutomatedBackups
            ))
            .handleError((describeRequest, exception, client, model, context) ->Commons.handleException(
                ProgressEvent.progress(model, context),
                exception,
                DESCRIBE_AUTOMATED_BACKUPS_SOFTFAIL_ERROR_RULE_SET,
                requestLogger
            ))
            .done((describeRequest, describeResponse, proxyInvocation, model, context) -> {
                DBInstanceAutomatedBackup dbInstanceAutomatedBackup = describeResponse.dbInstanceAutomatedBackups().get(0);
                model.setAutomaticBackupReplicationRetentionPeriod(dbInstanceAutomatedBackup.backupRetentionPeriod());
                model.setAutomaticBackupReplicationRegion(replicationRegion);
                model.setAutomaticBackupReplicationKmsKeyId(dbInstanceAutomatedBackup.kmsKeyId());
                context.setAutomaticBackupReplicationStarted(true);
                return ProgressEvent.progress(model, context);
            });
    }
}
