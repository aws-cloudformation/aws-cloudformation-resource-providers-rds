package software.amazon.rds.dbinstance;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.HandlerMethod;
import software.amazon.rds.common.handler.Tagging;

public class CreateHandlerV12 extends BaseCreateHandler {

    public CreateHandlerV12(final HandlerConfig config) {
        super(config);
    }

    @Override
    protected RdsClient buildRdsClient() {
        return RdsClientBuilder.getClient(API_VERSION_V12);
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> createDbInstance(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet tagSet
    ) {
        return proxy.initiate(
                        "rds::create-db-instance-v12",
                        rdsProxyClient,
                        progress.getResourceModel(),
                        progress.getCallbackContext()
                ).translateToServiceRequest(TranslatorV12::createDbInstanceRequest)
                .backoffDelay(config.getBackoff())
                .makeServiceCall((createRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        createRequest,
                        proxyInvocation.client()::createDBInstance
                ))
                .stabilize((request, response, proxyInvocation, model, context) ->
                        isDbInstanceStabilized(proxyInvocation, model))
                .handleError((request, exception, client, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        CREATE_DB_INSTANCE_ERROR_RULE_SET
                ))
                .progress();
    }

    protected ProgressEvent<ResourceModel, CallbackContext> createDbInstanceReadReplica(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet tagSet
    ) {
        return proxy.initiate(
                        "rds::create-db-instance-read-replica-v12",
                        rdsProxyClient,
                        progress.getResourceModel(),
                        progress.getCallbackContext()
                ).translateToServiceRequest(TranslatorV12::createDbInstanceReadReplicaRequest)
                .backoffDelay(config.getBackoff())
                .makeServiceCall((createRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        createRequest,
                        proxyInvocation.client()::createDBInstanceReadReplica
                ))
                .stabilize((request, response, proxyInvocation, model, context) ->
                        isDbInstanceStabilized(proxyInvocation, model))
                .handleError((request, exception, client, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        CREATE_DB_INSTANCE_READ_REPLICA_ERROR_RULE_SET
                ))
                .progress();
    }

    protected ProgressEvent<ResourceModel, CallbackContext> restoreDbInstanceFromSnapshot(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet tagSet
    ) {
        return proxy.initiate(
                        "rds::restore-db-instance-from-snapshot-v12",
                        rdsProxyClient,
                        progress.getResourceModel(),
                        progress.getCallbackContext()
                ).translateToServiceRequest(TranslatorV12::restoreDbInstanceFromSnapshotRequest)
                .backoffDelay(config.getBackoff())
                .makeServiceCall((restoreRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        restoreRequest,
                        proxyInvocation.client()::restoreDBInstanceFromDBSnapshot
                ))
                .stabilize((request, response, proxyInvocation, model, context) ->
                        isDbInstanceStabilized(proxyInvocation, model))
                .handleError((request, exception, client, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        RESTORE_DB_INSTANCE_ERROR_RULE_SET
                ))
                .progress();
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> updateDbInstanceAfterCreate(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final ResourceModel desiredModel
    ) {
        return proxy.initiate(
                        "rds::modify-after-create-db-instance-v12",
                        rdsProxyClient,
                        progress.getResourceModel(),
                        progress.getCallbackContext()
                )
                .translateToServiceRequest(resourceModel -> TranslatorV12.modifyDbInstanceRequest(null, desiredModel, false))
                .backoffDelay(config.getBackoff())
                .makeServiceCall((modifyRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        modifyRequest,
                        proxyInvocation.client()::modifyDBInstance
                ))
                .stabilize((request, response, proxyInvocation, model, context) -> withProbing(
                        context,
                        "modify-after-create-db-instance-available",
                        3,
                        () -> isDbInstanceStabilized(proxyInvocation, model)
                ))
                .handleError((request, exception, client, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        MODIFY_DB_INSTANCE_ERROR_RULE_SET
                ))
                .progress();
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> safeCreate(
            final HandlerMethod<ResourceModel, CallbackContext> createMethod,
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet allTags
    ) {
        return createMethod.invoke(proxy, rdsProxyClient, progress, allTags);
    }
}
