package software.amazon.rds.dbinstance.common.create;

import lombok.AllArgsConstructor;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.HandlerMethod;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.dbinstance.CallbackContext;
import software.amazon.rds.dbinstance.DBInstancePredicates;
import software.amazon.rds.dbinstance.ResourceModel;
import software.amazon.rds.dbinstance.Translator;
import software.amazon.rds.dbinstance.client.VersionedProxyClient;
import software.amazon.rds.dbinstance.common.ErrorRuleSets;
import software.amazon.rds.dbinstance.common.Fetch;
import software.amazon.rds.dbinstance.util.ResourceModelHelper;

@AllArgsConstructor
public class ReadReplica implements DBInstanceFactory {

    private final AmazonWebServicesClientProxy proxy;
    private final VersionedProxyClient<RdsClient> rdsProxyClient;
    private final Tagging.TagSet allTags;
    private final RequestLogger requestLogger;
    private final HandlerConfig config;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> create(ProgressEvent<ResourceModel, CallbackContext> progress) {
        return safeAddTags(this::createDbInstanceReadReplica)
                .invoke(proxy, rdsProxyClient.defaultClient(), progress, allTags);
    }

    @Override
    public boolean modelSatisfiesConstructor(ResourceModel model) {
        return ResourceModelHelper.isReadReplica(model);
    }

    private ProgressEvent<ResourceModel, CallbackContext> createDbInstanceReadReplica(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet tagSet
    ) {
        final Fetch fetch = new Fetch(rdsProxyClient);
        final String currentRegion = progress.getCallbackContext().getCurrentRegion();
        return proxy.initiate(
                        "rds::create-db-instance-read-replica",
                        rdsProxyClient,
                        progress.getResourceModel(),
                        progress.getCallbackContext()
                ).translateToServiceRequest(model -> Translator.createDbInstanceReadReplicaRequest(model, tagSet, currentRegion))
                .backoffDelay(config.getBackoff())
                .makeServiceCall((createRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        createRequest,
                        proxyInvocation.client()::createDBInstanceReadReplica
                ))
                .stabilize((request, response, proxyInvocation, model, context) -> {
                    final DBInstance dbInstance = fetch.dbInstance(model);
                    return DBInstancePredicates.isDBInstanceStabilizedAfterMutate(dbInstance, model, context, requestLogger);
                })
                .handleError((request, exception, client, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        ErrorRuleSets.CREATE_DB_INSTANCE_READ_REPLICA,
                        requestLogger
                ))
                .progress();
    }

    private HandlerMethod<ResourceModel, CallbackContext> safeAddTags(final HandlerMethod<ResourceModel, CallbackContext> handlerMethod) {
        return (proxy, rdsProxyClient, progress, tagSet) -> progress.then(p -> Tagging.createWithTaggingFallback(proxy, rdsProxyClient, handlerMethod, progress, tagSet));
    }
}
