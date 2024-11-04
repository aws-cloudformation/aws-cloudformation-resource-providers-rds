package software.amazon.rds.dbinstance.common.create;

import lombok.AllArgsConstructor;
import org.apache.commons.lang3.NotImplementedException;
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
public class FromPointInTime implements DBInstanceFactory {

    private final AmazonWebServicesClientProxy proxy;
    private final VersionedProxyClient<RdsClient> rdsProxyClient;
    private final Tagging.TagSet allTags;
    private final RequestLogger requestLogger;
    private final HandlerConfig config;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> create(ProgressEvent<ResourceModel, CallbackContext> progress) {
        return safeAddTags(this::restoreDbInstanceToPointInTimeRequest)
                .invoke(proxy, rdsProxyClient.defaultClient(), progress, allTags);
    }

    @Override
    public boolean modelSatisfiesConstructor(ResourceModel model) {
        return ResourceModelHelper.isRestoreToPointInTime(model);
    }

    private ProgressEvent<ResourceModel, CallbackContext> restoreDbInstanceToPointInTimeRequest(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet tagSet
    ) {
        final Fetch fetch = new Fetch(rdsProxyClient);
        return proxy.initiate(
                        "rds::restore-db-instance-to-point-in-time",
                        rdsProxyClient,
                        progress.getResourceModel(),
                        progress.getCallbackContext()
                ).translateToServiceRequest(model -> Translator.restoreDbInstanceToPointInTimeRequest(model, tagSet))
                .backoffDelay(config.getBackoff())
                .makeServiceCall((restoreRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        restoreRequest,
                        proxyInvocation.client()::restoreDBInstanceToPointInTime
                ))
                .stabilize((request, response, proxyInvocation, model, context) -> {
                    final DBInstance dbInstance = fetch.dbInstance(model);
                    return DBInstancePredicates.isDBInstanceStabilizedAfterMutate(dbInstance, model, context, requestLogger);
                })
                .handleError((request, exception, client, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        ErrorRuleSets.RESTORE_DB_INSTANCE,
                        requestLogger
                ))
                .progress();
    }

    private HandlerMethod<ResourceModel, CallbackContext> safeAddTags(final HandlerMethod<ResourceModel, CallbackContext> handlerMethod) {
        return (proxy, rdsProxyClient, progress, tagSet) -> progress.then(p -> Tagging.createWithTaggingFallback(proxy, rdsProxyClient, handlerMethod, progress, tagSet));
    }
}
