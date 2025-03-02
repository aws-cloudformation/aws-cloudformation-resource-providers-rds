package software.amazon.rds.dbinstance.common.create;

import com.amazonaws.util.StringUtils;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.BooleanUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DBSnapshot;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsResponse;
import software.amazon.awssdk.utils.ImmutableMap;
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
import software.amazon.rds.dbinstance.client.ApiVersion;
import software.amazon.rds.dbinstance.client.ApiVersionDispatcher;
import software.amazon.rds.dbinstance.client.VersionedProxyClient;
import software.amazon.rds.dbinstance.common.ErrorRuleSets;
import software.amazon.rds.dbinstance.common.Fetch;
import software.amazon.rds.dbinstance.util.ResourceModelHelper;

import java.util.Map;

@AllArgsConstructor
public class FromSnapshot implements DBInstanceFactory {

    private final AmazonWebServicesClientProxy proxy;
    private final VersionedProxyClient<RdsClient> rdsProxyClient;
    private final Tagging.TagSet allTags;
    private final RequestLogger requestLogger;
    private final HandlerConfig config;
    private final ApiVersionDispatcher<ResourceModel, CallbackContext> apiVersionDispatcher;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> create(ProgressEvent<ResourceModel, CallbackContext> progress) {
        final ResourceModel model = progress.getResourceModel();
        final CallbackContext callbackContext = progress.getCallbackContext();
        final ApiVersion apiVersion = selectVersion(model, callbackContext);

        final Fetch fetch = new Fetch(rdsProxyClient.defaultClient());
        final boolean isMultiAZ = BooleanUtils.isTrue(model.getMultiAZ());
        if (ResourceModelHelper.isRestoreFromSnapshot(model) && !isMultiAZ) {
            try {
                final DBSnapshot snapshot = fetch.dbSnapshot(model);
                final String engine = snapshot.engine();
                if (StringUtils.isNullOrEmpty(model.getEngine())) {
                    model.setEngine(engine);
                }
                if (model.getMultiAZ() == null) {
                    model.setMultiAZ(ResourceModelHelper.getDefaultMultiAzForEngine(engine));
                }
            } catch (Exception e) {
                return Commons.handleException(progress, e, ErrorRuleSets.RESTORE_DB_INSTANCE, requestLogger);
            }
        }

        if (apiVersion == ApiVersion.V12) {
            return restoreDbInstanceFromSnapshotV12(proxy, rdsProxyClient.forVersion(apiVersion), progress, allTags);
        }

        return safeAddTags(this::restoreDbInstanceFromSnapshot).invoke(proxy, rdsProxyClient.forVersion(apiVersion), progress, allTags);
    }

    @Override
    public boolean modelSatisfiesConstructor(ResourceModel model) {
        return ResourceModelHelper.isRestoreFromSnapshot(model) || ResourceModelHelper.isRestoreFromClusterSnapshot(model);
    }

    private ProgressEvent<ResourceModel, CallbackContext> restoreDbInstanceFromSnapshotV12(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet tagSet
    ) {
        requestLogger.log("RestoreDbInstanceFromSnapshotAPIv12Invoked");
        requestLogger.log("API version 12 restore detected",
                "This indicates that the customer is using DBSecurityGroup, which may result in certain features not" +
                        " functioning properly. Please refer to the API model for supported parameters");
        final Fetch fetch = new Fetch(rdsProxyClient);
        return proxy.initiate(
                        "rds::restore-db-instance-from-snapshot-v12",
                        rdsProxyClient,
                        progress.getResourceModel(),
                        progress.getCallbackContext()
                ).translateToServiceRequest(Translator::restoreDbInstanceFromSnapshotRequestV12)
                .backoffDelay(config.getBackoff())
                .makeServiceCall((restoreRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        restoreRequest,
                        proxyInvocation.client()::restoreDBInstanceFromDBSnapshot
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

    private ProgressEvent<ResourceModel, CallbackContext> restoreDbInstanceFromSnapshot(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet tagSet
    ) {
        final Fetch fetch = new Fetch(rdsProxyClient);
        return proxy.initiate(
                        "rds::restore-db-instance-from-snapshot",
                        rdsProxyClient,
                        progress.getResourceModel(),
                        progress.getCallbackContext()
                ).translateToServiceRequest(model -> Translator.restoreDbInstanceFromSnapshotRequest(model, tagSet))
                .backoffDelay(config.getBackoff())
                .makeServiceCall((restoreRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        restoreRequest,
                        proxyInvocation.client()::restoreDBInstanceFromDBSnapshot
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

    private ApiVersion selectVersion(ResourceModel model, CallbackContext callbackContext) {
        return apiVersionDispatcher.dispatch(model, callbackContext);
    }

    private HandlerMethod<ResourceModel, CallbackContext> safeAddTags(final HandlerMethod<ResourceModel, CallbackContext> handlerMethod) {
        return (proxy, rdsProxyClient, progress, tagSet) -> progress.then(p -> Tagging.createWithTaggingFallback(proxy, rdsProxyClient, handlerMethod, progress, tagSet));
    }
}
