package software.amazon.rds.dbinstance.common.create;

import lombok.AllArgsConstructor;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesResponse;
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
import software.amazon.rds.dbinstance.common.Errors;

import java.util.Map;

@AllArgsConstructor
public class FreshInstance implements DBInstanceFactory {

    private final AmazonWebServicesClientProxy proxy;
    private final VersionedProxyClient<RdsClient> rdsProxyClient;
    private final Tagging.TagSet allTags;
    private final RequestLogger requestLogger;
    private final HandlerConfig config;
    private final ApiVersionDispatcher<ResourceModel, CallbackContext> apiVersionDispatcher;

    public ProgressEvent<ResourceModel, CallbackContext> create(ProgressEvent<ResourceModel, CallbackContext> progress) {
        return versioned(proxy, rdsProxyClient, progress, allTags, ImmutableMap.of(
            ApiVersion.V12, this::createDbInstanceV12,
            ApiVersion.DEFAULT, safeAddTags(this::createDbInstance)
        ));
    }

    private ProgressEvent<ResourceModel, CallbackContext> createDbInstance(
        final AmazonWebServicesClientProxy proxy,
        final ProxyClient<RdsClient> rdsProxyClient,
        final ProgressEvent<ResourceModel, CallbackContext> progress,
        final Tagging.TagSet tagSet
    ) {
        return proxy.initiate(
                "rds::create-db-instance",
                rdsProxyClient,
                progress.getResourceModel(),
                progress.getCallbackContext()
            ).translateToServiceRequest(model -> Translator.createDbInstanceRequest(model, tagSet))
            .backoffDelay(config.getBackoff())
            .makeServiceCall((createRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                createRequest,
                proxyInvocation.client()::createDBInstance
            ))
            .stabilize((request, response, proxyInvocation, model, context) ->
            {
                final DBInstance dbInstance = fetchDBInstance(rdsProxyClient, model);
                return DBInstancePredicates.isDBInstanceStabilizedAfterMutate(dbInstance, model, context, requestLogger) ;
            })
            .handleError((request, exception, client, model, context) -> Commons.handleException(
                ProgressEvent.progress(model, context),
                exception,
                Errors.CREATE_DB_INSTANCE_ERROR_RULE_SET,
                requestLogger
            ))
            .progress();
    }

    private DBInstance fetchDBInstance(
        final ProxyClient<RdsClient> rdsProxyClient,
        final ResourceModel model
    ) {
        final DescribeDbInstancesResponse response = rdsProxyClient.injectCredentialsAndInvokeV2(
            Translator.describeDbInstancesRequest(model),
            rdsProxyClient.client()::describeDBInstances
        );
        return response.dbInstances().get(0);
    }

    private ProgressEvent<ResourceModel, CallbackContext> createDbInstanceV12(
        final AmazonWebServicesClientProxy proxy,
        final ProxyClient<RdsClient> rdsProxyClient,
        final ProgressEvent<ResourceModel, CallbackContext> progress,
        final Tagging.TagSet tagSet
    ) {
        requestLogger.log("CreateDbInstanceAPIv12Invoked");
        requestLogger.log("API version 12 create detected",
            "This indicates that the customer is using DBSecurityGroup, which may result in certain features not" +
                " functioning properly. Please refer to the API model for supported parameters");
        return proxy.initiate(
                "rds::create-db-instance-v12",
                rdsProxyClient,
                progress.getResourceModel(),
                progress.getCallbackContext()
            ).translateToServiceRequest(Translator::createDbInstanceRequestV12)
            .backoffDelay(config.getBackoff())
            .makeServiceCall((createRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                createRequest,
                proxyInvocation.client()::createDBInstance
            ))
            .stabilize((request, response, proxyInvocation, model, context) -> {
                final DBInstance dbInstance = fetchDBInstance(rdsProxyClient, model);
                return DBInstancePredicates.isDBInstanceStabilizedAfterMutate(dbInstance, model, context, requestLogger) ;
            })
            .handleError((request, exception, client, model, context) -> Commons.handleException(
                ProgressEvent.progress(model, context),
                exception,
                Errors.CREATE_DB_INSTANCE_ERROR_RULE_SET,
                requestLogger
            ))
            .progress();
    }

    protected ProgressEvent<ResourceModel, CallbackContext> versioned(
        final AmazonWebServicesClientProxy proxy,
        final VersionedProxyClient<RdsClient> rdsProxyClient,
        final ProgressEvent<ResourceModel, CallbackContext> progress,
        final Tagging.TagSet allTags,
        final Map<ApiVersion, HandlerMethod<ResourceModel, CallbackContext>> methodVersions
    ) {
        final ResourceModel model = progress.getResourceModel();
        final CallbackContext callbackContext = progress.getCallbackContext();
        final ApiVersion apiVersion = getApiVersionDispatcher().dispatch(model, callbackContext);
        if (!methodVersions.containsKey(apiVersion)) {
            throw new RuntimeException("Missing method version");
        }
        return methodVersions.get(apiVersion).invoke(proxy, rdsProxyClient.forVersion(apiVersion), progress, allTags);
    }

    protected ApiVersionDispatcher<ResourceModel, CallbackContext> getApiVersionDispatcher() {
        return apiVersionDispatcher;
    }

    private HandlerMethod<ResourceModel, CallbackContext> safeAddTags(final HandlerMethod<ResourceModel, CallbackContext> handlerMethod) {
        return (proxy, rdsProxyClient, progress, tagSet) -> progress.then(p -> Tagging.createWithTaggingFallback(proxy, rdsProxyClient, handlerMethod, progress, tagSet));
    }


}
