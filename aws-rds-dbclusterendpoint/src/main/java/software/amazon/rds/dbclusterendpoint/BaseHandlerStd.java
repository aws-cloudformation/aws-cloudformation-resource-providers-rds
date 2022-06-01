package software.amazon.rds.dbclusterendpoint;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.*;
import software.amazon.cloudformation.proxy.*;
import software.amazon.rds.common.error.ErrorRuleSet;
import software.amazon.rds.common.error.ErrorStatus;
import software.amazon.rds.common.handler.Commons;

import java.util.HashMap;
import java.util.Map;

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {

    protected static final String DB_CLUSTER_ENDPOINT_AVAILABLE = "available";

    protected static final ErrorRuleSet DEFAULT_DB_CLUSTER_ENDPOINT_ERROR_RULE_SET = ErrorRuleSet.builder()
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    DbClusterEndpointAlreadyExistsException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    DbClusterEndpointNotFoundException.class,
                    DbClusterNotFoundException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ServiceLimitExceeded),
                    DbClusterEndpointQuotaExceededException.class)
            .build()
            .orElse(Commons.DEFAULT_ERROR_RULE_SET);

    @Override
    public final ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger) {
        return handleRequest(
                proxy,
                request,
                callbackContext != null ? callbackContext : new CallbackContext(),
                proxy.newProxy(ClientBuilder::getClient),
                logger
        );
    }

    protected abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger);

    protected boolean isStabilized(final ResourceModel model, final ProxyClient<RdsClient> proxyClient) {
        final String status = proxyClient.injectCredentialsAndInvokeV2(
                        Translator.describeDbClustersEndpointRequest(model),
                        proxyClient.client()::describeDBClusterEndpoints)
                .dbClusterEndpoints().stream().findFirst().get().status();
        return status.equals(DB_CLUSTER_ENDPOINT_AVAILABLE);
    }

    protected <K, V> Map<K, V> mergeMaps(Map<K, V> m1, Map<K, V> m2) {
        final Map<K, V> result = new HashMap<>();
        if (m1 != null) {
            result.putAll(m1);
        }
        if (m2 != null) {
            result.putAll(m2);
        }
        return result;
    }
}
