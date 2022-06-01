package software.amazon.rds.dbclusterendpoint;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DbClusterEndpointAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DbClusterEndpointNotFoundException;
import software.amazon.awssdk.services.rds.model.DbClusterEndpointQuotaExceededException;
import software.amazon.awssdk.services.rds.model.DbClusterNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.error.ErrorRuleSet;
import software.amazon.rds.common.error.ErrorStatus;
import software.amazon.rds.common.handler.Commons;

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
}
