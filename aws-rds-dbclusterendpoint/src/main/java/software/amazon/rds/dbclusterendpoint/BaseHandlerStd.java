package software.amazon.rds.dbclusterendpoint;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBClusterEndpoint;
import software.amazon.awssdk.services.rds.model.DbClusterEndpointAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DbClusterEndpointNotFoundException;
import software.amazon.awssdk.services.rds.model.DbClusterEndpointQuotaExceededException;
import software.amazon.awssdk.services.rds.model.DbClusterNotFoundException;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterEndpointsResponse;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.common.error.ErrorRuleSet;
import software.amazon.rds.common.error.ErrorStatus;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;

import java.time.Duration;
import java.util.Optional;

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {

    protected static final String DB_CLUSTER_ENDPOINT_AVAILABLE = "available";
    protected static final String CUSTOM_ENDPOINT = "CUSTOM";

    protected static final Constant BACKOFF_DELAY = Constant.of()
            .timeout(Duration.ofSeconds(180L))
            .delay(Duration.ofSeconds(15L))
            .build();
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

    protected final HandlerConfig config;

    public BaseHandlerStd(final HandlerConfig config) {
        super();
        this.config = config;
    }

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
        final DBClusterEndpoint endpoint = fetchDBClusterEndpoint(model, proxyClient);
        return DB_CLUSTER_ENDPOINT_AVAILABLE.equals(endpoint.status());
    }

    protected ProgressEvent<ResourceModel, CallbackContext> updateTags(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet previousTags,
            final Tagging.TagSet desiredTags
    ) {
        return proxy.initiate("rds::tag-db-cluster-endpoint", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(Translator::describeDbClustersEndpointRequest)
                .backoffDelay(config.getBackoff())
                .makeServiceCall((describeRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        describeRequest,
                        proxyInvocation.client()::describeDBClusterEndpoints
                )).handleError((describeRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DEFAULT_DB_CLUSTER_ENDPOINT_ERROR_RULE_SET
                ))
                .done((describeRequest, describeResponse, invocation, resourceModel, ctx) -> {
                    final Tagging.TagSet tagsToAdd = Tagging.exclude(desiredTags, previousTags);
                    final Tagging.TagSet tagsToRemove = Tagging.exclude(previousTags, desiredTags);

                    if (tagsToAdd.isEmpty() && tagsToRemove.isEmpty()) {
                        return progress;
                    }

                    final String arn = describeResponse.dbClusterEndpoints().stream().findFirst().get().dbClusterEndpointArn();

                    try {
                        Tagging.removeTags(proxyClient, arn, Tagging.translateTagsToSdk(tagsToRemove));
                        Tagging.addTags(proxyClient, arn, Tagging.translateTagsToSdk(tagsToAdd));
                    } catch (Exception exception) {
                        return Commons.handleException(
                                progress,
                                exception,
                                Tagging.bestEffortErrorRuleSet(tagsToAdd, tagsToRemove).orElse(DEFAULT_DB_CLUSTER_ENDPOINT_ERROR_RULE_SET)
                        );
                    }
                    return progress;
                });

    }

    protected DBClusterEndpoint fetchDBClusterEndpoint(
            final ResourceModel model,
            final ProxyClient<RdsClient> proxyClient
    ) {
        final DescribeDbClusterEndpointsResponse response = proxyClient.injectCredentialsAndInvokeV2(
                Translator.describeDbClustersEndpointRequest(model),
                proxyClient.client()::describeDBClusterEndpoints
        );

        final Optional<DBClusterEndpoint> clusterEndpoint = response
                .dbClusterEndpoints().stream().findFirst();

        return clusterEndpoint.orElseThrow(() -> new CfnNotFoundException(ResourceModel.TYPE_NAME,
                "DBClusterEndpoint " + model.getDBClusterEndpointIdentifier() + " not found"));
    }
}
