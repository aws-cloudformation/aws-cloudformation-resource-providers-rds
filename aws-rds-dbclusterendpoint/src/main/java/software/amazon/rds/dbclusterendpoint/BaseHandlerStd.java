package software.amazon.rds.dbclusterendpoint;

import java.time.Duration;
import java.util.Collection;
import java.util.Optional;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBClusterEndpoint;
import software.amazon.awssdk.services.rds.model.DbClusterEndpointAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DbClusterEndpointNotFoundException;
import software.amazon.awssdk.services.rds.model.DbClusterEndpointQuotaExceededException;
import software.amazon.awssdk.services.rds.model.DbClusterNotFoundException;
import software.amazon.awssdk.services.rds.model.DbInstanceNotFoundException;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterEndpointsResponse;
import software.amazon.awssdk.services.rds.model.InvalidDbClusterEndpointStateException;
import software.amazon.awssdk.services.rds.model.InvalidDbClusterStateException;
import software.amazon.awssdk.services.rds.model.InvalidDbInstanceStateException;
import software.amazon.awssdk.services.rds.model.Tag;
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

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {
    protected static final int RESOURCE_ID_MAX_LENGTH = 63;
    protected static final String RESOURCE_IDENTIFIER = "dbclusterendpoint";
    protected static final String DEFAULT_STACK_NAME = "rds";
    protected static final String DB_CLUSTER_ENDPOINT_AVAILABLE = "available";
    protected static final String CUSTOM_ENDPOINT = "CUSTOM";

    protected static final Constant BACKOFF_DELAY = Constant.of()
            .timeout(Duration.ofSeconds(180L))
            .delay(Duration.ofSeconds(15L))
            .build();

    protected static final ErrorRuleSet DEFAULT_DB_CLUSTER_ENDPOINT_ERROR_RULE_SET = ErrorRuleSet
            .extend(Commons.DEFAULT_ERROR_RULE_SET)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    DbClusterEndpointAlreadyExistsException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    DbClusterEndpointNotFoundException.class,
                    DbClusterNotFoundException.class,
                    DbInstanceNotFoundException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ServiceLimitExceeded),
                    DbClusterEndpointQuotaExceededException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
                    InvalidDbInstanceStateException.class,
                    InvalidDbClusterStateException.class,
                    InvalidDbClusterEndpointStateException.class)
            .build();

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
                proxy.newProxy(new ClientProvider()::getClient),
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
            final String dbClusterEndpointArn,
            final Tagging.TagSet previousTags,
            final Tagging.TagSet desiredTags
    ) {
        final Collection<Tag> effectivePreviousTags = Tagging.translateTagsToSdk(previousTags);
        final Collection<Tag> effectiveDesiredTags = Tagging.translateTagsToSdk(desiredTags);

        final Collection<Tag> tagsToRemove = Tagging.exclude(effectivePreviousTags, effectiveDesiredTags);
        final Collection<Tag> tagsToAdd = Tagging.exclude(effectiveDesiredTags, effectivePreviousTags);

        if (tagsToAdd.isEmpty() && tagsToRemove.isEmpty()) {
            return progress;
        }

        final Tagging.TagSet rulesetTagsToAdd = Tagging.exclude(desiredTags, previousTags);
        final Tagging.TagSet rulesetTagsToRemove = Tagging.exclude(previousTags, desiredTags);

        try {
            Tagging.removeTags(proxyClient, dbClusterEndpointArn, Tagging.translateTagsToSdk(tagsToRemove));
            Tagging.addTags(proxyClient, dbClusterEndpointArn, Tagging.translateTagsToSdk(tagsToAdd));
        } catch (Exception exception) {
            return Commons.handleException(
                    progress,
                    exception,
                    DEFAULT_DB_CLUSTER_ENDPOINT_ERROR_RULE_SET.extendWith(Tagging.bestEffortErrorRuleSet(rulesetTagsToAdd, rulesetTagsToRemove))
            );
        }
        return progress;
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

        return clusterEndpoint.orElseThrow(() -> DbClusterEndpointNotFoundException.builder().message(
                "DBClusterEndpoint " + model.getDBClusterEndpointIdentifier() + " not found").build());
    }
}
