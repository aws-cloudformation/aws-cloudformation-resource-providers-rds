package software.amazon.rds.dbclusterendpoint;

import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.time.Instant;

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
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.*;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.common.error.ErrorRuleSet;
import software.amazon.rds.common.error.ErrorStatus;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.logging.LoggingProxyClient;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.common.printer.FilteredJsonPrinter;

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {
    protected static final int RESOURCE_ID_MAX_LENGTH = 63;
    protected static final String RESOURCE_IDENTIFIER = "dbclusterendpoint";
    protected static final String DEFAULT_STACK_NAME = "rds";
    protected static final String DB_CLUSTER_ENDPOINT_AVAILABLE = "available";
    protected static final String CUSTOM_ENDPOINT = "CUSTOM";
    protected static final String DB_CLUSTER_ENDPOINT_REQUEST_STARTED_AT = "dbclusterendpoint-request-started-at";
    protected static final String DB_CLUSTER_ENDPOINT_REQUEST_IN_PROGRESS_AT = "dbclusterendpoint-request-in-progress-at";
    protected static final String DB_CLUSTER_ENDPOINT_STABILIZATION_TIME = "dbclusterendpoint-stabilization-time";

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

    private final FilteredJsonPrinter PARAMETERS_FILTER = new FilteredJsonPrinter();
    protected final HandlerConfig config;
    protected RequestLogger requestLogger;

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
        final CallbackContext context = callbackContext != null ? callbackContext : new CallbackContext();
        return RequestLogger.handleRequest(
                logger,
                request,
                PARAMETERS_FILTER,
                requestLogger -> handleRequest(
                        proxy,
                        new LoggingProxyClient<>(requestLogger, proxy.newProxy(new ClientProvider()::getClient)), request,
                        context, requestLogger
                ));
    }

    protected abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext);

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final RequestLogger requestLogger
    ){
        this.requestLogger = requestLogger;
        resourceStabilizationTime(callbackContext);
        return handleRequest(proxy, proxyClient, request, callbackContext);
    }

    protected boolean isStabilized(final ResourceModel model, final ProxyClient<RdsClient> proxyClient) {
        final DBClusterEndpoint endpoint = fetchDBClusterEndpoint(model, proxyClient);
        return DB_CLUSTER_ENDPOINT_AVAILABLE.equals(endpoint.status());
    }

    private void resourceStabilizationTime(final CallbackContext callbackContext) {
        callbackContext.timestampOnce(DB_CLUSTER_ENDPOINT_REQUEST_STARTED_AT, Instant.now());
        callbackContext.timestamp(DB_CLUSTER_ENDPOINT_REQUEST_IN_PROGRESS_AT, Instant.now());
        callbackContext.calculateTimeDeltaInMinutes(DB_CLUSTER_ENDPOINT_STABILIZATION_TIME,
                callbackContext.getTimestamp(DB_CLUSTER_ENDPOINT_REQUEST_IN_PROGRESS_AT),
                callbackContext.getTimestamp(DB_CLUSTER_ENDPOINT_REQUEST_STARTED_AT));
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
                    DEFAULT_DB_CLUSTER_ENDPOINT_ERROR_RULE_SET.extendWith(Tagging.getUpdateTagsAccessDeniedRuleSet(rulesetTagsToAdd, rulesetTagsToRemove)),
                    requestLogger
            );
        }
        return progress;
    }

    protected DBClusterEndpoint fetchDBClusterEndpoint(
            final ResourceModel model,
            final ProxyClient<RdsClient> proxyClient
    ) {
        try {
            final DescribeDbClusterEndpointsResponse response = proxyClient.injectCredentialsAndInvokeV2(
                    Translator.describeDbClustersEndpointRequest(model),
                    proxyClient.client()::describeDBClusterEndpoints
            );

            final Optional<DBClusterEndpoint> clusterEndpoint = response
                    .dbClusterEndpoints().stream().findFirst();

            return clusterEndpoint.orElseThrow(() -> new CfnNotFoundException(ResourceModel.TYPE_NAME, model.getDBClusterEndpointIdentifier()));
        } catch (DbClusterEndpointNotFoundException e) {
            throw new CfnNotFoundException(e);
        }
    }
}
