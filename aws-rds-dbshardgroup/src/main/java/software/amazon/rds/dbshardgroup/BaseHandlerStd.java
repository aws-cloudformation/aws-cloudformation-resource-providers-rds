package software.amazon.rds.dbshardgroup;

import com.amazonaws.arn.Arn;
import com.amazonaws.arn.ArnResource;
import java.util.Set;
import java.util.function.BiFunction;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBShardGroup;
import software.amazon.awssdk.services.rds.model.DbClusterNotFoundException;
import software.amazon.awssdk.services.rds.model.DbShardGroupAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DbShardGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.InvalidDbClusterStateException;
import software.amazon.awssdk.services.rds.model.InvalidVpcNetworkStateException;
import software.amazon.awssdk.services.rds.model.MaxDbShardGroupLimitReachedException;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.awssdk.services.rds.model.UnsupportedDbEngineVersionException;
import software.amazon.awssdk.utils.ImmutableMap;
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
import software.amazon.rds.common.logging.LoggingProxyClient;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.common.printer.FilteredJsonPrinter;

import java.time.Duration;
import java.util.Collection;

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {
    protected final static HandlerConfig DEFAULT_DB_SHARD_GROUP_HANDLER_CONFIG = HandlerConfig.builder()
            .backoff(Constant.of().delay(Duration.ofSeconds(30)).timeout(Duration.ofMinutes(180)).build())
            .build();

    protected final static HandlerConfig DB_SHARD_GROUP_HANDLER_CONFIG_36H = HandlerConfig.builder()
            .backoff(Constant.of().delay(Duration.ofSeconds(30)).timeout(Duration.ofHours(36)).build())
            .build();

    protected static final ErrorRuleSet DEFAULT_DB_SHARD_GROUP_ERROR_RULE_SET = ErrorRuleSet
            .extend(Commons.DEFAULT_ERROR_RULE_SET)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    DbShardGroupAlreadyExistsException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    DbShardGroupNotFoundException.class,
                    DbClusterNotFoundException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ServiceLimitExceeded),
                    MaxDbShardGroupLimitReachedException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
                    UnsupportedDbEngineVersionException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
                    InvalidDbClusterStateException.class,
                    InvalidVpcNetworkStateException.class)
            .build();

    private final FilteredJsonPrinter PARAMETERS_FILTER = new FilteredJsonPrinter();

    /**
     * Custom handler config, mostly to facilitate faster unit test
     */
    final HandlerConfig config;
    protected RequestLogger requestLogger;
    protected static final BiFunction<ResourceModel, ProxyClient<RdsClient>, ResourceModel> NOOP_CALL = (model, proxyClient) -> model;

    public BaseHandlerStd() {
        this(HandlerConfig.builder().build());
    }

    BaseHandlerStd(HandlerConfig config) {
        this.config = config;
    }

    @Override
    public final ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger) {
        return RequestLogger.handleRequest(
                logger,
                request,
                PARAMETERS_FILTER,
                requestLogger -> handleRequest(
                        proxy,
                        new LoggingProxyClient<>(requestLogger, proxy.newProxy(new ClientProvider()::getClient)),
                        request,
                        callbackContext != null ? callbackContext : new CallbackContext(),
                        requestLogger
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
            final CallbackContext context,
            final RequestLogger requestLogger
    ) {
        this.requestLogger = requestLogger;
        return handleRequest(proxy, proxyClient, request, context);
    }

    protected boolean isDBShardGroupStabilizedAfterMutate(
            final ResourceModel model,
            final ProxyClient<RdsClient> proxyClient
    ) {
        boolean isDBShardGroupStabilized = isDBShardGroupStabilized(model, proxyClient);
        boolean isDBClusterStabilized = isDBClusterStabilized(model, proxyClient);

        requestLogger.log(String.format("isDBShardGroupStabilizedAfterMutate: %b", isDBShardGroupStabilized && isDBClusterStabilized),
                ImmutableMap.of("isDBShardGroupStabilized", isDBShardGroupStabilized,
                        "isDBClusterStabilized", isDBClusterStabilized),
                ImmutableMap.of("Description", "isDBShardGroupStabilizedAfterMutate method will be repeatedly" +
                " called with a backoff mechanism after the modify call until it returns true. This" +
                " process will continue until all included flags are true."));

        return isDBShardGroupStabilized && isDBClusterStabilized;
    }

    protected boolean isDBShardGroupStabilized(
            final ResourceModel model,
            final ProxyClient<RdsClient> proxyClient
    ) {
        final DBShardGroup dbShardGroup = proxyClient.injectCredentialsAndInvokeV2(
                Translator.describeDbShardGroupsRequest(model),
                proxyClient.client()::describeDBShardGroups
        ).dbShardGroups().get(0);

        return isDBShardGroupAvailable(dbShardGroup);
    }

    protected boolean isDBClusterStabilized(
            final ResourceModel model,
            final ProxyClient<RdsClient> proxyClient
    ) {
        final DBCluster dbCluster = proxyClient.injectCredentialsAndInvokeV2(
                Translator.describeDbClustersRequest(model),
                proxyClient.client()::describeDBClusters
        ).dbClusters().get(0);

        return isDBClusterAvailable(dbCluster);
    }

    protected boolean isDBShardGroupAvailable(final DBShardGroup dbShardGroup) {
        return ResourceStatus.AVAILABLE.equalsString(dbShardGroup.status());
    }

    protected boolean isDBClusterAvailable(final DBCluster dbCluster) {
        return ResourceStatus.AVAILABLE.equalsString(dbCluster.status());
    }

    protected ProgressEvent<ResourceModel, CallbackContext> addTags(
            final ProxyClient<RdsClient> proxyClient,
            final ResourceHandlerRequest request,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet desiredTags
    ) {
        DBShardGroup dbShardGroup;
        try {
            dbShardGroup = proxyClient.injectCredentialsAndInvokeV2(
                    Translator.describeDbShardGroupsRequest(progress.getResourceModel()), proxyClient.client()::describeDBShardGroups
            ).dbShardGroups().get(0);
        } catch (Exception exception) {
            return Commons.handleException(progress, exception, DEFAULT_DB_SHARD_GROUP_ERROR_RULE_SET, requestLogger);
        }

        final String arn = assembleArn(request.getAwsPartition(), request.getRegion(), request.getAwsAccountId(), dbShardGroup.dbShardGroupResourceId());

        try {
            Tagging.addTags(proxyClient, arn, Tagging.translateTagsToSdk(desiredTags));
        } catch (Exception exception) {
            return Commons.handleException(
                    progress,
                    exception,
                    DEFAULT_DB_SHARD_GROUP_ERROR_RULE_SET.extendWith(Tagging.getUpdateTagsAccessDeniedRuleSet(desiredTags, Tagging.TagSet.emptySet())),
                    requestLogger
            );
        }

        return progress;
    }

    protected ProgressEvent<ResourceModel, CallbackContext> updateTags(
            final ProxyClient<RdsClient> proxyClient,
            final ResourceHandlerRequest request,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
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

        DBShardGroup dbShardGroup;
        try {
            dbShardGroup = proxyClient.injectCredentialsAndInvokeV2(
                    Translator.describeDbShardGroupsRequest(progress.getResourceModel()), proxyClient.client()::describeDBShardGroups
            ).dbShardGroups().get(0);
        } catch (Exception exception) {
            return Commons.handleException(progress, exception, DEFAULT_DB_SHARD_GROUP_ERROR_RULE_SET, requestLogger);
        }

        final String arn = assembleArn(request.getAwsPartition(), request.getRegion(), request.getAwsAccountId(), dbShardGroup.dbShardGroupResourceId());

        try {
            Tagging.removeTags(proxyClient, arn, Tagging.translateTagsToSdk(tagsToRemove));
            Tagging.addTags(proxyClient, arn, Tagging.translateTagsToSdk(tagsToAdd));
        } catch (Exception exception) {
            return Commons.handleException(
                    progress,
                    exception,
                    DEFAULT_DB_SHARD_GROUP_ERROR_RULE_SET.extendWith(Tagging.getUpdateTagsAccessDeniedRuleSet(rulesetTagsToAdd, rulesetTagsToRemove)),
                    requestLogger
            );
        }

        return progress;
    }

    protected Set<Tag> getTags(
            final ProxyClient<RdsClient> proxyClient,
            final ResourceHandlerRequest request,
            final DBShardGroup dbShardGroup
    ) {
        String arn = assembleArn(request.getAwsPartition(), request.getRegion(), request.getAwsAccountId(), dbShardGroup.dbShardGroupResourceId());
        return Tagging.listTagsForResource(proxyClient, arn);
    }

    private String assembleArn(String partition, String region, String accountId, String dbShardGroupResourceId) {
        return Arn.builder()
                .withPartition(partition)
                .withService("rds")
                .withRegion(region)
                .withAccountId(accountId)
                .withResource(ArnResource.builder()
                        .withResourceType("shard-group")
                        .withResource(dbShardGroupResourceId)
                        .build().toString())
                .build().toString();
    }

    protected DBShardGroup fetchDbShardGroup(final ProxyClient<RdsClient> client, final ResourceModel model) {
        try {
            final var response = client.injectCredentialsAndInvokeV2(Translator.describeDbShardGroupsRequest(model), client.client()::describeDBShardGroups);
            if (!response.hasDbShardGroups() || response.dbShardGroups().isEmpty()) {
                throw new CfnNotFoundException(ResourceModel.TYPE_NAME, model.getDBShardGroupIdentifier());
            }
            return response.dbShardGroups().get(0);
        } catch (DbShardGroupNotFoundException e) {
            throw new CfnNotFoundException(e);
        }
    }
}
