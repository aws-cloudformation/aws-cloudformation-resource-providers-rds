package software.amazon.rds.dbcluster;

import static software.amazon.rds.dbcluster.Translator.addRoleToDbClusterRequest;
import static software.amazon.rds.dbcluster.Translator.removeRoleFromDbClusterRequest;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.ObjectUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeSecurityGroupsResponse;
import software.amazon.awssdk.services.ec2.model.SecurityGroup;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.ClusterPendingModifiedValues;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBSubnetGroup;
import software.amazon.awssdk.services.rds.model.DbClusterAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DbClusterNotFoundException;
import software.amazon.awssdk.services.rds.model.DbClusterParameterGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.DbClusterQuotaExceededException;
import software.amazon.awssdk.services.rds.model.DbClusterRoleAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DbClusterRoleNotFoundException;
import software.amazon.awssdk.services.rds.model.DbClusterSnapshotNotFoundException;
import software.amazon.awssdk.services.rds.model.DbInstanceNotFoundException;
import software.amazon.awssdk.services.rds.model.DbSubnetGroupDoesNotCoverEnoughAZsException;
import software.amazon.awssdk.services.rds.model.DbSubnetGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbSubnetGroupsResponse;
import software.amazon.awssdk.services.rds.model.DescribeGlobalClustersResponse;
import software.amazon.awssdk.services.rds.model.DomainNotFoundException;
import software.amazon.awssdk.services.rds.model.Event;
import software.amazon.awssdk.services.rds.model.GlobalCluster;
import software.amazon.awssdk.services.rds.model.GlobalClusterNotFoundException;
import software.amazon.awssdk.services.rds.model.InsufficientDbInstanceCapacityException;
import software.amazon.awssdk.services.rds.model.InsufficientStorageClusterCapacityException;
import software.amazon.awssdk.services.rds.model.InvalidDbClusterSnapshotStateException;
import software.amazon.awssdk.services.rds.model.InvalidDbClusterStateException;
import software.amazon.awssdk.services.rds.model.InvalidDbInstanceStateException;
import software.amazon.awssdk.services.rds.model.InvalidDbSubnetGroupStateException;
import software.amazon.awssdk.services.rds.model.InvalidGlobalClusterStateException;
import software.amazon.awssdk.services.rds.model.InvalidSubnetException;
import software.amazon.awssdk.services.rds.model.InvalidVpcNetworkStateException;
import software.amazon.awssdk.services.rds.model.KmsKeyNotAccessibleException;
import software.amazon.awssdk.services.rds.model.NetworkTypeNotSupportedException;
import software.amazon.awssdk.services.rds.model.SnapshotQuotaExceededException;
import software.amazon.awssdk.services.rds.model.StorageQuotaExceededException;
import software.amazon.awssdk.services.rds.model.StorageTypeNotAvailableException;
import software.amazon.awssdk.services.rds.model.StorageTypeNotSupportedException;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.awssdk.services.rds.model.WriteForwardingStatus;
import software.amazon.awssdk.utils.StringUtils;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.cloudformation.exceptions.ResourceNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.cloudformation.resource.ResourceTypeSchema;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.error.ErrorRuleSet;
import software.amazon.rds.common.error.ErrorStatus;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.Events;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.logging.LoggingProxyClient;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.common.printer.FilteredJsonPrinter;
import software.amazon.rds.common.printer.JsonPrinter;
import software.amazon.rds.common.request.RequestValidationException;
import software.amazon.rds.common.request.ValidatedRequest;
import software.amazon.rds.common.request.Validations;

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {
    public static final String RESOURCE_IDENTIFIER = "dbcluster";
    public static final String ENGINE_AURORA_POSTGRESQL = "aurora-postgresql";
    private static final String MASTER_USER_SECRET_ACTIVE = "active";
    protected static final String DB_CLUSTER_REQUEST_STARTED_AT = "dbcluster-request-started-at";
    protected static final String DB_CLUSTER_REQUEST_IN_PROGRESS_AT = "dbcluster-request-in-progress-at";
    protected static final String DB_CLUSTER_STABILIZATION_TIME = "dbcluster-stabilization-time";
    public static final String STACK_NAME = "rds";
    protected static final int RESOURCE_ID_MAX_LENGTH = 63;

    protected static final String RESOURCE_UPDATED_AT = "resource-updated-at";

    private static final List<Predicate<Event>> EVENT_FAIL_CHECKERS = ImmutableList.of(
            (e) -> Events.isEventMessageContains(e, "Database cluster is in a state that cannot be upgraded:"),
            (e) -> Events.isEventMessageContains(e, "Cluster failover failed"),
            (e) -> Events.isEventMessageContains(e, "Cluster reboot failed"),
            (e) -> Events.isEventMessageContains(e, "Amazon RDS can't access the KMS encryption key"),
            (e) -> Events.isEventMessageContains(e, "Failed to join a host to a domain"),
            (e) -> Events.isEventMessageContains(e, "Failed to join cluster instance"),
            (e) -> Events.isEventMessageContains(e, "Amazon RDS isn't able to associate the IAM role"),
            (e) -> Events.isEventMessageContains(e, "could not be removed from global cluster"),
            (e) -> Events.isEventMessageContains(e, "Unable to upgrade DB cluster"),
            (e) -> Events.isEventMessageContains(e, "Unable to perform a major version upgrade"),
            (e) -> Events.isEventMessageContains(e, "Unable to patch the primary DB cluster"),
            (e) -> Events.isEventMessageContains(e, "We were unable to create your Aurora Serverless DB cluster")
    );

    protected static final ErrorRuleSet DEFAULT_DB_CLUSTER_ERROR_RULE_SET = ErrorRuleSet
            .extend(Commons.DEFAULT_ERROR_RULE_SET)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    ErrorCode.DBClusterAlreadyExistsFault)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    ErrorCode.DefaultVpcDoesNotExist)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
                    ErrorCode.StorageTypeNotAvailableFault,
                    ErrorCode.StorageTypeNotSupportedFault)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
                    ErrorCode.InvalidDBSecurityGroupState)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    DbClusterAlreadyExistsException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    DbClusterNotFoundException.class,
                    DbClusterSnapshotNotFoundException.class,
                    DbClusterParameterGroupNotFoundException.class,
                    DbInstanceNotFoundException.class,
                    DbSubnetGroupNotFoundException.class,
                    DomainNotFoundException.class,
                    GlobalClusterNotFoundException.class,
                    ResourceNotFoundException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ServiceLimitExceeded),
                    DbClusterQuotaExceededException.class,
                    InsufficientStorageClusterCapacityException.class,
                    InsufficientDbInstanceCapacityException.class,
                    StorageQuotaExceededException.class,
                    SnapshotQuotaExceededException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
                    DbClusterRoleAlreadyExistsException.class,
                    InvalidDbClusterStateException.class,
                    InvalidDbInstanceStateException.class,
                    InvalidDbSubnetGroupStateException.class,
                    InvalidGlobalClusterStateException.class,
                    InvalidSubnetException.class,
                    InvalidVpcNetworkStateException.class,
                    InvalidDbClusterSnapshotStateException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
                    DbSubnetGroupDoesNotCoverEnoughAZsException.class,
                    KmsKeyNotAccessibleException.class,
                    StorageTypeNotAvailableException.class,
                    StorageTypeNotSupportedException.class,
                    NetworkTypeNotSupportedException.class)
            .build();

    protected static final ErrorRuleSet ADD_ASSOC_ROLES_SOFTFAIL_ERROR_RULE_SET = ErrorRuleSet
            .extend(DEFAULT_DB_CLUSTER_ERROR_RULE_SET)
            .withErrorClasses(ErrorStatus.ignore(),
                    DbClusterRoleAlreadyExistsException.class)
            .build();

    protected static final ErrorRuleSet REMOVE_ASSOC_ROLES_SOFTFAIL_ERROR_RULE_SET = ErrorRuleSet
            .extend(DEFAULT_DB_CLUSTER_ERROR_RULE_SET)
            .withErrorClasses(ErrorStatus.ignore(),
                    DbClusterRoleNotFoundException.class)
            .build();

    protected static final ErrorRuleSet DISABLE_HTTP_ENDPOINT_V2_ERROR_RULE_SET = ErrorRuleSet
            .extend(DEFAULT_DB_CLUSTER_ERROR_RULE_SET)
            .withErrorCodes(ErrorStatus.ignore(OperationStatus.IN_PROGRESS),
                    ErrorCode.InvalidParameterCombination,
                    ErrorCode.InvalidParameterValue)
            .build();

    protected final static HandlerConfig DB_CLUSTER_HANDLER_CONFIG_36H = HandlerConfig.builder()
            .backoff(Constant.of().delay(Duration.ofSeconds(30)).timeout(Duration.ofHours(36)).build())
            .probingEnabled(true)
            .build();

    private final JsonPrinter PARAMETERS_FILTER = new FilteredJsonPrinter("MasterUsername", "MasterUserPassword");

    protected static final ResourceTypeSchema resourceTypeSchema = ResourceTypeSchema.load(new Configuration().resourceSchemaJsonObject());

    protected HandlerConfig config;
    protected RequestLogger requestLogger;

    public BaseHandlerStd(final HandlerConfig config) {
        super();
        this.config = config;
    }

    protected abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ValidatedRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProxyClient<Ec2Client> ec2ProxyClient
    );

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProxyClient<Ec2Client> ec2ProxyClient,
            final RequestLogger requestLogger
    ) {
        this.requestLogger = requestLogger;
        resourceStabilizationTime(callbackContext);
        try {
            validateRequest(request);
        } catch (RequestValidationException exception) {
            return ProgressEvent.defaultFailureHandler(exception, HandlerErrorCode.InvalidRequest);
        }

        return handleRequest(proxy, new ValidatedRequest<>(request), callbackContext, rdsProxyClient, ec2ProxyClient);
    }

    @Override
    public final ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger
    ) {
        return RequestLogger.handleRequest(
                logger,
                request,
                PARAMETERS_FILTER,
                requestLogger -> handleRequest(
                        proxy,
                        request,
                        callbackContext != null ? callbackContext : new CallbackContext(),
                        new LoggingProxyClient<>(requestLogger, proxy.newProxy(new RdsClientProvider()::getClient)),
                        new LoggingProxyClient<>(requestLogger, proxy.newProxy(new Ec2ClientProvider()::getClient)),
                        requestLogger
                ));
    }

    protected void validateRequest(final ResourceHandlerRequest<ResourceModel> request) throws RequestValidationException {
        Validations.validateSourceRegion(request.getDesiredResourceState().getSourceRegion());
    }

    protected boolean isFailureEvent(final Event event) {
        return EVENT_FAIL_CHECKERS.stream().anyMatch(p -> p.test(event));
    }

    protected DBCluster fetchDBCluster(
            final ProxyClient<RdsClient> proxyClient,
            final ResourceModel model
    ) {
        final DescribeDbClustersResponse response = proxyClient.injectCredentialsAndInvokeV2(
                Translator.describeDbClustersRequest(model),
                proxyClient.client()::describeDBClusters
        );
        return response.dbClusters().get(0);
    }

    protected GlobalCluster fetchGlobalCluster(
            final ProxyClient<RdsClient> proxyClient,
            final String globalClusterIdentifier
    ) {
        final DescribeGlobalClustersResponse globalClustersResponse = proxyClient.injectCredentialsAndInvokeV2(
                Translator.describeGlobalClustersRequest(globalClusterIdentifier),
                proxyClient.client()::describeGlobalClusters
        );
        return globalClustersResponse.globalClusters().get(0);
    }

    protected DBSubnetGroup fetchDBSubnetGroup(
            final ProxyClient<RdsClient> proxyClient,
            final String DbSubnetGroupName
    ) {
        final DescribeDbSubnetGroupsResponse response = proxyClient.injectCredentialsAndInvokeV2(
                Translator.describeDbSubnetGroup(DbSubnetGroupName),
                proxyClient.client()::describeDBSubnetGroups
        );
        return response.dbSubnetGroups().get(0);
    }

    protected SecurityGroup fetchSecurityGroup(
            final ProxyClient<Ec2Client> ec2ProxyClient,
            final String vpcId,
            final String groupName
    ) {
        final DescribeSecurityGroupsResponse response = ec2ProxyClient.injectCredentialsAndInvokeV2(
                Translator.describeSecurityGroupsRequest(vpcId, groupName),
                ec2ProxyClient.client()::describeSecurityGroups
        );
        if (CollectionUtils.isEmpty(response.securityGroups())) {
            return null;
        }
        return response.securityGroups().get(0);
    }

    protected boolean isGlobalClusterMember(final ResourceModel model) {
        return StringUtils.isNotBlank(model.getGlobalClusterIdentifier());
    }

    protected boolean isDBClusterAvailable(final DBCluster dbCluster) {
        return DBClusterStatus.Available.equalsString(dbCluster.status());
    }

    protected boolean isNoPendingChanges(final DBCluster dbCluster) {
        final ClusterPendingModifiedValues modifiedValues = dbCluster.pendingModifiedValues();
        return modifiedValues == null || (
                modifiedValues.masterUserPassword() == null &&
                        modifiedValues.iamDatabaseAuthenticationEnabled() == null &&
                        modifiedValues.engineVersion() == null &&
                        modifiedValues.storageType() == null &&
                        modifiedValues.allocatedStorage() == null &&
                        modifiedValues.iops() == null);
    }

    protected boolean isDBClusterStabilized(
            final ProxyClient<RdsClient> proxyClient,
            final ResourceModel model
    ) {
        final DBCluster dbCluster = fetchDBCluster(proxyClient, model);

        assertNoDBClusterTerminalStatus(dbCluster);

        final boolean isDBClusterStabilizedResult = isDBClusterAvailable(dbCluster);
        final boolean isNoPendingChangesResult = isNoPendingChanges(dbCluster);
        final boolean isMasterUserSecretStabilizedResult = isMasterUserSecretStabilized(dbCluster);
        final boolean isGlobalWriteForwardingStabilizedResult = isGlobalWriteForwardingStabilized(dbCluster);

        requestLogger.log(String.format("isDbClusterStabilized: %b", isDBClusterStabilizedResult),
                ImmutableMap.of("isDbClusterAvailable", isDBClusterStabilizedResult,
                        "isNoPendingChanges", isNoPendingChangesResult,
                        "isMasterUserSecretStabilized", isMasterUserSecretStabilizedResult,
                        "isGlobalWriteForwardingStabilized", isGlobalWriteForwardingStabilizedResult),
                ImmutableMap.of("Description", "isDBClusterStabilized method will be repeatedly" +
                        " called with a backoff mechanism after the modify call until it returns true. This" +
                        " process will continue until all included flags are true."));
        return isDBClusterStabilizedResult && isNoPendingChangesResult && isMasterUserSecretStabilizedResult && isGlobalWriteForwardingStabilizedResult;
    }

    private void resourceStabilizationTime(final CallbackContext context) {
        context.timestampOnce(DB_CLUSTER_REQUEST_STARTED_AT, Instant.now());
        context.timestamp(DB_CLUSTER_REQUEST_IN_PROGRESS_AT, Instant.now());
        context.calculateTimeDeltaInMinutes(DB_CLUSTER_STABILIZATION_TIME, context.getTimestamp(DB_CLUSTER_REQUEST_STARTED_AT), context.getTimestamp(DB_CLUSTER_REQUEST_IN_PROGRESS_AT));
    }

    protected static boolean isMasterUserSecretStabilized(DBCluster dbCluster) {
        if (dbCluster.masterUserSecret() == null ||
                CollectionUtils.isEmpty(dbCluster.dbClusterMembers())) {
            return true;
        }
        return MASTER_USER_SECRET_ACTIVE.equalsIgnoreCase(dbCluster.masterUserSecret().secretStatus());
    }

    protected static boolean isGlobalWriteForwardingStabilized(DBCluster dbCluster) {
        return BooleanUtils.isNotTrue(dbCluster.globalWriteForwardingRequested()) ||
                // Even if GWF is requested the WF will not start until a replica is created by customers
                (dbCluster.globalWriteForwardingStatus() != WriteForwardingStatus.ENABLING &&
                dbCluster.globalWriteForwardingStatus() != WriteForwardingStatus.DISABLING);
    }

    protected boolean isClusterRemovedFromGlobalCluster(
            final ProxyClient<RdsClient> proxyClient,
            final String previousGlobalClusterIdentifier,
            final ResourceModel model
    ) {
        try {
            final GlobalCluster globalCluster = fetchGlobalCluster(proxyClient, previousGlobalClusterIdentifier);
            final DBCluster dbCluster = fetchDBCluster(proxyClient, model);
            return globalCluster.globalClusterMembers().stream().noneMatch(m -> m.dbClusterArn().equals(dbCluster.dbClusterArn()));
        } catch (GlobalClusterNotFoundException ex) {
            return true;
        }
    }

    protected boolean isDBClusterDeleted(
            final ProxyClient<RdsClient> proxyClient,
            final ResourceModel model
    ) {
        try {
            fetchDBCluster(proxyClient, model);
        } catch (DbClusterNotFoundException e) {
            return true;
        }
        return false;
    }

    protected ProgressEvent<ResourceModel, CallbackContext> updateAssociatedRoles(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Collection<DBClusterRole> previousRoles,
            final Collection<DBClusterRole> desiredRoles,
            final boolean isRollback
    ) {
        final Set<DBClusterRole> rolesToRemove = new LinkedHashSet<>(Optional.ofNullable(previousRoles).orElse(Collections.emptyList()));
        final Set<DBClusterRole> rolesToAdd = new LinkedHashSet<>(Optional.ofNullable(desiredRoles).orElse(Collections.emptyList()));

        rolesToAdd.removeAll(Optional.ofNullable(previousRoles).orElse(Collections.emptyList()));
        rolesToRemove.removeAll(Optional.ofNullable(desiredRoles).orElse(Collections.emptyList()));

        return progress
                .then(p -> removeAssociatedRoles(proxy, rdsProxyClient, p, rolesToRemove, isRollback))
                .then(p -> addAssociatedRoles(proxy, rdsProxyClient, p, rolesToAdd, isRollback));
    }

    protected ProgressEvent<ResourceModel, CallbackContext> addAssociatedRoles(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Collection<DBClusterRole> roles,
            final boolean isRollback
    ) {
        final ResourceModel model = progress.getResourceModel();
        final CallbackContext callbackContext = progress.getCallbackContext();

        for (final DBClusterRole dbClusterRole : Optional.ofNullable(roles).orElse(Collections.emptyList())) {
            final ProgressEvent<ResourceModel, CallbackContext> progressEvent = proxy.initiate("rds::add-roles-to-dbcluster", proxyClient, model, callbackContext)
                    .translateToServiceRequest(modelRequest -> addRoleToDbClusterRequest(
                            modelRequest.getDBClusterIdentifier(),
                            dbClusterRole.getRoleArn(),
                            dbClusterRole.getFeatureName()
                    ))
                    .backoffDelay(config.getBackoff())
                    .makeServiceCall((modelRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                            modelRequest,
                            proxyInvocation.client()::addRoleToDBCluster
                    ))
                    .stabilize((addRoleRequest, addRoleResponse, client, resourceModel, context) ->
                            isAssociatedRoleAttached(client, resourceModel, dbClusterRole)
                    )
                    .handleError((addRoleRequest, exception, client, resourceModel, context) -> Commons.handleException(
                            ProgressEvent.progress(resourceModel, context),
                            exception,
                            isRollback ? ADD_ASSOC_ROLES_SOFTFAIL_ERROR_RULE_SET : DEFAULT_DB_CLUSTER_ERROR_RULE_SET,
                            requestLogger
                    ))
                    .success();
            if (!progressEvent.isSuccess()) {
                return progressEvent;
            }
        }
        return progress;
    }

    protected ProgressEvent<ResourceModel, CallbackContext> removeAssociatedRoles(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Collection<DBClusterRole> roles,
            final boolean isRollback
    ) {
        final ResourceModel model = progress.getResourceModel();
        final CallbackContext callbackContext = progress.getCallbackContext();

        for (final DBClusterRole dbClusterRole : Optional.ofNullable(roles).orElse(Collections.emptyList())) {
            final ProgressEvent<ResourceModel, CallbackContext> progressEvent = proxy.initiate("rds::remove-roles-to-dbcluster", proxyClient, model, callbackContext)
                    .translateToServiceRequest(modelRequest -> removeRoleFromDbClusterRequest(
                            modelRequest.getDBClusterIdentifier(),
                            dbClusterRole.getRoleArn(),
                            dbClusterRole.getFeatureName()
                    ))
                    .backoffDelay(config.getBackoff())
                    .makeServiceCall((modelRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                            modelRequest,
                            proxyInvocation.client()::removeRoleFromDBCluster
                    ))
                    .stabilize((removeRoleRequest, removeRoleResponse, client, resourceModel, context) ->
                            isAssociatedRoleDetached(client, resourceModel, dbClusterRole)
                    )
                    .handleError((removeRoleRequest, exception, proxyInvocation, resourceModel, context) -> Commons.handleException(
                            ProgressEvent.progress(resourceModel, context),
                            exception,
                            REMOVE_ASSOC_ROLES_SOFTFAIL_ERROR_RULE_SET,
                            requestLogger
                    ))
                    .success();
            if (!progressEvent.isSuccess()) {
                return progressEvent;
            }
        }
        return ProgressEvent.progress(model, callbackContext);
    }

    boolean isHttpEndpointV2Set(ProxyClient<RdsClient> proxyClient, ResourceModel model, Boolean expectedValue) {
        final DBCluster dbCluster = fetchDBCluster(proxyClient, model);
        return dbCluster.httpEndpointEnabled().equals(expectedValue);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> enableHttpEndpointV2(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        final ResourceModel model = progress.getResourceModel();
        final CallbackContext callbackContext = progress.getCallbackContext();
        final String dbClusterArn = fetchDBCluster(proxyClient, model).dbClusterArn();

        return proxy.initiate("rds::enable-http-endpoint-v2", proxyClient, model, callbackContext)
                    .translateToServiceRequest(modelRequest -> Translator.enableHttpEndpointRequest(dbClusterArn))
                    .backoffDelay(config.getBackoff())
                    .makeServiceCall((enableRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                            enableRequest,
                            proxyInvocation.client()::enableHttpEndpoint
                    ))
                    .stabilize((enableHttpRequest, enableHttpResponse, client, resourceModel, context) ->
                            isHttpEndpointV2Set(client, resourceModel, true)
                    )
                    .handleError((enableHttpRequest, exception, client, resourceModel, callbackCtxt) -> Commons.handleException(
                            ProgressEvent.progress(resourceModel, callbackCtxt),
                            exception,
                            DEFAULT_DB_CLUSTER_ERROR_RULE_SET,
                            requestLogger
                    ))
                    .progress();
    }

    protected ProgressEvent<ResourceModel, CallbackContext> disableHttpEndpointV2(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        final ResourceModel model = progress.getResourceModel();
        final CallbackContext callbackContext = progress.getCallbackContext();
        final String dbClusterArn = fetchDBCluster(proxyClient, model).dbClusterArn();

        return proxy.initiate("rds::disable-http-endpoint-v2", proxyClient, model, callbackContext)
                .translateToServiceRequest(modelRequest -> Translator.disableHttpEndpointRequest(dbClusterArn))
                .backoffDelay(config.getBackoff())
                .makeServiceCall((enableRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        enableRequest,
                        proxyInvocation.client()::disableHttpEndpoint
                ))
                .stabilize((disableHttpRequest, disableHttpResponse, client, resourceModel, context) ->
                        isHttpEndpointV2Set(client, resourceModel, false)
                )
                .handleError((disableHttpRequest, exception, client, resourceModel, callbackCtxt) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, callbackCtxt),
                        exception,
                        DISABLE_HTTP_ENDPOINT_V2_ERROR_RULE_SET,
                        requestLogger
                ))
                .progress();
    }

    protected boolean isAssociatedRoleAttached(
            final ProxyClient<RdsClient> proxyClient,
            final ResourceModel model,
            final DBClusterRole role
    ) {
        final DBCluster dbCluster = fetchDBCluster(proxyClient, model);
        return Optional.ofNullable(dbCluster.associatedRoles())
                .orElse(Collections.emptyList())
                .stream()
                .anyMatch(sdkRole -> isAssociatedRolesEqual(role, sdkRole));
    }

    protected boolean isAssociatedRoleDetached(
            final ProxyClient<RdsClient> proxyClient,
            final ResourceModel model,
            final DBClusterRole role
    ) {
        final DBCluster dbCluster = fetchDBCluster(proxyClient, model);
        return Optional.ofNullable(dbCluster.associatedRoles())
                .orElse(Collections.emptyList())
                .stream()
                .noneMatch(sdkRole -> isAssociatedRolesEqual(role, sdkRole));
    }

    protected boolean isAssociatedRolesEqual(
            final DBClusterRole role,
            final software.amazon.awssdk.services.rds.model.DBClusterRole sdkRole
    ) {
        if (role == null || sdkRole == null) {
            return role == null && sdkRole == null;
        }
        return Objects.equals(role.getRoleArn(), sdkRole.roleArn()) &&
                Objects.equals(StringUtils.trimToNull(role.getFeatureName()), StringUtils.trimToNull(sdkRole.featureName()));
    }

    protected ProgressEvent<ResourceModel, CallbackContext> updateTags(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
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

        DBCluster dbCluster;
        try {
            dbCluster = fetchDBCluster(rdsProxyClient, progress.getResourceModel());
        } catch (Exception exception) {
            return Commons.handleException(progress, exception, DEFAULT_DB_CLUSTER_ERROR_RULE_SET, requestLogger);
        }

        final String arn = dbCluster.dbClusterArn();

        try {
            Tagging.removeTags(rdsProxyClient, arn, Tagging.translateTagsToSdk(tagsToRemove));
            Tagging.addTags(rdsProxyClient, arn, Tagging.translateTagsToSdk(tagsToAdd));
        } catch (Exception exception) {
            return Commons.handleException(
                    progress,
                    exception,
                    DEFAULT_DB_CLUSTER_ERROR_RULE_SET.extendWith(Tagging.getUpdateTagsAccessDeniedRuleSet(rulesetTagsToAdd, rulesetTagsToRemove)),
                    requestLogger
            );
        }

        return progress;
    }

    protected ProgressEvent<ResourceModel, CallbackContext> removeFromGlobalCluster(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final String globalClusterIdentifier
    ) {
        final ResourceModel resourceModel = progress.getResourceModel();
        DBCluster cluster;
        try {
            cluster = fetchDBCluster(proxyClient, resourceModel);
        } catch (Exception exception) {
            return Commons.handleException(
                    ProgressEvent.progress(resourceModel, progress.getCallbackContext()),
                    exception,
                    DEFAULT_DB_CLUSTER_ERROR_RULE_SET,
                    requestLogger);
        }
        final String clusterArn = cluster.dbClusterArn();
        return proxy.initiate("rds::remove-from-global-cluster", proxyClient, resourceModel, progress.getCallbackContext())
                .translateToServiceRequest(model -> Translator.removeFromGlobalClusterRequest(globalClusterIdentifier, clusterArn))
                .backoffDelay(config.getBackoff())
                .makeServiceCall((removeRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        removeRequest,
                        proxyInvocation.client()::removeFromGlobalCluster
                ))
                .stabilize((removeRequest, removeResponse, proxyInvocation, model, context) ->
                        isDBClusterStabilized(proxyClient, resourceModel) &&
                                isClusterRemovedFromGlobalCluster(proxyClient, globalClusterIdentifier, resourceModel))
                .handleError((removeRequest, exception, client, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        DEFAULT_DB_CLUSTER_ERROR_RULE_SET,
                        requestLogger
                ))
                .progress();
    }

    protected ProgressEvent<ResourceModel, CallbackContext> setDefaultVpcSecurityGroupIds(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProxyClient<Ec2Client> ec2ProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        final ResourceModel resourceModel = progress.getResourceModel();
        SecurityGroup securityGroup;
        try {
            DBCluster cluster = fetchDBCluster(rdsProxyClient, resourceModel);
            DBSubnetGroup subnetGroup = fetchDBSubnetGroup(rdsProxyClient, cluster.dbSubnetGroup());
            securityGroup = fetchSecurityGroup(ec2ProxyClient, subnetGroup.vpcId(), "default");
            if (securityGroup != null) {
                resourceModel.setVpcSecurityGroupIds(Lists.newArrayList(securityGroup.groupId()));
            }
            return progress;
        } catch (Exception exception) {
            return Commons.handleException(
                    ProgressEvent.progress(resourceModel, progress.getCallbackContext()),
                    exception,
                    DEFAULT_DB_CLUSTER_ERROR_RULE_SET,
                    requestLogger);
        }
    }

    protected boolean shouldSetDefaultVpcSecurityGroupIds(final ResourceModel previousState,
                                                          final ResourceModel desiredState) {
        if (previousState != null) {
            final List<String> previousVpcIds = CollectionUtils.isEmpty(previousState.getVpcSecurityGroupIds()) ?
                    Collections.emptyList() : previousState.getVpcSecurityGroupIds();
            final List<String> desiredVpcIds = CollectionUtils.isEmpty(desiredState.getVpcSecurityGroupIds()) ?
                    Collections.emptyList() : desiredState.getVpcSecurityGroupIds();

            if (CollectionUtils.isEqualCollection(previousVpcIds, desiredVpcIds)) {
                return false;
            }
        }
        return CollectionUtils.isEmpty(desiredState.getVpcSecurityGroupIds());
    }

    protected boolean shouldUpdateHttpEndpointV2(final ResourceModel previousState, final ResourceModel desiredState) {
        return ENGINE_AURORA_POSTGRESQL.equalsIgnoreCase(desiredState.getEngine()) &&
                !EngineMode.Serverless.equals(EngineMode.fromString(desiredState.getEngineMode())) &&
                ObjectUtils.notEqual(previousState.getEnableHttpEndpoint(), desiredState.getEnableHttpEndpoint());
    }

    private void assertNoDBClusterTerminalStatus(final DBCluster dbCluster) throws CfnNotStabilizedException {
        final DBClusterStatus status = DBClusterStatus.fromString(dbCluster.status());
        if (status != null && status.isTerminal()) {
            throw new CfnNotStabilizedException(new Exception("DBCluster is in state: " + status));
        }
    }
}
