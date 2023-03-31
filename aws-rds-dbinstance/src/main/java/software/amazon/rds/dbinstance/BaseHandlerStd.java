package software.amazon.rds.dbinstance;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.BooleanUtils;

import com.amazonaws.util.CollectionUtils;
import com.google.common.collect.ImmutableList;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeSecurityGroupsResponse;
import software.amazon.awssdk.services.ec2.model.SecurityGroup;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.AuthorizationNotFoundException;
import software.amazon.awssdk.services.rds.model.CertificateNotFoundException;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DBSnapshot;
import software.amazon.awssdk.services.rds.model.DbClusterNotFoundException;
import software.amazon.awssdk.services.rds.model.DbClusterSnapshotNotFoundException;
import software.amazon.awssdk.services.rds.model.DbInstanceAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DbInstanceAutomatedBackupQuotaExceededException;
import software.amazon.awssdk.services.rds.model.DbInstanceNotFoundException;
import software.amazon.awssdk.services.rds.model.DbInstanceRoleAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DbInstanceRoleNotFoundException;
import software.amazon.awssdk.services.rds.model.DbParameterGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.DbSecurityGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.DbSnapshotAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DbSnapshotNotFoundException;
import software.amazon.awssdk.services.rds.model.DbSubnetGroupDoesNotCoverEnoughAZsException;
import software.amazon.awssdk.services.rds.model.DbSubnetGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.DbUpgradeDependencyFailureException;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsResponse;
import software.amazon.awssdk.services.rds.model.DomainNotFoundException;
import software.amazon.awssdk.services.rds.model.Event;
import software.amazon.awssdk.services.rds.model.InstanceQuotaExceededException;
import software.amazon.awssdk.services.rds.model.InsufficientDbInstanceCapacityException;
import software.amazon.awssdk.services.rds.model.InvalidDbClusterStateException;
import software.amazon.awssdk.services.rds.model.InvalidDbInstanceStateException;
import software.amazon.awssdk.services.rds.model.InvalidDbSecurityGroupStateException;
import software.amazon.awssdk.services.rds.model.InvalidDbSnapshotStateException;
import software.amazon.awssdk.services.rds.model.InvalidRestoreException;
import software.amazon.awssdk.services.rds.model.InvalidSubnetException;
import software.amazon.awssdk.services.rds.model.InvalidVpcNetworkStateException;
import software.amazon.awssdk.services.rds.model.KmsKeyNotAccessibleException;
import software.amazon.awssdk.services.rds.model.NetworkTypeNotSupportedException;
import software.amazon.awssdk.services.rds.model.OptionGroupMembership;
import software.amazon.awssdk.services.rds.model.OptionGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.PendingModifiedValues;
import software.amazon.awssdk.services.rds.model.ProvisionedIopsNotAvailableInAzException;
import software.amazon.awssdk.services.rds.model.SnapshotQuotaExceededException;
import software.amazon.awssdk.services.rds.model.StorageQuotaExceededException;
import software.amazon.awssdk.services.rds.model.StorageTypeNotSupportedException;
import software.amazon.awssdk.utils.StringUtils;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.error.ErrorRuleSet;
import software.amazon.rds.common.error.ErrorStatus;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.Events;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.HandlerMethod;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.logging.LoggingProxyClient;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.common.printer.FilteredJsonPrinter;
import software.amazon.rds.common.request.Validations;
import software.amazon.rds.dbinstance.client.ApiVersion;
import software.amazon.rds.dbinstance.client.ApiVersionDispatcher;
import software.amazon.rds.dbinstance.client.Ec2ClientProvider;
import software.amazon.rds.dbinstance.client.RdsClientProvider;
import software.amazon.rds.dbinstance.client.VersionedProxyClient;
import software.amazon.rds.common.request.RequestValidationException;
import software.amazon.rds.common.request.ValidatedRequest;
import software.amazon.rds.dbinstance.status.DBInstanceStatus;
import software.amazon.rds.dbinstance.status.DBParameterGroupStatus;
import software.amazon.rds.dbinstance.status.DomainMembershipStatus;
import software.amazon.rds.dbinstance.status.OptionGroupStatus;
import software.amazon.rds.dbinstance.status.ReadReplicaStatus;
import software.amazon.rds.dbinstance.status.VPCSecurityGroupStatus;

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {

    public static final String SECRET_STATUS_ACTIVE = "active";
    public static final String RESOURCE_IDENTIFIER = "dbinstance";
    public static final String STACK_NAME = "rds";

    public static final String API_VERSION_V12 = "2012-09-17";

    static final String READ_REPLICA_STATUS_TYPE = "read replication";

    protected static final List<String> RDS_CUSTOM_ORACLE_ENGINES = ImmutableList.of(
            "custom-oracle-ee",
            "custom-oracle-ee-cdb"
    );

    protected static final int RESOURCE_ID_MAX_LENGTH = 63;

    protected final static HandlerConfig DEFAULT_DB_INSTANCE_HANDLER_CONFIG = HandlerConfig.builder()
            .backoff(Constant.of().delay(Duration.ofSeconds(30)).timeout(Duration.ofMinutes(180)).build())
            .build();

    protected final static HandlerConfig DB_INSTANCE_HANDLER_CONFIG_36H = HandlerConfig.builder()
            .backoff(Constant.of().delay(Duration.ofSeconds(30)).timeout(Duration.ofHours(36)).build())
            .build();

    protected static final RuntimeException MISSING_METHOD_VERSION_EXCEPTION = new RuntimeException("Missing method version");

    // Note: looking up this error message fragment is the only way to distinguish between an already deleting
    // instance and any other invalid states (e.g. a stopped instance). It relies on a specific error text returned by
    // AWS RDS API. The message text is by no means guarded by any public contract. This error message can change
    // in the future with no prior notice by AWS RDS. A change in this error message would cause a CFN stack failure
    // upon a stack deletion: if an instance is being deleted out-of-bounds. This is a pretty corner (still common) case
    // where the CFN handler is trying to help the customer. A regular stack deletion will not be impacted.
    // Considered bounded-safe.
    protected static final String IS_ALREADY_BEING_DELETED_ERROR_FRAGMENT = "is already being deleted";

    protected static final String ILLEGAL_DELETION_POLICY_ERROR = "DeletionPolicy:Snapshot cannot be specified for a cluster instance, use deletion policy on the cluster instead.";

    protected static final String UNKNOWN_SOURCE_REGION_ERROR = "Unknown source region";

    protected static final String RESOURCE_UPDATED_AT = "resource-updated-at";

    protected final HandlerConfig config;

    private final ApiVersionDispatcher<ResourceModel, CallbackContext> apiVersionDispatcher;

    private final FilteredJsonPrinter PARAMETERS_FILTER = new FilteredJsonPrinter("MasterUsername", "MasterUserPassword", "TdeCredentialPassword");

    protected static final BiFunction<ResourceModel, ProxyClient<RdsClient>, ResourceModel> NOOP_CALL = (model, proxyClient) -> model;

    protected static final Function<Exception, ErrorStatus> ignoreDBInstanceBeingDeletedConditionalErrorStatus = exception -> {
        if (isDBInstanceBeingDeletedException(exception)) {
            return ErrorStatus.ignore(OperationStatus.IN_PROGRESS);
        }
        return ErrorStatus.failWith(HandlerErrorCode.ResourceConflict);
    };

    //TODO: This list should be gone eventually. Event ID should be checked instead.
    private static final List<Predicate<Event>> EVENT_FAIL_CHECKERS = ImmutableList.of(
            (e) -> Events.isEventMessageContains(e, "failed to join a host to a domain"),
            (e) -> Events.isEventMessageContains(e, "failed to join cluster instance"),
            (e) -> Events.isEventMessageContains(e, "insufficient instance capacity"),
            (e) -> Events.isEventMessageContains(e, "rds custom couldn't modify the db instance"),
            (e) -> Events.isEventMessageContains(e, "the db engine version upgrade failed"),
            (e) -> Events.isEventMessageContains(e, "the instance could not be upgraded"),
            (e) -> Events.isEventMessageContains(e, "the storage volume limitation was exceeded"),
            (e) -> Events.isEventMessageContains(e, "the update of the replica mode failed"),
            (e) -> Events.isEventMessageContains(e, "unable to modify database instance class"),
            (e) -> Events.isEventMessageContains(e, "unable to modify the db instance class"),
            (e) -> Events.isEventMessageContains(e, "you can't create the db instance"),
            (e) -> Events.isEventMessageContains(e, "instance is in a state that cannot be upgraded")
    );

    protected static final ErrorRuleSet DEFAULT_DB_INSTANCE_ERROR_RULE_SET = ErrorRuleSet
            .extend(Commons.DEFAULT_ERROR_RULE_SET)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.ServiceLimitExceeded),
                    ErrorCode.InstanceQuotaExceeded,
                    ErrorCode.InsufficientDBInstanceCapacity,
                    ErrorCode.SnapshotQuotaExceeded,
                    ErrorCode.StorageQuotaExceeded)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
                    ErrorCode.DBSubnetGroupNotAllowedFault,
                    ErrorCode.InvalidParameterCombination,
                    ErrorCode.InvalidParameterValue,
                    ErrorCode.InvalidVPCNetworkStateFault,
                    ErrorCode.KMSKeyNotAccessibleFault,
                    ErrorCode.MissingParameter,
                    ErrorCode.ProvisionedIopsNotAvailableInAZFault,
                    ErrorCode.StorageTypeNotSupportedFault)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    ErrorCode.DBClusterNotFoundFault,
                    ErrorCode.DBParameterGroupNotFound,
                    ErrorCode.DBSecurityGroupNotFound,
                    ErrorCode.DBSnapshotNotFound,
                    ErrorCode.DBSubnetGroupNotFoundFault)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    CertificateNotFoundException.class,
                    DbClusterNotFoundException.class,
                    DbInstanceNotFoundException.class,
                    DbParameterGroupNotFoundException.class,
                    DbSecurityGroupNotFoundException.class,
                    DbSnapshotNotFoundException.class,
                    DbClusterSnapshotNotFoundException.class,
                    DbSubnetGroupNotFoundException.class,
                    DomainNotFoundException.class,
                    OptionGroupNotFoundException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ServiceLimitExceeded),
                    DbInstanceAutomatedBackupQuotaExceededException.class,
                    InsufficientDbInstanceCapacityException.class,
                    InstanceQuotaExceededException.class,
                    SnapshotQuotaExceededException.class,
                    StorageQuotaExceededException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
                    InvalidDbInstanceStateException.class,
                    InvalidDbClusterStateException.class,
                    DbUpgradeDependencyFailureException.class,
                    InvalidDbSecurityGroupStateException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
                    AuthorizationNotFoundException.class,
                    DbSubnetGroupDoesNotCoverEnoughAZsException.class,
                    InvalidVpcNetworkStateException.class,
                    KmsKeyNotAccessibleException.class,
                    NetworkTypeNotSupportedException.class,
                    ProvisionedIopsNotAvailableInAzException.class,
                    StorageTypeNotSupportedException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    DbInstanceAlreadyExistsException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.GeneralServiceException),
                    InvalidSubnetException.class)
            .build();

    public static final ErrorRuleSet RESTORE_DB_INSTANCE_ERROR_RULE_SET = ErrorRuleSet
            .extend(DEFAULT_DB_INSTANCE_ERROR_RULE_SET)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    ErrorCode.DBInstanceAlreadyExists)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
                    ErrorCode.InvalidDBSnapshotState,
                    ErrorCode.InvalidRestoreFault)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    DbInstanceAlreadyExistsException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
                    InvalidDbSnapshotStateException.class,
                    InvalidRestoreException.class)
            .build();

    protected static final ErrorRuleSet CREATE_DB_INSTANCE_ERROR_RULE_SET = ErrorRuleSet
            .extend(DEFAULT_DB_INSTANCE_ERROR_RULE_SET)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    ErrorCode.DBInstanceAlreadyExists)
            .withErrorClasses(
                    ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    DbInstanceAlreadyExistsException.class)
            .build();

    protected static final ErrorRuleSet CREATE_DB_INSTANCE_READ_REPLICA_ERROR_RULE_SET = ErrorRuleSet
            .extend(DEFAULT_DB_INSTANCE_ERROR_RULE_SET)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    ErrorCode.DBInstanceAlreadyExists)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    DbInstanceAlreadyExistsException.class)
            .build();

    protected static final ErrorRuleSet REBOOT_DB_INSTANCE_ERROR_RULE_SET = ErrorRuleSet
            .extend(DEFAULT_DB_INSTANCE_ERROR_RULE_SET)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    ErrorCode.DBInstanceNotFound)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
                    ErrorCode.InvalidDBInstanceState)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    DbInstanceNotFoundException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
                    InvalidDbInstanceStateException.class)
            .build();

    protected static final ErrorRuleSet MODIFY_DB_INSTANCE_ERROR_RULE_SET = ErrorRuleSet
            .extend(DEFAULT_DB_INSTANCE_ERROR_RULE_SET)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
                    ErrorCode.InvalidDBInstanceState)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    ErrorCode.DBInstanceNotFound)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
                    ErrorCode.InvalidDBSecurityGroupState,
                    ErrorCode.InvalidParameterCombination)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
                    InvalidDbInstanceStateException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    DbInstanceNotFoundException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
                    InvalidDbSecurityGroupStateException.class)
            .build();

    protected static final ErrorRuleSet UPDATE_ASSOCIATED_ROLES_ERROR_RULE_SET = ErrorRuleSet
            .extend(DEFAULT_DB_INSTANCE_ERROR_RULE_SET)
            .withErrorClasses(ErrorStatus.ignore(),
                    DbInstanceRoleAlreadyExistsException.class,
                    DbInstanceRoleNotFoundException.class)
            .build();

    protected static final ErrorRuleSet DELETE_DB_INSTANCE_ERROR_RULE_SET = ErrorRuleSet
            .extend(DEFAULT_DB_INSTANCE_ERROR_RULE_SET)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
                    ErrorCode.InvalidParameterValue)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    ErrorCode.DBInstanceNotFound)
            .withErrorCodes(ErrorStatus.conditional(ignoreDBInstanceBeingDeletedConditionalErrorStatus),
                    ErrorCode.InvalidDBInstanceState)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
                    ErrorCode.DBSnapshotAlreadyExists)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    DbInstanceNotFoundException.class)
            .withErrorClasses(ErrorStatus.conditional(ignoreDBInstanceBeingDeletedConditionalErrorStatus),
                    InvalidDbInstanceStateException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
                    DbSnapshotAlreadyExistsException.class)
            .build();

    public BaseHandlerStd(final HandlerConfig config) {
        super();
        this.config = config;
        this.apiVersionDispatcher = new ApiVersionDispatcher<ResourceModel, CallbackContext>()
                .register(ApiVersion.V12, (m, c) -> !software.amazon.awssdk.utils.CollectionUtils.isNullOrEmpty(m.getDBSecurityGroups()));
    }

    private static boolean looksLikeDBInstanceBeingDeletedMessage(final String message) {
        if (StringUtils.isBlank(message)) {
            return false;
        }
        return message.contains(IS_ALREADY_BEING_DELETED_ERROR_FRAGMENT);
    }

    private static boolean isDBInstanceBeingDeletedException(final Exception e) {
        if (e instanceof InvalidDbInstanceStateException) {
            return looksLikeDBInstanceBeingDeletedMessage(e.getMessage());
        }
        return false;
    }

    protected ApiVersionDispatcher<ResourceModel, CallbackContext> getApiVersionDispatcher() {
        return apiVersionDispatcher;
    }

    protected void validateRequest(final ResourceHandlerRequest<ResourceModel> request) throws RequestValidationException {
        Validations.validateSourceRegion(request.getDesiredResourceState().getSourceRegion());
    }

    protected abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ValidatedRequest<ResourceModel> request,
            final CallbackContext context,
            final VersionedProxyClient<RdsClient> rdsProxyClient,
            final VersionedProxyClient<Ec2Client> ec2ProxyClient,
            final Logger logger
    );

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext context,
            final VersionedProxyClient<RdsClient> rdsProxyClient,
            final VersionedProxyClient<Ec2Client> ec2ProxyClient,
            final Logger logger
    ) {
        try {
            validateRequest(request);
        } catch (RequestValidationException exception) {
            return ProgressEvent.defaultFailureHandler(exception, HandlerErrorCode.InvalidRequest);
        }

        return handleRequest(proxy, new ValidatedRequest<ResourceModel>(request), context, rdsProxyClient, ec2ProxyClient, logger);
    }

    @Override
    public final ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext context,
            final Logger logger
    ) {
        return RequestLogger.handleRequest(
                logger,
                request,
                PARAMETERS_FILTER,
                requestLogger -> handleRequest(
                        proxy,
                        request,
                        context != null ? context : new CallbackContext(),
                        new VersionedProxyClient<RdsClient>()
                                .register(ApiVersion.V12, new LoggingProxyClient<>(requestLogger, proxy.newProxy(() -> new RdsClientProvider().getClientForApiVersion(API_VERSION_V12))))
                                .register(ApiVersion.DEFAULT, new LoggingProxyClient<>(requestLogger, proxy.newProxy(new RdsClientProvider()::getClient))),
                        new VersionedProxyClient<Ec2Client>()
                                .register(ApiVersion.DEFAULT, new LoggingProxyClient<>(requestLogger, proxy.newProxy(new Ec2ClientProvider()::getClient))),
                        logger
                ));
    }

    protected ProgressEvent<ResourceModel, CallbackContext> updateDbInstanceV12(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        return proxy.initiate("rds::modify-db-instance-v12", rdsProxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(resourceModel -> Translator.modifyDbInstanceRequestV12(
                        request.getPreviousResourceState(),
                        request.getDesiredResourceState(),
                        BooleanUtils.isTrue(request.getRollback()))
                )
                .backoffDelay(config.getBackoff())
                .makeServiceCall((modifyRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        modifyRequest,
                        proxyInvocation.client()::modifyDBInstance
                ))
                .stabilize((modifyRequest, response, proxyInvocation, model, context) -> isDBInstanceStabilizedAfterMutate(proxyInvocation, model, context))
                .handleError((modifyRequest, exception, client, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        MODIFY_DB_INSTANCE_ERROR_RULE_SET
                ))
                .progress();
    }

    protected ProgressEvent<ResourceModel, CallbackContext> updateDbInstance(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        return proxy.initiate("rds::modify-db-instance", rdsProxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(resourceModel -> Translator.modifyDbInstanceRequest(
                        request.getPreviousResourceState(),
                        request.getDesiredResourceState(),
                        BooleanUtils.isTrue(request.getRollback()))
                )
                .backoffDelay(config.getBackoff())
                .makeServiceCall((modifyRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        modifyRequest,
                        proxyInvocation.client()::modifyDBInstance
                ))
                .stabilize((modifyRequest, response, proxyInvocation, model, context) -> isDBInstanceStabilizedAfterMutate(proxyInvocation, model, context))
                .handleError((modifyRequest, exception, client, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        MODIFY_DB_INSTANCE_ERROR_RULE_SET
                ))
                .progress();
    }

    protected boolean isDBClusterMember(final ResourceModel model) {
        return StringUtils.isNotBlank(model.getDBClusterIdentifier());
    }

    protected boolean isRdsCustomOracleInstance(final ResourceModel model) {
        return RDS_CUSTOM_ORACLE_ENGINES.contains(model.getEngine());
    }

    protected boolean isFailureEvent(final Event event) {
        return EVENT_FAIL_CHECKERS.stream().anyMatch(p -> p.test(event));
    }

    protected DBInstance fetchDBInstance(
            final ProxyClient<RdsClient> rdsProxyClient,
            final ResourceModel model
    ) {
        final DescribeDbInstancesResponse response = rdsProxyClient.injectCredentialsAndInvokeV2(
                Translator.describeDbInstancesRequest(model),
                rdsProxyClient.client()::describeDBInstances
        );
        return response.dbInstances().get(0);
    }

    protected DBCluster fetchDBCluster(
            final ProxyClient<RdsClient> rdsProxyClient,
            final ResourceModel model
    ) {
        final DescribeDbClustersResponse response = rdsProxyClient.injectCredentialsAndInvokeV2(
                Translator.describeDbClustersRequest(model),
                rdsProxyClient.client()::describeDBClusters
        );
        return response.dbClusters().get(0);
    }

    protected DBSnapshot fetchDBSnapshot(
            final ProxyClient<RdsClient> rdsProxyClient,
            final ResourceModel model
    ) {
        final DescribeDbSnapshotsResponse response = rdsProxyClient.injectCredentialsAndInvokeV2(
                Translator.describeDbSnapshotsRequest(model),
                rdsProxyClient.client()::describeDBSnapshots
        );
        return response.dbSnapshots().get(0);
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
        return Optional.ofNullable(response.securityGroups())
                .orElse(Collections.emptyList())
                .stream()
                .findFirst()
                .orElse(null);
    }

    protected boolean isDbInstanceDeleted(
            final ProxyClient<RdsClient> rdsProxyClient,
            final ResourceModel model
    ) {
        DBInstance dbInstance;
        try {
            dbInstance = fetchDBInstance(rdsProxyClient, model);
        } catch (DbInstanceNotFoundException e) {
            // the instance is gone, exactly what we need
            return true;
        }

        assertNoTerminalStatus(dbInstance);

        return false;
    }

    private void assertNoDBInstanceTerminalStatus(final DBInstance dbInstance) throws CfnNotStabilizedException {
        final DBInstanceStatus status = DBInstanceStatus.fromString(dbInstance.dbInstanceStatus());
        if (status != null && status.isTerminal()) {
            throw new CfnNotStabilizedException(new Exception("DB Instance is in state: " + status.toString()));
        }
    }

    private void assertNoOptionGroupTerminalStatus(final DBInstance dbInstance) throws CfnNotStabilizedException {
        final List<OptionGroupMembership> termOptionGroups = Optional.ofNullable(dbInstance.optionGroupMemberships()).orElse(Collections.emptyList())
                .stream()
                .filter(optionGroup -> {
                    final OptionGroupStatus status = OptionGroupStatus.fromString(optionGroup.status());
                    return status != null && status.isTerminal();
                })
                .collect(Collectors.toList());

        if (!termOptionGroups.isEmpty()) {
            throw new CfnNotStabilizedException(new Exception(
                    String.format("OptionGroup %s is in a terminal state",
                            termOptionGroups.get(0).optionGroupName())));
        }
    }

    private void assertNoTerminalStatus(final DBInstance dbInstance) throws CfnNotStabilizedException {
        assertNoDBInstanceTerminalStatus(dbInstance);
        assertNoOptionGroupTerminalStatus(dbInstance);
    }

    protected boolean isDBInstanceStabilizedAfterMutate(
            final ProxyClient<RdsClient> rdsProxyClient,
            final ResourceModel model,
            final CallbackContext context
    ) {
        final DBInstance dbInstance = fetchDBInstance(rdsProxyClient, model);

        assertNoTerminalStatus(dbInstance);

        return isDBInstanceAvailable(dbInstance) &&
                isReplicationComplete(dbInstance) &&
                isDBParameterGroupNotApplying(dbInstance) &&
                isNoPendingChanges(dbInstance) &&
                isCaCertificateChangesApplied(dbInstance, model) &&
                isVpcSecurityGroupsActive(dbInstance) &&
                isDomainMembershipsJoined(dbInstance) &&
                isMasterUserSecretStabilized(dbInstance);
    }

    protected boolean isDBInstanceStabilizedAfterReboot(
            final ProxyClient<RdsClient> rdsProxyClient,
            final ResourceModel model
    ) {
        final DBInstance dbInstance = fetchDBInstance(rdsProxyClient, model);

        assertNoTerminalStatus(dbInstance);

        return isDBInstanceAvailable(dbInstance) &&
                isDBParameterGroupInSync(dbInstance) &&
                isOptionGroupInSync(dbInstance) &&
                (!isDBClusterMember(model) || isDBClusterParameterGroupStabilized(rdsProxyClient, model));
    }

    boolean isDBInstanceAvailable(final DBInstance dbInstance) {
        return DBInstanceStatus.Available.equalsString(dbInstance.dbInstanceStatus());
    }

    boolean isDomainMembershipsJoined(final DBInstance dbInstance) {
        return Optional.ofNullable(dbInstance.domainMemberships()).orElse(Collections.emptyList())
                .stream()
                .allMatch(membership -> DomainMembershipStatus.Joined.equalsString(membership.status()) ||
                        DomainMembershipStatus.KerberosEnabled.equalsString(membership.status()));
    }

    boolean isVpcSecurityGroupsActive(final DBInstance dbInstance) {
        return Optional.ofNullable(dbInstance.vpcSecurityGroups()).orElse(Collections.emptyList())
                .stream()
                .allMatch(group -> VPCSecurityGroupStatus.Active.equalsString(group.status()));
    }

    boolean isNoPendingChanges(final DBInstance dbInstance) {
        final PendingModifiedValues pending = dbInstance.pendingModifiedValues();
        return (pending == null) || (pending.dbInstanceClass() == null &&
                pending.allocatedStorage() == null &&
                pending.masterUserPassword() == null &&
                pending.port() == null &&
                pending.backupRetentionPeriod() == null &&
                pending.multiAZ() == null &&
                pending.engineVersion() == null &&
                pending.iops() == null &&
                pending.dbInstanceIdentifier() == null &&
                pending.licenseModel() == null &&
                pending.storageType() == null &&
                pending.dbSubnetGroupName() == null &&
                pending.pendingCloudwatchLogsExports() == null &&
                CollectionUtils.isNullOrEmpty(pending.processorFeatures()) &&
                pending.iamDatabaseAuthenticationEnabled() == null &&
                pending.automationMode() == null &&
                pending.resumeFullAutomationModeTime() == null
        );
    }

    boolean isCaCertificateChangesApplied(final DBInstance dbInstance, final ResourceModel model) {
        final PendingModifiedValues pending = dbInstance.pendingModifiedValues();
        return pending == null ||
                pending.caCertificateIdentifier() == null ||
                BooleanUtils.isNotTrue(model.getCertificateRotationRestart());
    }

    boolean isDBParameterGroupNotApplying(final DBInstance dbInstance) {
        return Optional.ofNullable(dbInstance.dbParameterGroups()).orElse(Collections.emptyList())
                .stream()
                .noneMatch(group -> DBParameterGroupStatus.Applying.equalsString(group.parameterApplyStatus()));
    }

    boolean isReplicationComplete(final DBInstance dbInstance) {
        return Optional.ofNullable(dbInstance.statusInfos()).orElse(Collections.emptyList())
                .stream()
                .filter(statusInfo -> READ_REPLICA_STATUS_TYPE.equals(statusInfo.statusType()))
                .allMatch(statusInfo -> ReadReplicaStatus.Replicating.equalsString(statusInfo.status()));
    }

    protected boolean isOptionGroupStabilized(
            final ProxyClient<RdsClient> rdsProxyClient,
            final ResourceModel model
    ) {
        return isOptionGroupInSync(fetchDBInstance(rdsProxyClient, model));
    }

    protected boolean isOptionGroupInSync(final DBInstance dbInstance) {
        return Optional.ofNullable(dbInstance.optionGroupMemberships()).orElse(Collections.emptyList())
                .stream()
                .allMatch(optionGroup -> OptionGroupStatus.InSync.equalsString(optionGroup.status()));
    }

    protected boolean isDBParameterGroupStabilized(
            final ProxyClient<RdsClient> rdsProxyClient,
            final ResourceModel model
    ) {
        return isDBParameterGroupInSync(fetchDBInstance(rdsProxyClient, model));
    }

    protected boolean isDBParameterGroupInSync(final DBInstance dbInstance) {
        return Optional.ofNullable(dbInstance.dbParameterGroups()).orElse(Collections.emptyList())
                .stream()
                .allMatch(parameterGroup -> DBParameterGroupStatus.InSync.equalsString(parameterGroup.parameterApplyStatus()));
    }

    protected boolean isDBClusterParameterGroupStabilized(
            final ProxyClient<RdsClient> rdsProxyClient,
            final ResourceModel model
    ) {
        return isDBClusterParameterGroupInSync(model, fetchDBCluster(rdsProxyClient, model));
    }

    protected boolean isDBClusterParameterGroupInSync(final ResourceModel model, final DBCluster dbCluster) {
        return Optional.ofNullable(dbCluster.dbClusterMembers()).orElse(Collections.emptyList())
                .stream()
                .filter(member -> model.getDBInstanceIdentifier().equalsIgnoreCase(member.dbInstanceIdentifier()))
                .anyMatch(member -> DBParameterGroupStatus.InSync.equalsString(member.dbClusterParameterGroupStatus()));
    }

    protected boolean isDBInstanceRoleStabilized(
            final ProxyClient<RdsClient> rdsProxyClient,
            final ResourceModel model,
            final Function<Stream<software.amazon.awssdk.services.rds.model.DBInstanceRole>, Boolean> predicate
    ) {
        final DBInstance dbInstance = fetchDBInstance(rdsProxyClient, model);
        return predicate.apply(Optional.ofNullable(
                dbInstance.associatedRoles()
        ).orElse(Collections.emptyList()).stream());
    }

    protected boolean isDBInstanceRoleAdditionStabilized(
            final ProxyClient<RdsClient> rdsProxyClient,
            final ResourceModel model,
            final DBInstanceRole lookupRole
    ) {
        return isDBInstanceRoleStabilized(
                rdsProxyClient,
                model,
                (roles) -> roles.anyMatch(role -> role.roleArn().equals(lookupRole.getRoleArn()) &&
                        (role.featureName() == null || role.featureName().equals(lookupRole.getFeatureName())))
        );
    }

    protected boolean isDBInstanceRoleRemovalStabilized(
            final ProxyClient<RdsClient> rdsProxyClient,
            final ResourceModel model,
            final DBInstanceRole lookupRole
    ) {
        return isDBInstanceRoleStabilized(
                rdsProxyClient,
                model,
                (roles) -> roles.noneMatch(role -> role.roleArn().equals(lookupRole.getRoleArn()))
        );
    }

    protected boolean isMasterUserSecretStabilized(final DBInstance instance) {
        if (instance.masterUserSecret() == null) {
            return true;
        }
        return SECRET_STATUS_ACTIVE.equalsIgnoreCase(instance.masterUserSecret().secretStatus());
    }

    protected ProgressEvent<ResourceModel, CallbackContext> updateAssociatedRoles(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            ProgressEvent<ResourceModel, CallbackContext> progress,
            Collection<DBInstanceRole> previousRoles,
            Collection<DBInstanceRole> desiredRoles
    ) {
        final Set<DBInstanceRole> rolesToRemove = new LinkedHashSet<>(Optional.ofNullable(previousRoles).orElse(Collections.emptyList()));
        final Set<DBInstanceRole> rolesToAdd = new LinkedHashSet<>(Optional.ofNullable(desiredRoles).orElse(Collections.emptyList()));

        rolesToAdd.removeAll(Optional.ofNullable(previousRoles).orElse(Collections.emptyList()));
        rolesToRemove.removeAll(Optional.ofNullable(desiredRoles).orElse(Collections.emptyList()));

        return progress
                .then(p -> removeOldRoles(proxy, rdsProxyClient, p, rolesToRemove))
                .then(p -> addNewRoles(proxy, rdsProxyClient, p, rolesToAdd));
    }

    protected ProgressEvent<ResourceModel, CallbackContext> addNewRoles(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Collection<DBInstanceRole> rolesToAdd
    ) {
        for (final DBInstanceRole role : rolesToAdd) {
            final ProgressEvent<ResourceModel, CallbackContext> progressEvent = proxy.initiate("rds::add-roles-to-db-instance", rdsProxyClient, progress.getResourceModel(), progress.getCallbackContext())
                    .translateToServiceRequest(addRequest -> Translator.addRoleToDbInstanceRequest(progress.getResourceModel(), role))
                    .backoffDelay(config.getBackoff())
                    .makeServiceCall((request, proxyInvocation) -> {
                        return proxyInvocation.injectCredentialsAndInvokeV2(request, proxyInvocation.client()::addRoleToDBInstance);
                    })
                    .stabilize((request, response, proxyInvocation, modelRequest, callbackContext) -> isDBInstanceRoleAdditionStabilized(
                            proxyInvocation, modelRequest, role
                    ))
                    .handleError((request, exception, proxyInvocation, resourceModel, context) -> Commons.handleException(
                            ProgressEvent.progress(resourceModel, context),
                            exception,
                            UPDATE_ASSOCIATED_ROLES_ERROR_RULE_SET
                    ))
                    .success();
            if (!progressEvent.isSuccess()) {
                return progressEvent;
            }
        }
        return ProgressEvent.progress(progress.getResourceModel(), progress.getCallbackContext());
    }

    protected ProgressEvent<ResourceModel, CallbackContext> removeOldRoles(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Collection<DBInstanceRole> rolesToRemove
    ) {
        for (final DBInstanceRole role : rolesToRemove) {
            final ProgressEvent<ResourceModel, CallbackContext> progressEvent = proxy.initiate("rds::remove-roles-from-db-instance", rdsProxyClient, progress.getResourceModel(), progress.getCallbackContext())
                    .translateToServiceRequest(removeRequest -> Translator.removeRoleFromDbInstanceRequest(
                            progress.getResourceModel(), role
                    ))
                    .backoffDelay(config.getBackoff())
                    .makeServiceCall((request, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                            request, proxyInvocation.client()::removeRoleFromDBInstance
                    ))
                    .stabilize((request, response, proxyInvocation, modelRequest, callbackContext) -> isDBInstanceRoleRemovalStabilized(
                            proxyInvocation, modelRequest, role
                    ))
                    .handleError((request, exception, proxyInvocation, resourceModel, context) -> Commons.handleException(
                            ProgressEvent.progress(resourceModel, context),
                            exception,
                            UPDATE_ASSOCIATED_ROLES_ERROR_RULE_SET
                    ))
                    .success();
            if (!progressEvent.isSuccess()) {
                return progressEvent;
            }
        }
        return ProgressEvent.progress(progress.getResourceModel(), progress.getCallbackContext());
    }

    protected ProgressEvent<ResourceModel, CallbackContext> reboot(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        return proxy.initiate(
                        "rds::reboot-db-instance",
                        rdsProxyClient,
                        progress.getResourceModel(),
                        progress.getCallbackContext()
                ).translateToServiceRequest(Translator::rebootDbInstanceRequest)
                .backoffDelay(config.getBackoff())
                .makeServiceCall((rebootRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        rebootRequest,
                        proxyInvocation.client()::rebootDBInstance
                ))
                .handleError((request, exception, client, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        REBOOT_DB_INSTANCE_ERROR_RULE_SET
                ))
                .progress();
    }

    protected ProgressEvent<ResourceModel, CallbackContext> rebootAwait(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        return reboot(proxy, rdsProxyClient, progress).then(p -> stabilizeDBInstanceAfterReboot(proxy, rdsProxyClient, p));
    }

    protected ProgressEvent<ResourceModel, CallbackContext> stabilizeDBInstanceAfterReboot(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        return proxy.initiate(
                        "rds::stabilize-db-instance-after-reboot-" + getClass().getSimpleName(),
                        rdsProxyClient,
                        progress.getResourceModel(),
                        progress.getCallbackContext()
                )
                .translateToServiceRequest(Function.identity())
                .backoffDelay(config.getBackoff())
                .makeServiceCall(NOOP_CALL)
                .stabilize((request, response, proxyInvocation, model, context) -> isDBInstanceStabilizedAfterReboot(proxyInvocation, model))
                .handleError((request, exception, proxyInvocation, resourceModel, context) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, context),
                        exception,
                        UPDATE_ASSOCIATED_ROLES_ERROR_RULE_SET
                ))
                .progress();
    }

    protected ProgressEvent<ResourceModel, CallbackContext> ensureEngineSet(
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        final ResourceModel model = progress.getResourceModel();
        if (StringUtils.isEmpty(model.getEngine())) {
            try {
                final DBInstance dbInstance = fetchDBInstance(rdsProxyClient, model);
                model.setEngine(dbInstance.engine());
            } catch (Exception e) {
                return Commons.handleException(progress, e, DEFAULT_DB_INSTANCE_ERROR_RULE_SET);
            }
        }
        return progress;
    }

    protected ProgressEvent<ResourceModel, CallbackContext> updateTags(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet previousTags,
            final Tagging.TagSet desiredTags
    ) {
        final Tagging.TagSet tagsToAdd = Tagging.exclude(desiredTags, previousTags);
        final Tagging.TagSet tagsToRemove = Tagging.exclude(previousTags, desiredTags);

        if (tagsToAdd.isEmpty() && tagsToRemove.isEmpty()) {
            return progress;
        }

        DBInstance dbInstance;
        try {
            dbInstance = fetchDBInstance(rdsProxyClient, progress.getResourceModel());
        } catch (Exception exception) {
            return Commons.handleException(progress, exception, DEFAULT_DB_INSTANCE_ERROR_RULE_SET);
        }

        final String arn = dbInstance.dbInstanceArn();

        try {
            Tagging.removeTags(rdsProxyClient, arn, Tagging.translateTagsToSdk(tagsToRemove));
            Tagging.addTags(rdsProxyClient, arn, Tagging.translateTagsToSdk(tagsToAdd));
        } catch (Exception exception) {
            return Commons.handleException(
                    progress,
                    exception,
                    DEFAULT_DB_INSTANCE_ERROR_RULE_SET.extendWith(Tagging.bestEffortErrorRuleSet(tagsToAdd, tagsToRemove))
            );
        }

        return progress;
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
            throw MISSING_METHOD_VERSION_EXCEPTION;
        }
        return methodVersions.get(apiVersion).invoke(proxy, rdsProxyClient.forVersion(apiVersion), progress, allTags);
    }
}
