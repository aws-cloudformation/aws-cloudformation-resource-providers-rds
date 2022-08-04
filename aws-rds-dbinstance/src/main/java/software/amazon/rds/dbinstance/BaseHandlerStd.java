package software.amazon.rds.dbinstance;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.commons.lang3.BooleanUtils;

import com.amazonaws.util.CollectionUtils;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeSecurityGroupsResponse;
import software.amazon.awssdk.services.ec2.model.SecurityGroup;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DBSnapshot;
import software.amazon.awssdk.services.rds.model.DbInstanceAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DbInstanceAutomatedBackupQuotaExceededException;
import software.amazon.awssdk.services.rds.model.DbInstanceNotFoundException;
import software.amazon.awssdk.services.rds.model.DbInstanceRoleAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DbInstanceRoleNotFoundException;
import software.amazon.awssdk.services.rds.model.DbParameterGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.DbSecurityGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.DbSnapshotAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DbSnapshotNotFoundException;
import software.amazon.awssdk.services.rds.model.DbSubnetGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.DbUpgradeDependencyFailureException;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsResponse;
import software.amazon.awssdk.services.rds.model.InstanceQuotaExceededException;
import software.amazon.awssdk.services.rds.model.InsufficientDbInstanceCapacityException;
import software.amazon.awssdk.services.rds.model.InvalidDbClusterStateException;
import software.amazon.awssdk.services.rds.model.InvalidDbInstanceStateException;
import software.amazon.awssdk.services.rds.model.InvalidDbSecurityGroupStateException;
import software.amazon.awssdk.services.rds.model.InvalidDbSnapshotStateException;
import software.amazon.awssdk.services.rds.model.InvalidRestoreException;
import software.amazon.awssdk.services.rds.model.InvalidVpcNetworkStateException;
import software.amazon.awssdk.services.rds.model.KmsKeyNotAccessibleException;
import software.amazon.awssdk.services.rds.model.PendingModifiedValues;
import software.amazon.awssdk.services.rds.model.ProvisionedIopsNotAvailableInAzException;
import software.amazon.awssdk.services.rds.model.SnapshotQuotaExceededException;
import software.amazon.awssdk.services.rds.model.StorageQuotaExceededException;
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
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.HandlerMethod;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.logging.LoggingProxyClient;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.common.printer.FilteredJsonPrinter;
import software.amazon.rds.dbinstance.client.ApiVersion;
import software.amazon.rds.dbinstance.client.ApiVersionDispatcher;
import software.amazon.rds.dbinstance.client.Ec2ClientBuilder;
import software.amazon.rds.dbinstance.client.RdsClientBuilder;
import software.amazon.rds.dbinstance.client.VersionedProxyClient;

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {

    public static final String RESOURCE_IDENTIFIER = "dbinstance";
    public static final String STACK_NAME = "rds";

    public static final String API_VERSION_V12 = "2012-09-17";

    static final String DB_PARAMETER_GROUP_STATUS_APPLYING = "applying";
    static final String IN_SYNC_STATUS = "in-sync";
    static final String PENDING_REBOOT_STATUS = "pending-reboot";
    static final String READ_REPLICA_STATUS = "read replication";
    static final String READ_REPLICA_STATUS_REPLICATING = "replicating";
    static final String VPC_SECURITY_GROUP_STATUS_ACTIVE = "active";
    static final String DOMAIN_MEMBERSHIP_JOINED = "joined";
    static final String DOMAIN_MEMBERSHIP_KERBEROS_ENABLED = "kerberos-enabled";

    protected static final int RESOURCE_ID_MAX_LENGTH = 63;

    protected static final List<String> SQLSERVER_ENGINES_WITH_MIRRORING = Arrays.asList(
            "sqlserver-ee",
            "sqlserver-se"
    );

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
    protected static final String IS_ALREADY_BEING_DELETED_ERROR_MSG = "is already being deleted";

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

    protected static final ErrorRuleSet DEFAULT_DB_INSTANCE_ERROR_RULE_SET = ErrorRuleSet
            .extend(Commons.DEFAULT_ERROR_RULE_SET)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.ServiceLimitExceeded),
                    ErrorCode.InstanceQuotaExceeded,
                    ErrorCode.InsufficientDBInstanceCapacity,
                    ErrorCode.SnapshotQuotaExceeded,
                    ErrorCode.StorageQuotaExceeded)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
                    ErrorCode.InvalidParameterCombination,
                    ErrorCode.InvalidParameterValue,
                    ErrorCode.InvalidVPCNetworkStateFault,
                    ErrorCode.KMSKeyNotAccessibleFault,
                    ErrorCode.MissingParameter,
                    ErrorCode.ProvisionedIopsNotAvailableInAZFault)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    ErrorCode.DBParameterGroupNotFound,
                    ErrorCode.DBSecurityGroupNotFound,
                    ErrorCode.DBSnapshotNotFound,
                    ErrorCode.DBSubnetGroupNotFoundFault)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    DbInstanceNotFoundException.class,
                    DbParameterGroupNotFoundException.class,
                    DbSecurityGroupNotFoundException.class,
                    DbSnapshotNotFoundException.class,
                    DbSubnetGroupNotFoundException.class)
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
                    InvalidVpcNetworkStateException.class,
                    KmsKeyNotAccessibleException.class,
                    ProvisionedIopsNotAvailableInAzException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    DbInstanceAlreadyExistsException.class)
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
                    ErrorCode.InvalidDBInstanceState,
                    ErrorCode.InvalidParameterCombination)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    ErrorCode.DBInstanceNotFound)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
                    ErrorCode.InvalidDBSecurityGroupState)
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
        return message.contains(IS_ALREADY_BEING_DELETED_ERROR_MSG);
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

    protected abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext context,
            final VersionedProxyClient<RdsClient> rdsProxyClient,
            final VersionedProxyClient<Ec2Client> ec2ProxyClient,
            final Logger logger);

    @Override
    public final ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext context,
            final Logger logger) {
        return RequestLogger.handleRequest(
                logger,
                request,
                PARAMETERS_FILTER,
                requestLogger -> handleRequest(
                        proxy,
                        request,
                        context != null ? context : new CallbackContext(),
                        new VersionedProxyClient<RdsClient>()
                                .register(ApiVersion.V12, new LoggingProxyClient<>(requestLogger, proxy.newProxy(() -> new RdsClientBuilder().getClient(API_VERSION_V12))))
                                .register(ApiVersion.DEFAULT, new LoggingProxyClient<>(requestLogger, proxy.newProxy(new RdsClientBuilder()::getClient))),
                        new VersionedProxyClient<Ec2Client>()
                                .register(ApiVersion.DEFAULT, new LoggingProxyClient<>(requestLogger, proxy.newProxy(new Ec2ClientBuilder()::getClient))),
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
                .stabilize((modifyRequest, response, proxyInvocation, model, context) -> isDBInstanceStabilizedAfterMutate(proxyInvocation, model))
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
                .stabilize((modifyRequest, response, proxyInvocation, model, context) -> isDBInstanceStabilizedAfterMutate(proxyInvocation, model))
                .handleError((modifyRequest, exception, client, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        MODIFY_DB_INSTANCE_ERROR_RULE_SET
                ))
                .progress();
    }

    protected boolean isReadReplica(final ResourceModel model) {
        return StringUtils.isNotBlank(model.getSourceDBInstanceIdentifier());
    }

    protected boolean isRestoreFromSnapshot(final ResourceModel model) {
        return StringUtils.isNotBlank(model.getDBSnapshotIdentifier());
    }

    protected boolean isDBClusterMember(final ResourceModel model) {
        return StringUtils.isNotBlank(model.getDBClusterIdentifier());
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

    private void assertNoTerminalStatus(final DBInstance dbInstance) throws CfnNotStabilizedException {
        final DBInstanceStatus status = DBInstanceStatus.fromString(dbInstance.dbInstanceStatus());
        if (status != null && status.isTerminal()) {
            throw new CfnNotStabilizedException(new Exception("DB Instance is in state: " + status.toString()));
        }
    }

    protected boolean isDBInstanceStabilizedAfterMutate(
            final ProxyClient<RdsClient> rdsProxyClient,
            final ResourceModel model
    ) {
        final DBInstance dbInstance = fetchDBInstance(rdsProxyClient, model);

        assertNoTerminalStatus(dbInstance);

        return isDBInstanceAvailable(dbInstance) &&
                isReplicationComplete(dbInstance) &&
                isDBParameterGroupNotApplying(dbInstance) &&
                isNoPendingChanges(dbInstance) &&
                isVpcSecurityGroupsActive(dbInstance) &&
                isDomainMembershipsJoined(dbInstance) &&
                isPromotionTierUpdated(dbInstance, model);
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

    boolean isPromotionTierUpdated(final DBInstance dbInstance, final ResourceModel model) {
        return model.getPromotionTier() == null || model.getPromotionTier().equals(dbInstance.promotionTier());
    }

    boolean isDomainMembershipsJoined(final DBInstance dbInstance) {
        return Optional.ofNullable(dbInstance.domainMemberships()).orElse(Collections.emptyList())
                .stream()
                .allMatch(membership -> DOMAIN_MEMBERSHIP_JOINED.equals(membership.status()) ||
                        DOMAIN_MEMBERSHIP_KERBEROS_ENABLED.equals(membership.status()));
    }

    boolean isVpcSecurityGroupsActive(final DBInstance dbInstance) {
        return Optional.ofNullable(dbInstance.vpcSecurityGroups()).orElse(Collections.emptyList())
                .stream()
                .allMatch(group -> VPC_SECURITY_GROUP_STATUS_ACTIVE.equals(group.status()));
    }

    boolean isNoPendingChanges(final DBInstance dbInstance) {
        final PendingModifiedValues pending = dbInstance.pendingModifiedValues();
        return (pending == null) || (pending.dbInstanceClass() == null &&
                pending.allocatedStorage() == null &&
                pending.caCertificateIdentifier() == null &&
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

    boolean isDBParameterGroupNotApplying(final DBInstance dbInstance) {
        return Optional.ofNullable(dbInstance.dbParameterGroups()).orElse(Collections.emptyList())
                .stream()
                .noneMatch(group -> DB_PARAMETER_GROUP_STATUS_APPLYING.equals(group.parameterApplyStatus()));
    }

    boolean isReplicationComplete(final DBInstance dbInstance) {
        return Optional.ofNullable(dbInstance.statusInfos()).orElse(Collections.emptyList())
                .stream()
                .filter(statusInfo -> READ_REPLICA_STATUS.equals(statusInfo.statusType()))
                .allMatch(statusInfo -> READ_REPLICA_STATUS_REPLICATING.equals(statusInfo.status()));
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
                .allMatch(optionGroup -> IN_SYNC_STATUS.equals(optionGroup.status()));
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
                .allMatch(parameterGroup -> IN_SYNC_STATUS.equals(parameterGroup.parameterApplyStatus()));
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
                .anyMatch(member -> IN_SYNC_STATUS.equals(member.dbClusterParameterGroupStatus()));
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
            if (isRestoreFromSnapshot(model)) {
                try {
                    final DBInstance dbInstance = fetchDBInstance(rdsProxyClient, model);
                    model.setEngine(dbInstance.engine());
                } catch (Exception e) {
                    return Commons.handleException(progress, e, DEFAULT_DB_INSTANCE_ERROR_RULE_SET);
                }
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
