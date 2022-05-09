package software.amazon.rds.dbinstance;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.amazonaws.util.CollectionUtils;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeSecurityGroupsResponse;
import software.amazon.awssdk.services.ec2.model.SecurityGroup;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBClusterMember;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DBParameterGroupStatus;
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
import software.amazon.awssdk.services.rds.model.OptionGroupMembership;
import software.amazon.awssdk.services.rds.model.ProvisionedIopsNotAvailableInAzException;
import software.amazon.awssdk.services.rds.model.SnapshotQuotaExceededException;
import software.amazon.awssdk.services.rds.model.StorageQuotaExceededException;
import software.amazon.awssdk.utils.StringUtils;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.error.ErrorRuleSet;
import software.amazon.rds.common.error.ErrorStatus;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.logging.LoggingProxyClient;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.common.printer.FilteredJsonPrinter;

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {

    public static final String RESOURCE_IDENTIFIER = "dbinstance";
    public static final String STACK_NAME = "rds";

    public static final String API_VERSION_V12 = "2012-09-17";

    public static final String PENDING_REBOOT_STATUS = "pending-reboot";
    public static final String IN_SYNC_STATUS = "in-sync";

    protected static final int RESOURCE_ID_MAX_LENGTH = 63;

    protected static final List<String> SQLSERVER_ENGINES_WITH_MIRRORING = Arrays.asList(
            "sqlserver-ee",
            "sqlserver-se"
    );

    protected static final RuntimeException REQUEST_VERSION_ROUTE_MISSING = new RuntimeException("Failed to dispatch request to a particular API version");

    protected static final BiFunction<ResourceModel, ProxyClient<RdsClient>, ResourceModel> NOOP_CALL = (model, proxyClient) -> model;

    protected static final ErrorRuleSet DEFAULT_DB_INSTANCE_ERROR_RULE_SET = ErrorRuleSet.builder()
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
            .build()
            .orElse(Commons.DEFAULT_ERROR_RULE_SET);

    public static final ErrorRuleSet RESTORE_DB_INSTANCE_ERROR_RULE_SET = ErrorRuleSet.builder()
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
            .build()
            .orElse(DEFAULT_DB_INSTANCE_ERROR_RULE_SET);

    protected static final ErrorRuleSet CREATE_DB_INSTANCE_ERROR_RULE_SET = ErrorRuleSet.builder()
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    ErrorCode.DBInstanceAlreadyExists)
            .withErrorClasses(
                    ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    DbInstanceAlreadyExistsException.class)
            .build()
            .orElse(DEFAULT_DB_INSTANCE_ERROR_RULE_SET);

    protected static final ErrorRuleSet CREATE_DB_INSTANCE_READ_REPLICA_ERROR_RULE_SET = ErrorRuleSet.builder()
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    ErrorCode.DBInstanceAlreadyExists)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    DbInstanceAlreadyExistsException.class)
            .build()
            .orElse(DEFAULT_DB_INSTANCE_ERROR_RULE_SET);

    protected static final ErrorRuleSet REBOOT_DB_INSTANCE_ERROR_RULE_SET = ErrorRuleSet.builder()
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    ErrorCode.DBInstanceNotFound)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
                    ErrorCode.InvalidDBInstanceState)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    DbInstanceNotFoundException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
                    InvalidDbInstanceStateException.class)
            .build()
            .orElse(DEFAULT_DB_INSTANCE_ERROR_RULE_SET);

    protected static final ErrorRuleSet MODIFY_DB_INSTANCE_ERROR_RULE_SET = ErrorRuleSet.builder()
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
            .build()
            .orElse(DEFAULT_DB_INSTANCE_ERROR_RULE_SET);

    protected static final ErrorRuleSet UPDATE_ASSOCIATED_ROLES_ERROR_RULE_SET = ErrorRuleSet.builder()
            .withErrorClasses(ErrorStatus.ignore(),
                    DbInstanceRoleAlreadyExistsException.class,
                    DbInstanceRoleNotFoundException.class)
            .build()
            .orElse(DEFAULT_DB_INSTANCE_ERROR_RULE_SET);

    protected static final ErrorRuleSet DELETE_DB_INSTANCE_ERROR_RULE_SET = ErrorRuleSet.builder()
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
                    ErrorCode.InvalidParameterValue)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    ErrorCode.DBInstanceNotFound)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
                    ErrorCode.InvalidDBInstanceState)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
                    ErrorCode.DBSnapshotAlreadyExists)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    DbInstanceNotFoundException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
                    InvalidDbInstanceStateException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
                    DbSnapshotAlreadyExistsException.class)
            .build()
            .orElse(DEFAULT_DB_INSTANCE_ERROR_RULE_SET);

    protected final static HandlerConfig DEFAULT_DB_INSTANCE_HANDLER_CONFIG = HandlerConfig.builder()
            .probingEnabled(true)
            .backoff(Constant.of().delay(Duration.ofSeconds(30)).timeout(Duration.ofMinutes(180)).build())
            .build();

    private final FilteredJsonPrinter PARAMETERS_FILTER = new FilteredJsonPrinter("MasterUsername", "MasterUserPassword", "TdeCredentialPassword");

    protected HandlerConfig config;

    public BaseHandlerStd(final HandlerConfig config) {
        super();
        this.config = config;
    }

    protected abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext context,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProxyClient<Ec2Client> ec2ProxyClient,
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
                        new LoggingProxyClient<>(requestLogger, proxy.newProxy(this::buildRdsClient)),
                        new LoggingProxyClient<>(requestLogger, proxy.newProxy(this::buildEc2Client)),
                        logger
                ));
    }

    protected ProgressEvent<ResourceModel, CallbackContext> awaitDBInstanceAvailableStatus(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        return proxy.initiate(
                        "rds::stabilize-db-instance-" + getClass().getSimpleName(),
                        rdsProxyClient,
                        progress.getResourceModel(),
                        progress.getCallbackContext()
                )
                .translateToServiceRequest(Function.identity())
                .backoffDelay(config.getBackoff())
                .makeServiceCall(NOOP_CALL)
                .stabilize((request, response, proxyInvocation, model, context) -> isDbInstanceStabilized(proxyInvocation, model))
                .handleError((request, exception, proxyInvocation, resourceModel, context) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, context),
                        exception,
                        UPDATE_ASSOCIATED_ROLES_ERROR_RULE_SET
                ))
                .progress();
    }

    protected boolean withProbing(
            final CallbackContext context,
            final String probeName,
            final int nProbes,
            final Supplier<Boolean> checker
    ) {
        final boolean check = checker.get();
        if (!config.isProbingEnabled()) {
            return check;
        }
        if (!check) {
            context.flushProbes(probeName);
            return false;
        }
        context.incProbes(probeName);
        if (context.getProbes(probeName) >= nProbes) {
            context.flushProbes(probeName);
            return true;
        }
        return false;
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
                TranslatorV19.describeDbInstancesRequest(model),
                rdsProxyClient.client()::describeDBInstances
        );
        return response.dbInstances().stream().findFirst().get();
    }

    protected DBCluster fetchDBCluster(
            final ProxyClient<RdsClient> rdsProxyClient,
            final ResourceModel model
    ) {
        final DescribeDbClustersResponse response = rdsProxyClient.injectCredentialsAndInvokeV2(
                TranslatorV19.describeDbClustersRequest(model),
                rdsProxyClient.client()::describeDBClusters
        );
        return response.dbClusters().stream().findFirst().get();
    }

    protected DBSnapshot fetchDBSnapshot(
            final ProxyClient<RdsClient> rdsProxyClient,
            final ResourceModel model
    ) {
        final DescribeDbSnapshotsResponse response = rdsProxyClient.injectCredentialsAndInvokeV2(
                TranslatorV19.describeDbSnapshotsRequest(model),
                rdsProxyClient.client()::describeDBSnapshots
        );
        return response.dbSnapshots().stream().findFirst().get();
    }

    protected SecurityGroup fetchSecurityGroup(
            final ProxyClient<Ec2Client> ec2ProxyClient,
            final String vpcId,
            final String groupName
    ) {
        final DescribeSecurityGroupsResponse response = ec2ProxyClient.injectCredentialsAndInvokeV2(
                TranslatorV19.describeSecurityGroupsRequest(vpcId, groupName),
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
        try {
            fetchDBInstance(rdsProxyClient, model);
        } catch (DbInstanceNotFoundException e) {
            // the instance is gone, exactly what we need
            return true;
        }
        return false;
    }

    protected boolean isDbInstanceStabilized(
            final ProxyClient<RdsClient> rdsProxyClient,
            final ResourceModel model
    ) {
        final DBInstance dbInstance = fetchDBInstance(rdsProxyClient, model);
        return DBInstanceStatus.Available.equalsString(dbInstance.dbInstanceStatus());
    }

    protected boolean isOptionGroupStabilized(
            final ProxyClient<RdsClient> rdsProxyClient,
            final ResourceModel model
    ) {
        final DBInstance dbInstance = fetchDBInstance(rdsProxyClient, model);
        final List<OptionGroupMembership> optionGroupMemberships = dbInstance.optionGroupMemberships();
        if (CollectionUtils.isNullOrEmpty(optionGroupMemberships)) {
            // no option group membership, the best we can do is to return true
            return true;
        }
        return IN_SYNC_STATUS.equals(optionGroupMemberships.get(0).status());
    }

    protected boolean isDBParameterGroupStabilized(
            final ProxyClient<RdsClient> rdsProxyClient,
            final ResourceModel model
    ) {
        final DBInstance dbInstance = fetchDBInstance(rdsProxyClient, model);
        final List<DBParameterGroupStatus> dbParameterGroupStatuses = dbInstance.dbParameterGroups();
        if (CollectionUtils.isNullOrEmpty(dbParameterGroupStatuses)) {
            return true;
        }
        return IN_SYNC_STATUS.equals(dbParameterGroupStatuses.get(0).parameterApplyStatus());
    }

    protected boolean isDBClusterParameterGroupStabilized(
            final ProxyClient<RdsClient> rdsProxyClient,
            final ResourceModel model
    ) {
        final DBCluster dbCluster = fetchDBCluster(rdsProxyClient, model);
        final List<DBClusterMember> dbClusterMembers = dbCluster.dbClusterMembers();
        if (CollectionUtils.isNullOrEmpty(dbClusterMembers)) {
            return true;
        }
        for (DBClusterMember member : dbClusterMembers) {
            if (model.getDBInstanceIdentifier().equals(member.dbInstanceIdentifier())) {
                return IN_SYNC_STATUS.equals(member.dbClusterParameterGroupStatus());
            }
        }
        return false;
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
                    .translateToServiceRequest(addRequest -> TranslatorV19.addRoleToDbInstanceRequest(progress.getResourceModel(), role))
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
                    .translateToServiceRequest(removeRequest -> TranslatorV19.removeRoleFromDbInstanceRequest(
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
                ).translateToServiceRequest(TranslatorV19::rebootDbInstanceRequest)
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
        return reboot(proxy, rdsProxyClient, progress).then(p -> awaitDBInstanceAvailableStatus(proxy, rdsProxyClient, p));
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
                    Tagging.bestEffortErrorRuleSet(tagsToAdd, tagsToRemove).orElse(DEFAULT_DB_INSTANCE_ERROR_RULE_SET)
            );
        }

        return progress;
    }

    protected RdsClient buildRdsClient() {
        return RdsClientBuilder.getClient();
    }

    protected Ec2Client buildEc2Client() {
        return Ec2ClientBuilder.getClient();
    }
}
