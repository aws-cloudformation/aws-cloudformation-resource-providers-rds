package software.amazon.rds.dbinstance;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.BooleanUtils;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeSecurityGroupsResponse;
import software.amazon.awssdk.services.ec2.model.SecurityGroup;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBClusterSnapshot;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DBInstanceAutomatedBackup;
import software.amazon.awssdk.services.rds.model.DBSnapshot;
import software.amazon.awssdk.services.rds.model.DbInstanceNotFoundException;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterSnapshotsResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbInstanceAutomatedBackupsResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsResponse;
import software.amazon.awssdk.services.rds.model.Event;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.awssdk.utils.StringUtils;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.cloudformation.resource.ResourceTypeSchema;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.Events;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.HandlerMethod;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.logging.LoggingProxyClient;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.common.printer.FilteredJsonPrinter;
import software.amazon.rds.common.request.RequestValidationException;
import software.amazon.rds.common.request.ValidatedRequest;
import software.amazon.rds.common.request.Validations;
import software.amazon.rds.dbinstance.client.ApiVersion;
import software.amazon.rds.dbinstance.client.ApiVersionDispatcher;
import software.amazon.rds.dbinstance.client.Ec2ClientProvider;
import software.amazon.rds.dbinstance.client.RdsClientProvider;
import software.amazon.rds.dbinstance.client.VersionedProxyClient;
import software.amazon.rds.dbinstance.common.ErrorRuleSets;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {

    public static final String RESOURCE_IDENTIFIER = "dbinstance";
    public static final String STACK_NAME = "rds";

    public static final String API_VERSION_V12 = "2012-09-17";

    protected static final int RESOURCE_ID_MAX_LENGTH = 63;

    protected final static HandlerConfig DEFAULT_DB_INSTANCE_HANDLER_CONFIG = HandlerConfig.builder()
        .backoff(Constant.of().delay(Duration.ofSeconds(30)).timeout(Duration.ofMinutes(180)).build())
        .build();

    protected final static HandlerConfig DB_INSTANCE_HANDLER_CONFIG_36H = HandlerConfig.builder()
        .backoff(Constant.of().delay(Duration.ofSeconds(30)).timeout(Duration.ofHours(36)).build())
        .build();

    protected static final RuntimeException MISSING_METHOD_VERSION_EXCEPTION = new RuntimeException("Missing method version");


    protected static final String ILLEGAL_DELETION_POLICY_ERROR = "DeletionPolicy:Snapshot cannot be specified for a cluster instance, use deletion policy on the cluster instead.";

    protected static final String UNKNOWN_SOURCE_REGION_ERROR = "Unknown source region";

    protected static final String RESOURCE_UPDATED_AT = "resource-updated-at";

    protected static final String DB_INSTANCE_REQUEST_STARTED_AT = "dbinstance-request-started-at";

    protected static final String DB_INSTANCE_REQUEST_IN_PROGRESS_AT = "dbinstance-request-in-progress-at";

    protected static final String DB_INSTANCE_STABILIZATION_TIME = "dbinstance-stabilization-time";

    protected final HandlerConfig config;

    protected RequestLogger requestLogger;

    private final ApiVersionDispatcher<ResourceModel, CallbackContext> apiVersionDispatcher;

    protected final FilteredJsonPrinter PARAMETERS_FILTER = new FilteredJsonPrinter("MasterUsername", "MasterUserPassword", "TdeCredentialPassword");

    protected static final BiFunction<ResourceModel, ProxyClient<RdsClient>, ResourceModel> NOOP_CALL = (model, proxyClient) -> model;

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

    protected static final ResourceTypeSchema resourceTypeSchema = ResourceTypeSchema.load(new Configuration().resourceSchemaJsonObject());

    public BaseHandlerStd(final HandlerConfig config) {
        super();
        this.config = config;
        this.apiVersionDispatcher = new ApiVersionDispatcher<ResourceModel, CallbackContext>()
            .register(ApiVersion.V12, (m, c) -> !software.amazon.awssdk.utils.CollectionUtils.isNullOrEmpty(m.getDBSecurityGroups()));
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
        final VersionedProxyClient<Ec2Client> ec2ProxyClient
    );

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext context,
        final VersionedProxyClient<RdsClient> rdsProxyClient,
        final VersionedProxyClient<Ec2Client> ec2ProxyClient,
        final RequestLogger requestLogger
    ) {
        this.requestLogger = requestLogger;
        resourceStabilizationTime(context);
        try {
            validateRequest(request);
        } catch (RequestValidationException exception) {
            return ProgressEvent.defaultFailureHandler(exception, HandlerErrorCode.InvalidRequest);
        }

        return handleRequest(proxy, new ValidatedRequest<ResourceModel>(request), context, rdsProxyClient, ec2ProxyClient);
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
                requestLogger
            ));
    }

    protected ProgressEvent<ResourceModel, CallbackContext> updateDbInstanceV12(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final ProxyClient<RdsClient> rdsProxyClient,
        final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        requestLogger.log("UpdateDbInstanceAPIv12Invoked");
        requestLogger.log("Detected API Version 12", "Detected modifyDbInstanceRequestV12. " +
            "This indicates that the customer is using DBSecurityGroup, which may result in certain features not" +
            " functioning properly. Please refer to the API model for supported parameters");
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
                software.amazon.rds.dbinstance.common.ErrorRuleSets.MODIFY_DB_INSTANCE,
                requestLogger
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
                software.amazon.rds.dbinstance.common.ErrorRuleSets.MODIFY_DB_INSTANCE,
                requestLogger
            ))
            .progress();
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

    protected DBInstance fetchDBInstance(
        final ProxyClient<RdsClient> rdsProxyClient,
        final String dbInstanceIdentifier
    ) {
        final DescribeDbInstancesResponse response = rdsProxyClient.injectCredentialsAndInvokeV2(
            Translator.describeDbInstanceByDBInstanceIdentifierRequest(dbInstanceIdentifier),
            rdsProxyClient.client()::describeDBInstances
        );
        return response.dbInstances().get(0);
    }

    protected DBInstance fetchDBInstanceByResourceId(
        final ProxyClient<RdsClient> rdsProxyClient,
        final String resourceId
    ) {
        final DescribeDbInstancesResponse response = rdsProxyClient.injectCredentialsAndInvokeV2(
            Translator.describeDbInstanceByResourceIdRequest(resourceId),
            rdsProxyClient.client()::describeDBInstances
        );
        return response.dbInstances().get(0);
    }

    protected DBInstanceAutomatedBackup fetchAutomaticBackup(
        final ProxyClient<RdsClient> rdsProxyClient,
        final String automaticBackupArn
    ) {
        final DescribeDbInstanceAutomatedBackupsResponse response = rdsProxyClient.injectCredentialsAndInvokeV2(
            Translator.describeDBInstanceAutomaticBackup(automaticBackupArn),
            rdsProxyClient.client()::describeDBInstanceAutomatedBackups
        );
        return response.dbInstanceAutomatedBackups().get(0);
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

    protected DBCluster fetchDBCluster(
        final ProxyClient<RdsClient> rdsProxyClient,
        final String dbClusterIdentifier
    ) {
        final DescribeDbClustersResponse response = rdsProxyClient.injectCredentialsAndInvokeV2(
            Translator.describeDbClusterRequest(dbClusterIdentifier),
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

    protected DBClusterSnapshot fetchDBClusterSnapshot(
        final ProxyClient<RdsClient> rdsProxyClient,
        final ResourceModel model
    ) {
        final DescribeDbClusterSnapshotsResponse response = rdsProxyClient.injectCredentialsAndInvokeV2(
            Translator.describeDbClusterSnapshotsRequest(model),
            rdsProxyClient.client()::describeDBClusterSnapshots
        );
        return response.dbClusterSnapshots().get(0);
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
            fetchDBInstance(rdsProxyClient, model);
        } catch (DbInstanceNotFoundException e) {
            // the instance is gone, exactly what we need
            return true;
        }

        return false;
    }

    protected boolean isDBInstanceStabilizedAfterMutate(
        final ProxyClient<RdsClient> rdsProxyClient,
        final ResourceModel model,
        final CallbackContext context
    ) {
        final DBInstance dbInstance = fetchDBInstance(rdsProxyClient, model);

        return DBInstancePredicates.isDBInstanceStabilizedAfterMutate(dbInstance, model, context, requestLogger);
    }

    private void resourceStabilizationTime(final CallbackContext context) {
        context.timestampOnce(DB_INSTANCE_REQUEST_STARTED_AT, Instant.now());
        context.timestamp(DB_INSTANCE_REQUEST_IN_PROGRESS_AT, Instant.now());
        context.calculateTimeDeltaInMinutes(DB_INSTANCE_STABILIZATION_TIME,
            context.getTimestamp(DB_INSTANCE_REQUEST_IN_PROGRESS_AT),
            context.getTimestamp(DB_INSTANCE_REQUEST_STARTED_AT));
    }

    protected boolean isInstanceStabilizedAfterReplicationStop(
        final ProxyClient<RdsClient> rdsProxyClient,
        final ResourceModel model
    ) {
        final DBInstance dbInstance = fetchDBInstance(rdsProxyClient, model);

        return DBInstancePredicates.isInstanceStabilizedAfterReplicationStop(dbInstance, model);
    }

    protected boolean isInstanceStabilizedAfterReplicationStart(final ProxyClient<RdsClient> rdsProxyClient,
                                                                final ResourceModel model) {
        final DBInstance dbInstance = fetchDBInstance(rdsProxyClient, model);

        return DBInstancePredicates.isInstanceStabilizedAfterReplicationStart(dbInstance, model);
    }

    protected boolean isDBInstanceStabilizedAfterReboot(
        final ProxyClient<RdsClient> rdsProxyClient,
        final ResourceModel model
    ) {
        final DBInstance dbInstance = fetchDBInstance(rdsProxyClient, model);
        if (DBInstancePredicates.isDBClusterMember(model)) {
            final DBCluster dbCluster = fetchDBCluster(rdsProxyClient, model);
            return DBInstancePredicates.isDBInstanceStabilizedAfterReboot(dbInstance, dbCluster, model, requestLogger);
        } else {
            return DBInstancePredicates.isDBInstanceStabilizedAfterReboot(dbInstance, requestLogger);
        }
    }

    protected boolean isOptionGroupStabilized(
        final ProxyClient<RdsClient> rdsProxyClient,
        final ResourceModel model
    ) {
        final DBInstance dbInstance = fetchDBInstance(rdsProxyClient, model);

        return DBInstancePredicates.isOptionGroupInSync(dbInstance);
    }

    protected boolean isDBParameterGroupStabilized(
        final ProxyClient<RdsClient> rdsProxyClient,
        final ResourceModel model
    ) {
        final DBInstance dbInstance = fetchDBInstance(rdsProxyClient, model);

        return DBInstancePredicates.isDBParameterGroupInSync(dbInstance);
    }

    protected boolean isDBClusterParameterGroupStabilized(
        final ProxyClient<RdsClient> rdsProxyClient,
        final ResourceModel model
    ) {
        final DBCluster dbCluster = fetchDBCluster(rdsProxyClient, model);

        return DBInstancePredicates.isDBClusterParameterGroupInSync(model, dbCluster);
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
                Objects.equals(StringUtils.trimToNull(role.featureName()), StringUtils.trimToNull(lookupRole.getFeatureName())))
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
                    software.amazon.rds.dbinstance.common.ErrorRuleSets.UPDATE_ASSOCIATED_ROLES,
                    requestLogger
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
                    software.amazon.rds.dbinstance.common.ErrorRuleSets.UPDATE_ASSOCIATED_ROLES,
                    requestLogger
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
                ErrorRuleSets.REBOOT_DB_INSTANCE,
                requestLogger
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
                software.amazon.rds.dbinstance.common.ErrorRuleSets.UPDATE_ASSOCIATED_ROLES,
                requestLogger
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
                return Commons.handleException(progress, e, software.amazon.rds.dbinstance.common.ErrorRuleSets.DEFAULT_DB_INSTANCE, requestLogger);
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

        final Collection<Tag> effectivePreviousTags = Tagging.translateTagsToSdk(previousTags);
        final Collection<Tag> effectiveDesiredTags = Tagging.translateTagsToSdk(desiredTags);

        final Collection<Tag> tagsToRemove = Tagging.exclude(effectivePreviousTags, effectiveDesiredTags);
        final Collection<Tag> tagsToAdd = Tagging.exclude(effectiveDesiredTags, effectivePreviousTags);

        if (tagsToAdd.isEmpty() && tagsToRemove.isEmpty()) {
            return progress;
        }

        final Tagging.TagSet rulesetTagsToAdd = Tagging.exclude(desiredTags, previousTags);
        final Tagging.TagSet rulesetTagsToRemove = Tagging.exclude(previousTags, desiredTags);

        DBInstance dbInstance;
        try {
            dbInstance = fetchDBInstance(rdsProxyClient, progress.getResourceModel());
        } catch (Exception exception) {
            return Commons.handleException(progress, exception, software.amazon.rds.dbinstance.common.ErrorRuleSets.DEFAULT_DB_INSTANCE, requestLogger);
        }

        final String arn = dbInstance.dbInstanceArn();

        try {
            Tagging.removeTags(rdsProxyClient, arn, Tagging.translateTagsToSdk(tagsToRemove));
            Tagging.addTags(rdsProxyClient, arn, Tagging.translateTagsToSdk(tagsToAdd));
        } catch (Exception exception) {
            return Commons.handleException(
                progress,
                exception,
                software.amazon.rds.dbinstance.common.ErrorRuleSets.DEFAULT_DB_INSTANCE.extendWith(Tagging.getUpdateTagsAccessDeniedRuleSet(rulesetTagsToAdd, rulesetTagsToRemove)),
                requestLogger
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

    protected ProgressEvent<ResourceModel, CallbackContext> stopAutomaticBackupReplicationInRegion(
        final String dbInstanceArn,
        final AmazonWebServicesClientProxy proxy,
        final ProgressEvent<ResourceModel, CallbackContext> progress,
        final ProxyClient<RdsClient> sourceRegionClient,
        final String region
    ) {
        final ProxyClient<RdsClient> rdsClient = new LoggingProxyClient<>(requestLogger, proxy.newProxy(() -> new RdsClientProvider().getClientForRegion(region)));

        return proxy.initiate("rds::stop-db-instance-automatic-backup-replication", rdsClient, progress.getResourceModel(), progress.getCallbackContext())
            .translateToServiceRequest(resourceModel -> Translator.stopDbInstanceAutomatedBackupsReplicationRequest(dbInstanceArn))
            .backoffDelay(config.getBackoff())
            .makeServiceCall((request, client) -> rdsClient.injectCredentialsAndInvokeV2(
                request,
                rdsClient.client()::stopDBInstanceAutomatedBackupsReplication
            ))
            .stabilize((request, response, client, model, context) ->
                isInstanceStabilizedAfterReplicationStop(sourceRegionClient, model))
            .handleError((request, exception, client, model, context) -> Commons.handleException(
                ProgressEvent.progress(model, context),
                exception,
                software.amazon.rds.dbinstance.common.ErrorRuleSets.MODIFY_DB_INSTANCE_AUTOMATIC_BACKUP_REPLICATION,
                requestLogger
            ))
            .progress();
    }

    protected ProgressEvent<ResourceModel, CallbackContext> startAutomaticBackupReplicationInRegion(
        final String dbInstanceArn,
        final String kmsKeyId,
        final AmazonWebServicesClientProxy proxy,
        final ProgressEvent<ResourceModel, CallbackContext> progress,
        final ProxyClient<RdsClient> sourceRegionClient,
        final String region
    ) {
        final ProxyClient<RdsClient> rdsClient = new LoggingProxyClient<>(requestLogger, proxy.newProxy(() -> new RdsClientProvider().getClientForRegion(region)));
        final String AUTOMATIC_REPLICATION_KMS_KEY_ERROR = "Encrypted instances require a valid KMS key ID";
        final String AUTOMATIC_REPLICATION_KMS_KEY_EVENT_MESSAGE = "Provide a valid value for the AutomaticBackupReplicationKmsKeyId property.";

        return proxy.initiate("rds::start-db-instance-automatic-backup-replication", rdsClient, progress.getResourceModel(), progress.getCallbackContext())
            .translateToServiceRequest(resourceModel -> Translator.startDbInstanceAutomatedBackupsReplicationRequest(dbInstanceArn, kmsKeyId))
            .backoffDelay(config.getBackoff())
            .makeServiceCall((request, client) -> rdsClient.injectCredentialsAndInvokeV2(
                request,
                rdsClient.client()::startDBInstanceAutomatedBackupsReplication
            ))
            .stabilize((request, response, proxyInvocation, model, context) ->
                isInstanceStabilizedAfterReplicationStart(sourceRegionClient, model))
            .handleError((request, exception, client, model, context) -> {
                ProgressEvent<ResourceModel, CallbackContext> progressEvent = Commons.handleException(
                    ProgressEvent.progress(model, context),
                    exception,
                    software.amazon.rds.dbinstance.common.ErrorRuleSets.MODIFY_DB_INSTANCE_AUTOMATIC_BACKUP_REPLICATION,
                    requestLogger
                );
                if (exception.getMessage().contains(AUTOMATIC_REPLICATION_KMS_KEY_ERROR)) {
                    progressEvent.setMessage(StringUtils.trimToEmpty(progressEvent.getMessage())
                        .concat(" " + AUTOMATIC_REPLICATION_KMS_KEY_EVENT_MESSAGE));
                }
                return progressEvent;
            })
            .progress();
    }
}
