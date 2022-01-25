package software.amazon.rds.dbcluster;

import static software.amazon.rds.dbcluster.Translator.addRoleToDbClusterRequest;
import static software.amazon.rds.dbcluster.Translator.addTagsToResourceRequest;
import static software.amazon.rds.dbcluster.Translator.removeRoleFromDbClusterRequest;
import static software.amazon.rds.dbcluster.Translator.removeTagsFromResourceRequest;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBCluster;
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
import software.amazon.awssdk.services.rds.model.DomainNotFoundException;
import software.amazon.awssdk.services.rds.model.GlobalClusterNotFoundException;
import software.amazon.awssdk.services.rds.model.InsufficientStorageClusterCapacityException;
import software.amazon.awssdk.services.rds.model.InvalidDbClusterStateException;
import software.amazon.awssdk.services.rds.model.InvalidDbInstanceStateException;
import software.amazon.awssdk.services.rds.model.InvalidDbSubnetGroupStateException;
import software.amazon.awssdk.services.rds.model.InvalidGlobalClusterStateException;
import software.amazon.awssdk.services.rds.model.InvalidSubnetException;
import software.amazon.awssdk.services.rds.model.InvalidVpcNetworkStateException;
import software.amazon.awssdk.services.rds.model.KmsKeyNotAccessibleException;
import software.amazon.awssdk.services.rds.model.StorageQuotaExceededException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.error.ErrorRuleSet;
import software.amazon.rds.common.error.ErrorStatus;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.logging.RequestLogger;

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {
    public static final String RESOURCE_IDENTIFIER = "dbcluster";
    public static final String STACK_NAME = "rds";

    private static final String DB_CLUSTER_FAILED_TO_STABILIZE = "DBCluster %s failed to stabilize.";

    protected static final int RESOURCE_ID_MAX_LENGTH = 63;

    protected static final ErrorRuleSet DEFAULT_DB_CLUSTER_ERROR_RULE_SET = ErrorRuleSet.builder()
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    ErrorCode.DBClusterAlreadyExistsFault)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    DbClusterAlreadyExistsException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    DbClusterNotFoundException.class,
                    DbClusterSnapshotNotFoundException.class,
                    DbClusterParameterGroupNotFoundException.class,
                    DbInstanceNotFoundException.class,
                    DbSubnetGroupNotFoundException.class,
                    DomainNotFoundException.class,
                    GlobalClusterNotFoundException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ServiceLimitExceeded),
                    DbClusterQuotaExceededException.class,
                    InsufficientStorageClusterCapacityException.class,
                    StorageQuotaExceededException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
                    InvalidDbClusterStateException.class,
                    InvalidDbInstanceStateException.class,
                    InvalidDbSubnetGroupStateException.class,
                    InvalidGlobalClusterStateException.class,
                    InvalidSubnetException.class,
                    InvalidVpcNetworkStateException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
                    DbSubnetGroupDoesNotCoverEnoughAZsException.class,
                    KmsKeyNotAccessibleException.class)
            .build()
            .orElse(Commons.DEFAULT_ERROR_RULE_SET);

    protected static final ErrorRuleSet ADD_ASSOC_ROLES_ERROR_RULE_SET = ErrorRuleSet.builder()
            .withErrorClasses(ErrorStatus.ignore(),
                    DbClusterRoleAlreadyExistsException.class)
            .build()
            .orElse(DEFAULT_DB_CLUSTER_ERROR_RULE_SET);

    protected static final ErrorRuleSet REMOVE_ASSOC_ROLES_ERROR_RULE_SET = ErrorRuleSet.builder()
            .withErrorClasses(ErrorStatus.ignore(),
                    DbClusterRoleNotFoundException.class)
            .build()
            .orElse(DEFAULT_DB_CLUSTER_ERROR_RULE_SET);

    protected HandlerConfig config;

    private final Collection<String> SENSITIVE_PARAMETERS_PARENTS = ImmutableSet.of("desiredResourceState", "previousResourceState", "resourceModel");
    private final Set<String> SENSITIVE_PARAMETERS = ImmutableSet.of("masterUsername", "masterUserPassword");

    public BaseHandlerStd(final HandlerConfig config) {
        super();
        this.config = config;
    }

    @Override
    public final ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger
    ) {
        RequestLogger requestLogger = new RequestLogger(logger, request, parameterName -> true);
        logRequest(requestLogger, request);
        ProgressEvent<ResourceModel, CallbackContext> progressEvent = null;
        try {
            progressEvent = handleRequest(
                    proxy,
                    request,
                    callbackContext != null ? callbackContext : new CallbackContext(),
                    proxy.newProxy(ClientBuilder::getClient),
                    logger
            );
            logResponse(requestLogger, progressEvent);
        } catch (Throwable throwable) {
            requestLogger.logAndThrow(throwable);
        }
        return progressEvent;
    }

    protected abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger
    );

    protected DBCluster fetchDBCluster(
            final ProxyClient<RdsClient> proxyClient,
            final ResourceModel model
    ) {
        final DescribeDbClustersResponse response = proxyClient.injectCredentialsAndInvokeV2(
                Translator.describeDbClustersRequest(model),
                proxyClient.client()::describeDBClusters
        );
        return response.dbClusters().stream().findFirst().get();
    }

    protected boolean isDBClusterStabilized(
            final ProxyClient<RdsClient> proxyClient,
            final ResourceModel model,
            final DBClusterStatus expectedStatus
    ) {
        try {
            final DBCluster dbCluster = fetchDBCluster(proxyClient, model);
            return expectedStatus.equalsString(dbCluster.status());
        } catch (DbClusterNotFoundException e) {
            throw new CfnNotFoundException(ResourceModel.TYPE_NAME, e.getMessage());
        } catch (Exception e) {
            throw new CfnNotStabilizedException(DB_CLUSTER_FAILED_TO_STABILIZE, model.getDBClusterIdentifier(), e);
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

    protected ProgressEvent<ResourceModel, CallbackContext> addAssociatedRoles(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final List<DBClusterRole> roles
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
                            ADD_ASSOC_ROLES_ERROR_RULE_SET
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
            final List<DBClusterRole> roles
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
                            REMOVE_ASSOC_ROLES_ERROR_RULE_SET
                    ))
                    .success();
            if (!progressEvent.isSuccess()) return progressEvent;
        }
        return ProgressEvent.progress(model, callbackContext);
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
        return Optional.ofNullable(role.getRoleArn()).orElse("").equals(sdkRole.roleArn()) &&
                Optional.ofNullable(role.getFeatureName()).orElse("").equals(sdkRole.featureName());
    }

    protected ProgressEvent<ResourceModel, CallbackContext> tagResource(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Collection<Tag> previousTags,
            final Collection<Tag> desiredTags
    ) {
        final Set<Tag> tagsToAdd = new HashSet<>(desiredTags);
        final Set<Tag> tagsToRemove = new HashSet<>(previousTags);

        tagsToAdd.removeAll(previousTags);
        tagsToRemove.removeAll(desiredTags);

        if (tagsToAdd.isEmpty() && tagsToRemove.isEmpty()) {
            return progress;
        }

        try {
            final DBCluster dbCluster = fetchDBCluster(proxyClient, progress.getResourceModel());
            final String arn = dbCluster.dbClusterArn();

            proxyClient.injectCredentialsAndInvokeV2(
                    removeTagsFromResourceRequest(arn, tagsToRemove),
                    proxyClient.client()::removeTagsFromResource
            );
            proxyClient.injectCredentialsAndInvokeV2(
                    addTagsToResourceRequest(arn, tagsToAdd),
                    proxyClient.client()::addTagsToResource
            );
        } catch (Exception exception) {
            return Commons.handleException(progress, exception, DEFAULT_DB_CLUSTER_ERROR_RULE_SET);
        }

        return progress;
    }

    private void logResponse(final RequestLogger requestLogger,
                             final ProgressEvent<ResourceModel, CallbackContext> progressEvent) {
        requestLogger.log("Response ProgressEvent: ", progressEvent, SENSITIVE_PARAMETERS_PARENTS);
        requestLogger.log("Response ResourceModel: ", progressEvent.getResourceModel(), SENSITIVE_PARAMETERS);
    }

    private void logRequest(final RequestLogger requestLogger, final ResourceHandlerRequest<ResourceModel> request) {
        requestLogger.log("Request: ", request, SENSITIVE_PARAMETERS_PARENTS);
        requestLogger.log("DesiredResourceState: ", request.getDesiredResourceState(), SENSITIVE_PARAMETERS);
        requestLogger.log("PreviousResourceState: ", request.getPreviousResourceState(), SENSITIVE_PARAMETERS);
    }
}
