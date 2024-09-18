package software.amazon.rds.dbshardgroup;

import java.util.List;
import java.util.function.Function;
import org.apache.commons.collections.CollectionUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBShardGroup;
import software.amazon.awssdk.services.rds.model.DbClusterNotFoundException;
import software.amazon.awssdk.services.rds.model.DbShardGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.error.ErrorRuleSet;
import software.amazon.rds.common.error.ErrorStatus;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;

public class DeleteHandler extends BaseHandlerStd {
    protected static final ErrorRuleSet DELETE_DB_SHARD_GROUP_ERROR_RULE_SET = ErrorRuleSet
            .extend(DEFAULT_DB_SHARD_GROUP_ERROR_RULE_SET)
            .withErrorClasses(ErrorStatus.ignore(OperationStatus.IN_PROGRESS),
                    DbShardGroupNotFoundException.class)
            .withErrorClasses(ErrorStatus.ignore(OperationStatus.SUCCESS),
                    DbClusterNotFoundException.class)
            .build();

    /** Default constructor with default backoff */
    public DeleteHandler() {
        this(DB_SHARD_GROUP_HANDLER_CONFIG_36H);
    }

    /** Default constructor with custom config */
    public DeleteHandler(HandlerConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext) {

        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                .then(progress -> Commons.execOnce(progress, () -> deleteDbShardGroupIfNotInProgress(progress, proxy, request, callbackContext, proxyClient),
                        CallbackContext::isDescribed, CallbackContext::setDescribed))
                // Stabilize as an independent step for cases where there is an out-of-band delete
                .then(progress -> proxy.initiate("rds::delete-db-shard-group-stabilize", proxyClient, request.getDesiredResourceState(), callbackContext)
                        .translateToServiceRequest(Function.identity())
                        .backoffDelay(config.getBackoff())
                        .makeServiceCall(NOOP_CALL)
                        .stabilize((noopRequest, noopResponse, proxyInvocation, model, context) -> isDbShardGroupDeleted(model, proxyInvocation) && isDbClusterStabilizedOrDeleted(proxyInvocation, context))
                        .handleError((noopRequest, exception, client, model, context) -> Commons.handleException(
                                ProgressEvent.progress(model, context),
                                exception,
                                DELETE_DB_SHARD_GROUP_ERROR_RULE_SET,
                                requestLogger
                        )).progress())
                .then(progress -> ProgressEvent.defaultSuccessHandler(null));
    }

    private ProgressEvent<ResourceModel, CallbackContext> deleteDbShardGroupIfNotInProgress(final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                                            final AmazonWebServicesClientProxy proxy,
                                                                                            final ResourceHandlerRequest<ResourceModel> request,
                                                                                            final CallbackContext callbackContext,
                                                                                            final ProxyClient<RdsClient> proxyClient) {
        try {
            final DBShardGroup dbShardGroup = proxyClient.injectCredentialsAndInvokeV2(
                            Translator.describeDbShardGroupsRequest(request.getDesiredResourceState()),
                            proxyClient.client()::describeDBShardGroups)
                    .dbShardGroups().get(0);
            callbackContext.setDbClusterIdentifier(dbShardGroup.dbClusterIdentifier());
            if (!ResourceStatus.DELETING.equalsString(dbShardGroup.status())){
                return deleteDbShardGroup(proxy, request, callbackContext, proxyClient);
            } else {
                return progress;
            }
        } catch (DbShardGroupNotFoundException e) {
            // If the shard group is not found, we exit.
            return ProgressEvent.defaultFailureHandler(e, HandlerErrorCode.NotFound);
        }
    }

    private ProgressEvent<ResourceModel, CallbackContext> deleteDbShardGroup(final AmazonWebServicesClientProxy proxy,
                                                                             final ResourceHandlerRequest<ResourceModel> request,
                                                                             final CallbackContext callbackContext,
                                                                             final ProxyClient<RdsClient> proxyClient) {
        return proxy.initiate("rds::delete-db-shard-group", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::deleteDbShardGroupRequest)
                .backoffDelay(config.getBackoff())
                .makeServiceCall((deleteDbShardGroupRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(deleteDbShardGroupRequest, proxyInvocation.client()::deleteDBShardGroup))
                .handleError((deleteRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DELETE_DB_SHARD_GROUP_ERROR_RULE_SET,
                        requestLogger
                )).progress();
    }

    private boolean isDbShardGroupDeleted(final ResourceModel model,
                                final ProxyClient<RdsClient> proxyClient) {
        try {
            proxyClient.injectCredentialsAndInvokeV2(Translator.describeDbShardGroupsRequest(model), proxyClient.client()::describeDBShardGroups);
            return false;
        } catch (DbShardGroupNotFoundException e) {
            return true;
        }
    }

    private boolean isDbClusterStabilizedOrDeleted(final ProxyClient<RdsClient> proxyClient,
                                                   final CallbackContext callbackContext) {
        try {
            final List<DBCluster> dbClusters = proxyClient.injectCredentialsAndInvokeV2(
                    DescribeDbClustersRequest.builder()
                            .dbClusterIdentifier(callbackContext.getDbClusterIdentifier())
                            .build(),
                    proxyClient.client()::describeDBClusters
            ).dbClusters();

            if (CollectionUtils.isEmpty(dbClusters)) {
                // For an empty response, we assume the same behavior for a DbClusterNotFoundException
                // https://jira.rds.a2z.com/browse/WS-6673
                return true;
            } else {
                return isDBClusterAvailable(dbClusters.get(0));
            }
        } catch (DbClusterNotFoundException e) {
            return true;
        }
    }
}
