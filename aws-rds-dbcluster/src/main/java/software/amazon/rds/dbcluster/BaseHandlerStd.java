package software.amazon.rds.dbcluster;

import com.amazonaws.util.StringUtils;
import com.google.common.collect.Sets;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CloudwatchLogsExportConfiguration;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.cloudformation.exceptions.TerminalException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.CallChain;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.delay.Exponential;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {
    private static final String MESSAGE_FORMAT_FAILED_TO_STABILIZE = "DBCluster %s failed to stabilize.";
    protected static int NO_CALLBACK_DELAY = 0;
    protected static final Exponential EXPONENTIAL = Exponential.of().timeout(Duration.ofHours(12L)).minDelay(Duration.ofSeconds(10L)).build();

    @Override
    public final ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                             final ResourceHandlerRequest<ResourceModel> request,
                                                                             final CallbackContext callbackContext,
                                                                             final Logger logger) {

        return handleRequest(proxy, request, callbackContext != null ? callbackContext : new CallbackContext(), proxy.newProxy(ClientBuilder::getClient), logger);
    }

    protected abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(AmazonWebServicesClientProxy proxy,
                                                                                   ResourceHandlerRequest<ResourceModel> request,
                                                                                   CallbackContext callbackContext,
                                                                                   ProxyClient<RdsClient> proxyClient,
                                                                                   Logger logger);

    // DBCluster Stabilization
    protected boolean isDBClusterStabilized(final ProxyClient<RdsClient> proxyClient,
                                            final ResourceModel model,
                                            final DBClusterStatus.Status expectedStatus) {

        final String status = proxyClient.injectCredentialsAndInvokeV2(Translator.describeDbClustersRequest(model), proxyClient.client()::describeDBClusters)
                .dbClusters().stream().findFirst().get().status();

        if (DBClusterStatus.Status.Failed.equalsString(status))
            throw new TerminalException(String.format(MESSAGE_FORMAT_FAILED_TO_STABILIZE, model.getDBClusterIdentifier()));
        return expectedStatus.equalsString(status);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> waitForDBCluster(final AmazonWebServicesClientProxy proxy,
                                                                             final ProxyClient<RdsClient> proxyClient,
                                                                             final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                             final DBClusterStatus.Status expectedStatus) {

        return proxy.initiate("rds::stabilize-dbcluster" + getClass().getSimpleName(), proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .request(Function.identity())
                .call((m, c) -> m)
                .stabilize((model, response, proxyInvocation, _model1, _context) -> isDBClusterStabilized(proxyInvocation, model, expectedStatus)).progress();
    }

    protected DBCluster describeDBCluster(final ProxyClient<RdsClient> proxyClient,
                                          final ResourceModel model) {
        return proxyClient.injectCredentialsAndInvokeV2(Translator.describeDbClustersRequest(model), proxyClient.client()::describeDBClusters)
                .dbClusters().stream().findFirst().get();
    }

    // Create or Restore
    protected CallChain.Stabilizer<?, ?, RdsClient, ResourceModel, CallbackContext> createDBCluster(final AmazonWebServicesClientProxy proxy,
                                                                                                    final ProxyClient<RdsClient> proxyClient,
                                                                                                    final ResourceModel model,
                                                                                                    final CallbackContext callbackContext) {
        if (!StringUtils.isNullOrEmpty(model.getSourceDBClusterIdentifier())) {
            // restore to point in time
            return proxy.initiate("rds::restore-dbcluster-in-time", proxyClient, model, callbackContext)
                    .request(Translator::restoreDbClusterToPointInTimeRequest)
                    .retry(EXPONENTIAL)
                    .call((dbClusterRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(dbClusterRequest, proxyInvocation.client()::restoreDBClusterToPointInTime));
        } else if (!StringUtils.isNullOrEmpty(model.getSnapshotIdentifier())) {
            // restore from snapshot
            return proxy.initiate("rds::restore-dbcluster-snapshot", proxyClient, model, callbackContext)
                    .request(Translator::restoreDbClusterFromSnapshotRequest)
                    .retry(EXPONENTIAL)
                    .call((dbClusterRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(dbClusterRequest, proxyInvocation.client()::restoreDBClusterFromSnapshot));
        } else {
            // regular create
            return proxy.initiate("rds::create-dbcluster", proxyClient, model, callbackContext)
                    .request(Translator::createDbClusterRequest)
                    .retry(EXPONENTIAL)
                    .call((dbClusterRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(dbClusterRequest, proxyInvocation.client()::createDBCluster));
        }
    }

    // Modify or Post Create
    protected ProgressEvent<ResourceModel, CallbackContext> modifyDBCluster(final AmazonWebServicesClientProxy proxy,
                                                                            final ProxyClient<RdsClient> proxyClient,
                                                                            final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                            final CloudwatchLogsExportConfiguration config) {
        return proxy.initiate("rds::modify-dbcluster", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .request((modelRequest) -> Translator.modifyDbClusterRequest(modelRequest, config))
                .call((dbClusterModifyRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(dbClusterModifyRequest, proxyInvocation.client()::modifyDBCluster))
                .progress();
    }

    // Add|Remove DBCluster Roles
    protected ProgressEvent<ResourceModel, CallbackContext> addAssociatedRoles(final AmazonWebServicesClientProxy proxy,
                                                                               final ProxyClient<RdsClient> proxyClient,
                                                                               final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                               final List<DBClusterRole> roles) {

        final ResourceModel model = progress.getResourceModel();
        final CallbackContext callbackContext = progress.getCallbackContext();
        roles.stream().forEach(dbClusterRole ->
                proxy.initiate("rds::add-roles-to-dbcluster", proxyClient, model, callbackContext)
                        .request(modelRequest -> Translator.addRoleToDbClusterRequest(modelRequest.getDBClusterIdentifier(), dbClusterRole.getRoleArn(), dbClusterRole.getFeatureName()))
                        .call((modelRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(modelRequest, proxyInvocation.client()::addRoleToDBCluster))
                        .stabilize((addRoleToDbClusterRequest, addRoleToDBClusterResponse, proxyInvocation, modelRequest, context) ->
                                isRoleStabilized(proxyInvocation, modelRequest, dbClusterRole, true))
                        .progress());
        return ProgressEvent.defaultInProgressHandler(callbackContext, NO_CALLBACK_DELAY, model);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> removeAssociatedRoles(final AmazonWebServicesClientProxy proxy,
                                                                                  final ProxyClient<RdsClient> proxyClient,
                                                                                  final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                                  final List<DBClusterRole> roles) {

        final ResourceModel model = progress.getResourceModel();
        final CallbackContext callbackContext = progress.getCallbackContext();

        roles.stream().forEach(dbClusterRole ->
                proxy.initiate("rds::remove-roles-to-dbcluster", proxyClient, model, callbackContext)
                        .request(modelRequest -> Translator.removeRoleFromDbClusterRequest(modelRequest.getDBClusterIdentifier(), dbClusterRole.getRoleArn(), dbClusterRole.getFeatureName()))
                        .call((modelRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(modelRequest, proxyInvocation.client()::removeRoleFromDBCluster))
                        .stabilize((addRoleToDbClusterRequest, addRoleToDBClusterResponse, proxyInvocation, modelRequest, callbackContext1) ->
                                isRoleStabilized(proxyInvocation, modelRequest, dbClusterRole, false))
                        .progress());
        return ProgressEvent.defaultInProgressHandler(callbackContext, NO_CALLBACK_DELAY, model);
    }

    protected boolean isRoleStabilized(final ProxyClient<RdsClient> proxyClient,
                                       final ResourceModel model,
                                       final software.amazon.rds.dbcluster.DBClusterRole addedRole,
                                       final boolean attached) {
        Predicate<software.amazon.awssdk.services.rds.model.DBClusterRole> isAttached = dbCluster ->
                dbCluster.roleArn().equals(addedRole.getRoleArn()) &&
                (dbCluster.featureName().equals(addedRole.getFeatureName()) || StringUtils.isNullOrEmpty(dbCluster.featureName()));
        Predicate<software.amazon.awssdk.services.rds.model.DBClusterRole> isDetached = dbCluster -> !dbCluster.roleArn().equals(addedRole.getRoleArn());
        final DBCluster dbCluster = describeDBCluster(proxyClient, model);

        if (attached)
            return dbCluster.associatedRoles().stream().anyMatch(isAttached);
        return dbCluster.associatedRoles().isEmpty() || dbCluster.associatedRoles().stream().anyMatch(isDetached);
    }

    // Tag DBCluster
    protected ProgressEvent<ResourceModel, CallbackContext> tagResource(final AmazonWebServicesClientProxy proxy,
                                                                        final ProxyClient<RdsClient> proxyClient,
                                                                        final ProgressEvent<ResourceModel, CallbackContext> progress) {
        return proxy.initiate("rds::tag-dbcluster", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .request(Translator::describeDbClustersRequest)
                .call((describeDbClusterRequest, rdsClientProxyClient) -> rdsClientProxyClient.injectCredentialsAndInvokeV2(describeDbClusterRequest, rdsClientProxyClient.client()::describeDBClusters))
                .done((describeDbClusterRequest, describeDbClusterResponse, rdsClientProxyClient, resourceModel, context) -> {
                    final String arn = describeDbClusterResponse.dbClusters().stream().findFirst().get().dbClusterArn();

                    final Set<Tag> currentTags = new HashSet<>(Optional.ofNullable(resourceModel.getTags()).orElse(Collections.emptySet()));
                    final Set<Tag> existingTags = Translator.translateTagsFromSdk(rdsClientProxyClient.injectCredentialsAndInvokeV2(Translator.listTagsForResourceRequest(arn), rdsClientProxyClient.client()::listTagsForResource).tagList());
                    final Set<Tag> tagsToRemove = Sets.difference(existingTags, currentTags);
                    final Set<Tag> tagsToAdd = Sets.difference(currentTags, existingTags);
                    rdsClientProxyClient.injectCredentialsAndInvokeV2(Translator.removeTagsFromResourceRequest(arn, tagsToRemove), rdsClientProxyClient.client()::removeTagsFromResource);
                    rdsClientProxyClient.injectCredentialsAndInvokeV2(Translator.addTagsToResourceRequest(arn, tagsToAdd), rdsClientProxyClient.client()::addTagsToResource);
                    return ProgressEvent.defaultInProgressHandler(context, NO_CALLBACK_DELAY, resourceModel);
                });
    }
}
