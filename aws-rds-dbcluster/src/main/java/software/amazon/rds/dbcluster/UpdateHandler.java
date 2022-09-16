package software.amazon.rds.dbcluster;

import static software.amazon.rds.dbcluster.ModelAdapter.setDefaults;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.commons.lang3.BooleanUtils;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBClusterMember;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.dbcluster.util.ImmutabilityHelper;

public class UpdateHandler extends BaseHandlerStd {

    public UpdateHandler() {
        this(DB_CLUSTER_HANDLER_CONFIG_36H);
    }

    public UpdateHandler(final HandlerConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProxyClient<Ec2Client> ec2ProxyClient, final Logger logger) {
        final Tagging.TagSet previousTags = Tagging.TagSet.builder()
                .systemTags(Tagging.translateTagsToSdk(request.getPreviousSystemTags()))
                .stackTags(Tagging.translateTagsToSdk(request.getPreviousResourceTags()))
                .resourceTags(new HashSet<>(Translator.translateTagsToSdk(request.getPreviousResourceState().getTags())))
                .build();

        final Tagging.TagSet desiredTags = Tagging.TagSet.builder()
                .systemTags(Tagging.translateTagsToSdk(request.getSystemTags()))
                .stackTags(Tagging.translateTagsToSdk(request.getDesiredResourceTags()))
                .resourceTags(new HashSet<>(Translator.translateTagsToSdk(request.getDesiredResourceState().getTags())))
                .build();

        final ResourceModel previousResourceState = request.getPreviousResourceState();
        final ResourceModel desiredResourceState = setDefaults(request.getDesiredResourceState());
        final boolean isRollback = BooleanUtils.isTrue(request.getRollback());

        if (!ImmutabilityHelper.isChangeMutable(previousResourceState, desiredResourceState)) {
            return ProgressEvent.failed(
                    desiredResourceState,
                    callbackContext,
                    HandlerErrorCode.NotUpdatable,
                    "Resource is immutable"
            );
        }

        return ProgressEvent.progress(desiredResourceState, callbackContext)
                .then(progress -> {
                    if (shouldRemoveFromGlobalCluster(request.getPreviousResourceState(), request.getDesiredResourceState())) {
                        return removeFromGlobalCluster(proxy, rdsProxyClient, progress, request.getPreviousResourceState().getGlobalClusterIdentifier());
                    }
                    return progress;
                })
                .then(progress -> {
                    if (shouldSetDefaultVpcSecurityGroupIds(previousResourceState, desiredResourceState)) {
                        return setDefaultVpcSecurityGroupIds(proxy, rdsProxyClient, ec2ProxyClient, progress);
                    }
                    return progress;
                })
                .then(progress -> Commons.execOnce(
                        progress,
                        () -> modifyDBCluster(proxy, rdsProxyClient, progress, previousResourceState, desiredResourceState, isRollback),
                        CallbackContext::isModified,
                        CallbackContext::setModified))
                .then(progress -> Commons.execOnce(
                        progress,
                        () -> {
                            if (shouldRebootCluster(previousResourceState, desiredResourceState, isRollback)) {
                                return rebootCluster(proxy, rdsProxyClient, progress, desiredResourceState);
                            }
                            return progress;
                        },
                        CallbackContext::isRebooted,
                        CallbackContext::setRebooted
                ))
                .then(progress -> removeAssociatedRoles(proxy, rdsProxyClient, progress, setDefaults(request.getPreviousResourceState()).getAssociatedRoles()))
                .then(progress -> addAssociatedRoles(proxy, rdsProxyClient, progress, progress.getResourceModel().getAssociatedRoles()))
                .then(progress -> updateTags(proxy, rdsProxyClient, progress, previousTags, desiredTags))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, rdsProxyClient, ec2ProxyClient, logger));
    }

    protected ProgressEvent<ResourceModel, CallbackContext> modifyDBCluster(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final ResourceModel previousResourceState,
            final ResourceModel desiredResourceState,
            final boolean isRollback
    ) {
        return proxy.initiate("rds::modify-dbcluster", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(model -> Translator.modifyDbClusterRequest(previousResourceState, model, isRollback))
                .backoffDelay(config.getBackoff())
                .makeServiceCall((dbClusterModifyRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        dbClusterModifyRequest,
                        proxyInvocation.client()::modifyDBCluster
                ))
                .stabilize((modifyRequest, modifyResponse, proxyInvocation, model, context) ->
                        withProbing(context,
                                "db-cluster-stabilized",
                                3,
                                () -> isDBClusterStabilized(proxyClient, desiredResourceState))
                )
                .handleError((createRequest, exception, client, resourceModel, callbackCtx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, callbackCtx),
                        exception,
                        DEFAULT_DB_CLUSTER_ERROR_RULE_SET
                ))
                .progress();
    }

    private boolean shouldRemoveFromGlobalCluster(
            final ResourceModel previousResourceState,
            final ResourceModel desiredResourceState
    ) {
        return StringUtils.hasValue(previousResourceState.getGlobalClusterIdentifier()) &&
                StringUtils.isNullOrEmpty(desiredResourceState.getGlobalClusterIdentifier());
    }

    protected ProgressEvent<ResourceModel, CallbackContext> rebootCluster(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final ResourceModel resourceModel
    ) {
        DBCluster cluster;
        try {
            cluster = fetchDBCluster(proxyClient, resourceModel);
        } catch (Exception ex) {
            return Commons.handleException(progress, ex, DEFAULT_DB_CLUSTER_ERROR_RULE_SET);
        }

        ProgressEvent<ResourceModel, CallbackContext> result = progress;

        for (final DBClusterMember dbClusterMember: cluster.dbClusterMembers()) {
            final ProgressEvent<ResourceModel, CallbackContext> p = proxy.initiate("rds::reboot-dbcluster-member-" + dbClusterMember.dbInstanceIdentifier(),
                            proxyClient,
                            progress.getResourceModel(),
                            progress.getCallbackContext())
                    .translateToServiceRequest(model -> Translator.rebootDbInstanceRequest(dbClusterMember.dbInstanceIdentifier()))
                    .backoffDelay(config.getBackoff())
                    .makeServiceCall((rebootDbInstanceRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                            rebootDbInstanceRequest,
                            proxyInvocation.client()::rebootDBInstance
                    ))
                    .stabilize((rebootDbInstanceRequest, rebootDbInstanceResponse, proxyInvocation, model, context) ->
                            isDBInstanceStabilized(proxyClient, dbClusterMember.dbInstanceIdentifier(), model))
                    .handleError((createRequest, exception, client, model, callbackCtx) -> Commons.handleException(
                            ProgressEvent.progress(model, callbackCtx),
                            exception,
                            DEFAULT_DB_CLUSTER_ERROR_RULE_SET
                    ))
                    .progress();

            if (p.isFailed() || p.isInProgressCallbackDelay()) {
                return p;
            }
            result = p;
        }
        return result;
    }

    private boolean shouldRebootCluster(final ResourceModel previousResourceState, final ResourceModel desiredResourceState, boolean isRollback) {
        return !isRollback &&
                !Objects.equals(previousResourceState.getEngineVersion(), desiredResourceState.getEngineVersion()) &&
                !Objects.equals(previousResourceState.getDBInstanceParameterGroupName(), desiredResourceState.getDBInstanceParameterGroupName());
    }

    private boolean isDBInstanceStabilized(final ProxyClient<RdsClient> client, final String dbInstanceIdentifier, final ResourceModel resourceModel) {
        final DBInstance dbInstance = fetchDBInstance(client, dbInstanceIdentifier);
        final DBCluster dbCluster = fetchDBCluster(client, resourceModel);

        return "available".equals(dbInstance.dbInstanceStatus()) &&
                isParameterGroupApplied(dbInstance) &&
                isClusterParameterGroupApplied(dbCluster, dbInstance);
    }
    private boolean isParameterGroupApplied(final DBInstance dbInstance) {
        return Optional.ofNullable(dbInstance.dbParameterGroups()).orElse(Collections.emptyList()).stream()
                .allMatch(pg -> IN_SYNC_STATUS.equals(pg.parameterApplyStatus()));
    }

    private boolean isClusterParameterGroupApplied(final DBCluster dbCluster, final DBInstance dbInstance) {
        final DBClusterMember member = dbCluster.dbClusterMembers().stream()
                .filter(m -> dbInstance.dbInstanceIdentifier().equalsIgnoreCase(m.dbInstanceIdentifier()))
                .findFirst().get();
        return IN_SYNC_STATUS.equals(member.dbClusterParameterGroupStatus());
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
}
