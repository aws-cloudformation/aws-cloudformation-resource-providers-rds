package software.amazon.rds.dbcluster;

import static software.amazon.rds.dbcluster.ModelAdapter.setDefaults;

import java.time.Instant;
import java.util.HashSet;
import java.util.Objects;
import java.util.function.Function;

import org.apache.commons.lang3.BooleanUtils;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.SourceType;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.Events;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Probing;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.request.ValidatedRequest;
import software.amazon.rds.common.util.WaiterHelper;
import software.amazon.rds.dbcluster.util.ImmutabilityHelper;
import software.amazon.rds.dbcluster.util.ResourceModelHelper;

public class UpdateHandler extends BaseHandlerStd {
    static final int AURORA_SERVERLESS_V2_MAX_WAIT_SECONDS = 300;
    static final int AURORA_SERVERLESS_V2_POLL_SECONDS = 10;

    public UpdateHandler() {
        this(DB_CLUSTER_HANDLER_CONFIG_36H);
    }

    public UpdateHandler(final HandlerConfig config) {
        super(config);
    }

    final String handlerOperation = "UPDATE";

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ValidatedRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProxyClient<Ec2Client> ec2ProxyClient
    ) {
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

        final ResourceModel previousResourceState = setDefaults(request.getPreviousResourceState());
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

        if (!Objects.equals(request.getDesiredResourceState().getEngineLifecycleSupport(),
            request.getPreviousResourceState().getEngineLifecycleSupport()) &&
            !request.getRollback()) {
            throw new CfnInvalidRequestException("EngineLifecycleSupport cannot be modified.");
        }

        return ProgressEvent.progress(desiredResourceState, callbackContext)
                .then(progress -> {
                    if (shouldRemoveFromGlobalCluster(request.getPreviousResourceState(), request.getDesiredResourceState())) {
                        progress.getCallbackContext().timestampOnce(RESOURCE_UPDATED_AT, Instant.now());
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
                        () -> {
                            progress.getCallbackContext().timestampOnce(RESOURCE_UPDATED_AT, Instant.now());
                            return modifyDBCluster(proxy, rdsProxyClient, progress, previousResourceState, desiredResourceState, isRollback)
                                    .then(p -> {
                                        if (shouldUpdateHttpEndpointV2(previousResourceState, desiredResourceState)) {
                                            if (BooleanUtils.isTrue(desiredResourceState.getEnableHttpEndpoint())) {
                                                return enableHttpEndpointV2(proxy, rdsProxyClient, p);
                                            } else {
                                                return disableHttpEndpointV2(proxy, rdsProxyClient, p);
                                            }
                                        }
                                        return p;
                                    });
                        },
                        CallbackContext::isModified,
                        CallbackContext::setModified))
                .then(progress -> updateAssociatedRoles(
                        proxy,
                        rdsProxyClient,
                        progress,
                        request.getPreviousResourceState().getAssociatedRoles(),
                        progress.getResourceModel().getAssociatedRoles(),
                        BooleanUtils.isTrue(request.getRollback())))
                .then(p -> Events.checkFailedEvents(
                        rdsProxyClient,
                        p.getResourceModel().getDBClusterIdentifier(),
                        SourceType.DB_CLUSTER,
                        p.getCallbackContext().getTimestamp(RESOURCE_UPDATED_AT),
                        p,
                        this::isFailureEvent,
                        requestLogger
                ))
                .then(progress -> updateTags(proxy, rdsProxyClient, progress, previousTags, desiredTags))
            .then(progress -> {
                // Required delay for Aurora Serverless V2, because when scaling capacity, it kicks off an async
                // workflow which could occur after the DBCluster has completed CFN update
                // This delay attempts to force the async workflow to complete before the DBCluster update returns
                if (ResourceModelHelper.hasServerlessV2ScalingConfigurationChanged(previousResourceState, desiredResourceState)) {
                    return awaitDBClusterStabilization(proxy, rdsProxyClient, progress, desiredResourceState, AURORA_SERVERLESS_V2_MAX_WAIT_SECONDS, AURORA_SERVERLESS_V2_POLL_SECONDS);
                }
                return progress;
            })
            .then(progress -> {
                    desiredResourceState.setTags(Translator.translateTagsFromSdk(Tagging.translateTagsToSdk(desiredTags)));
                    return Commons.reportResourceDrift(
                            desiredResourceState,
                            new ReadHandler().handleRequest(proxy, request, progress.getCallbackContext(), rdsProxyClient, ec2ProxyClient),
                            resourceTypeSchema,
                            requestLogger,
                            handlerOperation
                    );
                });
    }

    protected ProgressEvent<ResourceModel, CallbackContext> awaitDBClusterStabilization(
        final AmazonWebServicesClientProxy proxy,
        final ProxyClient<RdsClient> proxyClient,
        final ProgressEvent<ResourceModel, CallbackContext> progress,
        final ResourceModel desiredResourceState,
        final int maxDelaySeconds,
        final int pollSeconds
    ) {
        if (WaiterHelper.shouldDelay(progress.getCallbackContext(), maxDelaySeconds)) {
            return WaiterHelper.delay(progress, maxDelaySeconds, pollSeconds);
        } else {
            return proxy.initiate("rds::stabilize-db-cluster", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(Function.identity())
                .backoffDelay(config.getBackoff())
                .makeServiceCall(NOOP_CALL)
                .stabilize((modifyRequest, modifyResponse, proxyInvocation, model, context) ->
                    Probing.withProbing(
                        context.getProbingContext(),
                        "db-cluster-stabilized",
                        3,
                        () -> isDBClusterStabilized(proxyClient, desiredResourceState))
                )
                .handleError((createRequest, exception, client, resourceModel, callbackCtx) -> Commons.handleException(
                    ProgressEvent.progress(resourceModel, callbackCtx),
                    exception,
                    DEFAULT_DB_CLUSTER_ERROR_RULE_SET,
                    requestLogger
                ))
                .progress();
        }
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
                        Probing.withProbing(
                                context.getProbingContext(),
                                "db-cluster-stabilized",
                                3,
                                () -> isDBClusterStabilized(proxyClient, desiredResourceState))
                )
                .handleError((createRequest, exception, client, resourceModel, callbackCtx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, callbackCtx),
                        exception,
                        DEFAULT_DB_CLUSTER_ERROR_RULE_SET,
                        requestLogger
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
}
