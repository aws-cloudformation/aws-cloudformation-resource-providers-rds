package software.amazon.rds.dbcluster;

import static software.amazon.rds.dbcluster.ModelAdapter.setDefaults;

import java.time.Instant;
import java.util.HashSet;

import org.apache.commons.lang3.BooleanUtils;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.SourceType;
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
                    desiredResourceState.setTags(Translator.translateTagsFromSdk(Tagging.translateTagsToSdk(desiredTags)));
                    return Commons.reportResourceDrift(
                            desiredResourceState,
                            new ReadHandler().handleRequest(proxy, request, progress.getCallbackContext(), rdsProxyClient, ec2ProxyClient),
                            resourceTypeSchema,
                            requestLogger
                    );
                });
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
