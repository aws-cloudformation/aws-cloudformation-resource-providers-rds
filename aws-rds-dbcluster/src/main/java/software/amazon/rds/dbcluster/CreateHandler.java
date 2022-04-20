package software.amazon.rds.dbcluster;

import java.time.Duration;
import java.util.HashSet;
import java.util.Optional;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.cloudformation.resource.IdentifierUtils;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;

public class CreateHandler extends BaseHandlerStd {

    public CreateHandler() {
        this(HandlerConfig.builder()
                .probingEnabled(true)
                .backoff(Constant.of().delay(Duration.ofSeconds(30)).timeout(Duration.ofMinutes(180)).build())
                .build());
    }

    public CreateHandler(final HandlerConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger
    ) {
        ResourceModel model = ModelAdapter.setDefaults(request.getDesiredResourceState());
        if (StringUtils.isNullOrEmpty(model.getDBClusterIdentifier())) {
            model.setDBClusterIdentifier(
                    IdentifierUtils.generateResourceIdentifier(
                            Optional.ofNullable(request.getStackId()).orElse(STACK_NAME),
                            Optional.ofNullable(request.getLogicalResourceIdentifier()).orElse(RESOURCE_IDENTIFIER),
                            request.getClientRequestToken(),
                            RESOURCE_ID_MAX_LENGTH
                    ).toLowerCase()
            );
        }

        final Tagging.TagSet systemTags = Tagging.TagSet.builder()
                .systemTags(Tagging.translateTagsToSdk(request.getSystemTags()))
                .build();

        final Tagging.TagSet extraTags = Tagging.TagSet.builder()
                .stackTags(Tagging.translateTagsToSdk(request.getDesiredResourceTags()))
                .resourceTags(new HashSet<>(Translator.translateTagsToSdk(request.getDesiredResourceState().getTags())))
                .build();

        return ProgressEvent.progress(model, callbackContext)
                .then(progress -> {
                    if (isRestoreToPointInTime(model)) {
                        return restoreDbClusterToPointInTime(proxy, proxyClient, progress, systemTags);
                    } else if (isRestoreFromSnapshot(model)) {
                        return restoreDbClusterFromSnapshot(proxy, proxyClient, progress, systemTags);
                    }
                    return createDbCluster(proxy, proxyClient, progress, systemTags);
                })
                .then(progress -> updateTags(proxy, proxyClient, progress, Tagging.TagSet.emptySet(), extraTags))
                .then(progress -> {
                    if (shouldUpdateAfterCreate(progress.getResourceModel())) {
                        return Commons.execOnce(
                                progress,
                                () -> modifyDBCluster(proxy, proxyClient, progress),
                                CallbackContext::isModified,
                                CallbackContext::setModified
                        );
                    }
                    return progress;
                })
                .then(progress -> addAssociatedRoles(proxy, proxyClient, progress, progress.getResourceModel().getAssociatedRoles()))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private ProgressEvent<ResourceModel, CallbackContext> createDbCluster(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet tagSet
    ) {
        return proxy.initiate("rds::create-dbcluster", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(model -> Translator.createDbClusterRequest(model, tagSet))
                .backoffDelay(config.getBackoff())
                .makeServiceCall((dbClusterRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        dbClusterRequest,
                        proxyInvocation.client()::createDBCluster
                ))
                .stabilize((modifyRequest, modifyResponse, proxyInvocation, model, context) -> {
                    return isDBClusterStabilized(proxyInvocation, model, DBClusterStatus.Available);
                })
                .handleError((request, exception, client, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        DEFAULT_DB_CLUSTER_ERROR_RULE_SET
                ))
                .progress();
    }

    private ProgressEvent<ResourceModel, CallbackContext> restoreDbClusterToPointInTime(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet tagSet
    ) {
        return proxy.initiate("rds::restore-dbcluster-to-point-in-time", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(model -> Translator.restoreDbClusterToPointInTimeRequest(model, tagSet))
                .backoffDelay(config.getBackoff())
                .makeServiceCall((dbClusterRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        dbClusterRequest,
                        proxyInvocation.client()::restoreDBClusterToPointInTime
                ))
                .stabilize((modifyRequest, modifyResponse, proxyInvocation, model, context) -> {
                    return isDBClusterStabilized(proxyInvocation, model, DBClusterStatus.Available);
                })
                .handleError((request, exception, client, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        DEFAULT_DB_CLUSTER_ERROR_RULE_SET
                ))
                .progress();
    }

    private ProgressEvent<ResourceModel, CallbackContext> restoreDbClusterFromSnapshot(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet tagSet
    ) {
        return proxy.initiate("rds::restore-dbcluster-from-snapshot", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(model -> Translator.restoreDbClusterFromSnapshotRequest(model, tagSet))
                .backoffDelay(config.getBackoff())
                .makeServiceCall((dbClusterRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        dbClusterRequest,
                        proxyInvocation.client()::restoreDBClusterFromSnapshot
                ))
                .stabilize((modifyRequest, modifyResponse, proxyInvocation, model, context) -> {
                    return isDBClusterStabilized(proxyInvocation, model, DBClusterStatus.Available);
                })
                .handleError((request, exception, client, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        DEFAULT_DB_CLUSTER_ERROR_RULE_SET
                ))
                .progress();
    }

    protected ProgressEvent<ResourceModel, CallbackContext> modifyDBCluster(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        return proxy.initiate("rds::modify-dbcluster", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(Translator::modifyDbClusterRequest)
                .backoffDelay(config.getBackoff())
                .makeServiceCall((dbClusterModifyRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        dbClusterModifyRequest,
                        proxyInvocation.client()::modifyDBCluster
                ))
                .stabilize((modifyRequest, modifyResponse, proxyInvocation, model, context) -> {
                    return isDBClusterStabilized(proxyInvocation, model, DBClusterStatus.Available);
                })
                .handleError((createRequest, exception, client, resourceModel, callbackCtxt) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, callbackCtxt),
                        exception,
                        DEFAULT_DB_CLUSTER_ERROR_RULE_SET
                ))
                .progress();
    }

    private boolean isRestoreToPointInTime(final ResourceModel model) {
        return StringUtils.hasValue(model.getSourceDBClusterIdentifier());
    }

    private boolean isRestoreFromSnapshot(final ResourceModel model) {
        return StringUtils.hasValue(model.getSnapshotIdentifier());
    }

    private boolean shouldUpdateAfterCreate(final ResourceModel model) {
        return isRestoreFromSnapshot(model);
    }
}
