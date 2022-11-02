package software.amazon.rds.dbcluster;

import java.util.HashSet;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.util.IdentifierFactory;

public class CreateHandler extends BaseHandlerStd {

    private final static IdentifierFactory dbClusterIdentifierFactory = new IdentifierFactory(
            STACK_NAME,
            RESOURCE_IDENTIFIER,
            RESOURCE_ID_MAX_LENGTH
    );

    public CreateHandler() {
        this(DB_CLUSTER_HANDLER_CONFIG_36H);
    }

    public CreateHandler(final HandlerConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProxyClient<Ec2Client> ec2ProxyClient, final Logger logger
    ) {
        final ResourceModel model = ModelAdapter.setDefaults(request.getDesiredResourceState());

        if (StringUtils.isNullOrEmpty(model.getDBClusterIdentifier())) {
            model.setDBClusterIdentifier(dbClusterIdentifierFactory.newIdentifier()
                    .withStackId(request.getStackId())
                    .withResourceId(request.getLogicalResourceIdentifier())
                    .withRequestToken(request.getClientRequestToken())
                    .toString());
        }

        final Tagging.TagSet allTags = Tagging.TagSet.builder()
                .systemTags(Tagging.translateTagsToSdk(request.getSystemTags()))
                .stackTags(Tagging.translateTagsToSdk(request.getDesiredResourceTags()))
                .resourceTags(new HashSet<>(Translator.translateTagsToSdk(request.getDesiredResourceState().getTags())))
                .build();

        return ProgressEvent.progress(model, callbackContext)
                .then(progress -> {
                    if (isRestoreToPointInTime(model)) {
                        return Tagging.safeCreate(proxy, rdsProxyClient, this::restoreDbClusterToPointInTime, progress, allTags);
                    } else if (isRestoreFromSnapshot(model)) {
                        return Tagging.safeCreate(proxy, rdsProxyClient, this::restoreDbClusterFromSnapshot, progress, allTags);
                    }
                    return Tagging.safeCreate(proxy, rdsProxyClient, this::createDbCluster, progress, allTags);
                })
                .then(progress -> Commons.execOnce(progress, () -> {
                    final Tagging.TagSet extraTags = Tagging.TagSet.builder()
                            .stackTags(allTags.getStackTags())
                            .resourceTags(allTags.getResourceTags())
                            .build();
                    return updateTags(proxy, rdsProxyClient, progress, Tagging.TagSet.emptySet(), extraTags);
                }, CallbackContext::isAddTagsComplete, CallbackContext::setAddTagsComplete))
                .then(progress -> {
                    if (shouldUpdateAfterCreate(progress.getResourceModel())) {
                        return Commons.execOnce(
                                progress,
                                () -> modifyDBCluster(proxy, rdsProxyClient, progress),
                                CallbackContext::isModified,
                                CallbackContext::setModified
                        );
                    }
                    return progress;
                })
                .then(progress -> addAssociatedRoles(proxy, rdsProxyClient, progress, progress.getResourceModel().getAssociatedRoles()))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, rdsProxyClient, ec2ProxyClient, logger));
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
                    return isDBClusterStabilized(proxyInvocation, model);
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
                    return isDBClusterStabilized(proxyInvocation, model);
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
                    return isDBClusterStabilized(proxyInvocation, model);
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
                .translateToServiceRequest(model -> Translator.modifyDbClusterRequest(model, model, false))
                .backoffDelay(config.getBackoff())
                .makeServiceCall((dbClusterModifyRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        dbClusterModifyRequest,
                        proxyInvocation.client()::modifyDBCluster
                ))
                .stabilize((modifyRequest, modifyResponse, proxyInvocation, model, context) -> {
                    return isDBClusterStabilized(proxyInvocation, model);
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
