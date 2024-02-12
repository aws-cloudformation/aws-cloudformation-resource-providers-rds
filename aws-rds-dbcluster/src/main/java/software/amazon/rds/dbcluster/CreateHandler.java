package software.amazon.rds.dbcluster;

import java.time.Instant;
import java.util.HashSet;

import org.apache.commons.lang3.BooleanUtils;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.SourceType;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.Events;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.request.RequestValidationException;
import software.amazon.rds.common.request.ValidatedRequest;
import software.amazon.rds.common.request.Validations;
import software.amazon.rds.common.util.IdentifierFactory;

public class CreateHandler extends BaseHandlerStd {

    private static final IdentifierFactory dbClusterIdentifierFactory = new IdentifierFactory(
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

    @Override
    protected void validateRequest(final ResourceHandlerRequest<ResourceModel> request) throws RequestValidationException {
        super.validateRequest(request);
        Validations.validateTimestamp(request.getDesiredResourceState().getRestoreToTime());
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ValidatedRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProxyClient<Ec2Client> ec2ProxyClient
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
                        return Tagging.createWithTaggingFallback(proxy, rdsProxyClient, this::restoreDbClusterToPointInTime, progress, allTags);
                    } else if (isRestoreFromSnapshot(model)) {
                        return Tagging.createWithTaggingFallback(proxy, rdsProxyClient, this::restoreDbClusterFromSnapshot, progress, allTags);
                    }
                    return Tagging.createWithTaggingFallback(proxy, rdsProxyClient, this::createDbCluster, progress, allTags);
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
                                () -> {
                                    progress.getCallbackContext().timestampOnce(RESOURCE_UPDATED_AT, Instant.now());
                                    return modifyDBCluster(proxy, rdsProxyClient, progress)
                                            .then(p -> {
                                                if (shouldEnableHttpEndpointV2AfterCreate(progress.getResourceModel())) {
                                                    return enableHttpEndpointV2(proxy, rdsProxyClient, progress);
                                                }
                                                return p;
                                            })
                                            .then(p -> Events.checkFailedEvents(
                                                    rdsProxyClient,
                                                    p.getResourceModel().getDBClusterIdentifier(),
                                                    SourceType.DB_CLUSTER,
                                                    p.getCallbackContext().getTimestamp(RESOURCE_UPDATED_AT),
                                                    p,
                                                    this::isFailureEvent,
                                                    requestLogger
                                            ));
                                },
                                CallbackContext::isModified,
                                CallbackContext::setModified
                        );
                    }
                    return progress;
                })
                .then(progress -> addAssociatedRoles(proxy, rdsProxyClient, progress, progress.getResourceModel().getAssociatedRoles(), false))
                .then(progress -> {
                    model.setTags(Translator.translateTagsFromSdk(Tagging.translateTagsToSdk(allTags)));
                    return Commons.reportResourceDrift(
                            model,
                            new ReadHandler().handleRequest(proxy, request, progress.getCallbackContext(), rdsProxyClient, ec2ProxyClient),
                            resourceTypeSchema,
                            requestLogger
                    );
                });
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
                        DEFAULT_DB_CLUSTER_ERROR_RULE_SET,
                        requestLogger
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
                        DEFAULT_DB_CLUSTER_ERROR_RULE_SET,
                        requestLogger
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
                        DEFAULT_DB_CLUSTER_ERROR_RULE_SET,
                        requestLogger
                ))
                .progress();
    }

    protected ProgressEvent<ResourceModel, CallbackContext> modifyDBCluster(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        return proxy.initiate("rds::modify-dbcluster", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(Translator::modifyDbClusterAfterCreateRequest)
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
                        DEFAULT_DB_CLUSTER_ERROR_RULE_SET,
                        requestLogger
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
        return isRestoreFromSnapshot(model) || isRestoreToPointInTime(model);
    }

    private boolean shouldEnableHttpEndpointV2AfterCreate(final ResourceModel model) {
        return BooleanUtils.isTrue(model.getEnableHttpEndpoint()) && !EngineMode.Serverless.equals(EngineMode.fromString(model.getEngineMode()))  ;
    }
}
