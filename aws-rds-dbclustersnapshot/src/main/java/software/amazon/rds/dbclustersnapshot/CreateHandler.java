package software.amazon.rds.dbclustersnapshot;

import com.amazonaws.util.StringUtils;
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

import java.util.HashSet;


public class CreateHandler extends BaseHandlerStd {
    private final static IdentifierFactory snapshotNameFactory = new IdentifierFactory(
            STACK_NAME,
            RESOURCE_IDENTIFIER,
            MAX_LENGTH_DB_SNAPSHOT
    );


    public CreateHandler() { this(HandlerConfig.builder().build()); }

    public CreateHandler(final HandlerConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger) {
        final ResourceModel model = request.getDesiredResourceState();

        final Tagging.TagSet allTags = Tagging.TagSet.builder()
                .systemTags(Tagging.translateTagsToSdk(request.getSystemTags()))
                .stackTags(Tagging.translateTagsToSdk(request.getDesiredResourceTags()))
                .resourceTags(new HashSet<>(Translator.translateTagsToSdk(request.getDesiredResourceState().getTags())))
                .build();

        if (StringUtils.isNullOrEmpty(model.getDBClusterSnapshotIdentifier())) {
            model.setDBClusterSnapshotIdentifier(snapshotNameFactory.newIdentifier()
                    .withStackId(request.getStackId())
                    .withResourceId(request.getLogicalResourceIdentifier())
                    .withRequestToken(request.getClientRequestToken())
                    .toString()); // FIXME: Double check
        }
        return ProgressEvent.progress(model, callbackContext)
                .then(progress -> {
                    if (isCopyDBClusterSnapshot(model)) {
                        return Tagging.safeCreate(proxy, proxyClient, this::copyDBClusterSnapshot, progress, allTags);
                    }
                    return Tagging.safeCreate(proxy, proxyClient, this::createDbClusterSnapshot, progress, allTags);
                })
                .then(progress -> Commons.execOnce(progress, () -> {
                            final Tagging.TagSet extraTags = Tagging.TagSet.builder()
                                    .stackTags(Tagging.translateTagsToSdk(request.getDesiredResourceTags()))
                                    .resourceTags(new HashSet<>(Translator.translateTagsToSdk(request.getDesiredResourceState().getTags())))
                                    .build();
                            return updateTags(proxy, proxyClient, progress/*, model.getDBClusterSnapshotArn()*/, Tagging.TagSet.emptySet(), extraTags); // FIXME?
                        }, CallbackContext::isAddTagsComplete, CallbackContext::setAddTagsComplete
                ))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private boolean isCopyDBClusterSnapshot(final ResourceModel model) {
        return StringUtils.hasValue(model.getSourceDBClusterSnapshotIdentifier());
    }

    private ProgressEvent<ResourceModel, CallbackContext> createDbClusterSnapshot(final AmazonWebServicesClientProxy proxy,
                                                                                  final ProxyClient<RdsClient> proxyClient,
                                                                                  final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                                  final Tagging.TagSet tags
    ) {
        return proxy.initiate("rds::create-db-cluster-snapshot", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest((resourceModel) -> {
                    return Translator.createDbClusterSnapshotRequest(resourceModel, tags);
                })
                .makeServiceCall((createDbSnapshotRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(createDbSnapshotRequest, proxyInvocation.client()::createDBClusterSnapshot))
                .stabilize((createDbSnapshotRequest, createDbSnapshotResponse, proxyInvocation, resourceModel, context) ->
                        isStabilized(resourceModel, proxyInvocation))
                .handleError((createRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DEFAULT_DB_SNAPSHOT_ERROR_RULE_SET))
                .done((createRequest, createResponse, proxyInvocation, resourceModel, context) -> {
                    resourceModel.setDBClusterSnapshotArn(createResponse.dbClusterSnapshot().dbClusterSnapshotArn());
                    return ProgressEvent.progress(resourceModel, context);
                });
    }

    private ProgressEvent<ResourceModel, CallbackContext> copyDBClusterSnapshot(final AmazonWebServicesClientProxy proxy,
                                                                                  final ProxyClient<RdsClient> proxyClient,
                                                                                  final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                                  final Tagging.TagSet tags
    ) {
        return proxy.initiate("rds::copy-db-cluster-snapshot", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest((resourceModel) -> {
                    return Translator.copyDbClusterSnapshotRequest(resourceModel, tags);
                })
                .makeServiceCall((copyDbSnapshotRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(copyDbSnapshotRequest, proxyInvocation.client()::copyDBClusterSnapshot))
                .stabilize((copyDbSnapshotRequest, copyDbSnapshotResponse, proxyInvocation, resourceModel, context) ->
                        isStabilized(resourceModel, proxyInvocation))
                .handleError((createRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DEFAULT_DB_SNAPSHOT_ERROR_RULE_SET))
                .done((createRequest, createResponse, proxyInvocation, resourceModel, context) -> {
                    resourceModel.setDBClusterSnapshotArn(createResponse.dbClusterSnapshot().dbClusterSnapshotArn());
                    return ProgressEvent.progress(resourceModel, context);
                });
    }
}
