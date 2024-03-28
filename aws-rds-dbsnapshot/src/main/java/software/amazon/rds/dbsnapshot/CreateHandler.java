package software.amazon.rds.dbsnapshot;

import java.util.HashSet;

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

public class CreateHandler extends BaseHandlerStd {

    private final static IdentifierFactory snapshotNameFactory = new IdentifierFactory(
            STACK_NAME,
            RESOURCE_IDENTIFIER,
            MAX_LENGTH_DB_SNAPSHOT
    );

    public CreateHandler() {
        this(HandlerConfig.builder().build());
    }

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

        return ProgressEvent.progress(model, callbackContext)
                .then(progress -> setDbSnapshotNameIfEmpty(request, progress))
                .then(progress -> safeCreateDbSnapshot(proxy, proxyClient, progress, allTags))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private ProgressEvent<ResourceModel, CallbackContext> safeCreateDbSnapshot(final AmazonWebServicesClientProxy proxy,
                                                                               final ProxyClient<RdsClient> proxyClient,
                                                                               final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                               final Tagging.TagSet allTags) {
        return Tagging.safeCreate(proxy, proxyClient, this::createDbSnapshot, progress, allTags)
                .then(p -> Commons.execOnce(p, () -> {
                    final Tagging.TagSet extraTags = Tagging.TagSet.builder()
                            .stackTags(allTags.getStackTags())
                            .resourceTags(allTags.getResourceTags())
                            .build();
                    return updateTags(proxy, proxyClient, p, Tagging.TagSet.emptySet(), extraTags);
                }, CallbackContext::isAddTagsComplete, CallbackContext::setAddTagsComplete));
    }

    private ProgressEvent<ResourceModel, CallbackContext> createDbSnapshot(final AmazonWebServicesClientProxy proxy,
                                                                           final ProxyClient<RdsClient> proxyClient,
                                                                           final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                           final Tagging.TagSet tags) {
        return proxy.initiate("rds::create-db-snapshot", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest((resourceModel) -> Translator.createDbSnapshotRequest(resourceModel, tags))
                .makeServiceCall((createDbSnapshotRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(createDbSnapshotRequest, proxyInvocation.client()::createDBSnapshot))
                .stabilize((createDbSnapshotRequest, createDbSnapshotResponse, proxyInvocation, resourceModel, context) ->
                        isStabilized(resourceModel, proxyInvocation))
                .handleError((createRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DEFAULT_DB_SNAPSHOT_ERROR_RULE_SET))
                .done((createRequest, createResponse, proxyInvocation, resourceModel, context) -> {
                    resourceModel.setDBSnapshotArn(createResponse.dbSnapshot().dbSnapshotArn());
                    return ProgressEvent.progress(resourceModel, context);
                });
    }

    private ProgressEvent<ResourceModel, CallbackContext> setDbSnapshotNameIfEmpty(final ResourceHandlerRequest<ResourceModel> request,
                                                                                   final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        ResourceModel model = progress.getResourceModel();
        if (StringUtils.isNullOrEmpty(model.getDBSnapshotIdentifier())) {
            model.setDBSnapshotIdentifier(snapshotNameFactory.newIdentifier()
                    .withStackId(request.getStackId())
                    .withResourceId(request.getLogicalResourceIdentifier())
                    .withRequestToken(request.getClientRequestToken())
                    .toString());
        }
        return progress;
    }
}
