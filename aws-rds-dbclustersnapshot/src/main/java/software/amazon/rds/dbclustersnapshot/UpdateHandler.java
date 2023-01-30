package software.amazon.rds.dbclustersnapshot;

import com.amazonaws.util.StringUtils;
import com.google.common.base.Objects;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;

import java.util.HashSet;

public class UpdateHandler extends BaseHandlerStd { // FIXME: "rds:DescribeDBClusters" to be added to FAS
    public UpdateHandler() { this(HandlerConfig.builder().build()); }

    public UpdateHandler(final HandlerConfig config) { super(config); }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger) {
        ResourceModel a = request.getPreviousResourceState();
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

        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                // FIXME: Add the update for modifyDBClusterSnapshotAttribute
                .then(progress -> assertChangeIsMutable(progress, callbackContext, request.getPreviousResourceState(), request.getDesiredResourceState()))
                .then(progress -> updateTags(proxy, proxyClient, progress, previousTags, desiredTags))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private boolean isSourceClusterSnapshotIdentifierMutable(final ResourceModel previous, final ResourceModel desired) {
        return Objects.equal(previous.getSourceDBClusterSnapshotIdentifier(), desired.getSourceDBClusterSnapshotIdentifier()) ||
                StringUtils.isNullOrEmpty(desired.getSourceDBClusterSnapshotIdentifier());
    }

    private final ProgressEvent<ResourceModel, CallbackContext> assertChangeIsMutable(
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            CallbackContext callbackContext,
            ResourceModel previousResourceState,
            ResourceModel desiredResourceState
    ) {
        if (isSourceClusterSnapshotIdentifierMutable(previousResourceState, desiredResourceState)) {
            return progress;
        }
        return ProgressEvent.failed(
                desiredResourceState,
                callbackContext,
                HandlerErrorCode.NotUpdatable,
                "Resource is immutable"
        );
    }
}
