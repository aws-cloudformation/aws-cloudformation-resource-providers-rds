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

    private final static IdentifierFactory dbSnapshotIdentifierFactory = new IdentifierFactory(
            STACK_NAME,
            RESOURCE_IDENTIFIER,
            RESOURCE_ID_MAX_LENGTH
    );

    public CreateHandler() {
        this(DEFAULT_HANDLER_CONFIG);
    }

    public CreateHandler(final HandlerConfig config) {
        super(config);
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> client,
            final Logger logger
    ) {
        final ResourceModel resourceModel = request.getDesiredResourceState();

        if (StringUtils.isNullOrEmpty(resourceModel.getDBSnapshotIdentifier())) {
            resourceModel.setDBSnapshotIdentifier(dbSnapshotIdentifierFactory.newIdentifier()
                    .withStackId(request.getStackId())
                    .withResourceId(request.getLogicalResourceIdentifier())
                    .withRequestToken(request.getClientRequestToken())
                    .toString());
        }

        final Tagging.TagSet tagSet = Tagging.TagSet.builder()
                .systemTags(Tagging.translateTagsToSdk(request.getSystemTags()))
                .stackTags(Tagging.translateTagsToSdk(request.getDesiredResourceTags()))
                .resourceTags(new HashSet<>(Translator.translateTagsToSdk(resourceModel.getTags())))
                .build();

        return ProgressEvent.progress(resourceModel, callbackContext)
                .then(progress -> {
                    if (isCopyDBSnapshot(progress.getResourceModel())) {
                        return copyDBSnapshot(proxy, client, progress, tagSet);
                    }
                    return createDBSnapshot(proxy, client, progress, tagSet)
                            .then(progressEvent -> {
                                if (shouldUpdateAfterCreate(progress.getResourceModel())) {
                                    return updateDBSnapshot(proxy, client, progressEvent);
                                }
                                return progress;
                            });
                })
                .then(progress -> new ReadHandler(config).handleRequest(proxy, request, progress.getCallbackContext(), client, logger));
    }

    private boolean isCopyDBSnapshot(final ResourceModel model) {
        return StringUtils.hasValue(model.getSourceDBSnapshotIdentifier());
    }

    private ProgressEvent<ResourceModel, CallbackContext> createDBSnapshot(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> client,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet tagSet
    ) {
        return proxy.initiate("rds::create-db-snapshot", client, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(model -> Translator.createDBSnapshotRequest(model, tagSet))
                .makeServiceCall((createRequest, proxyClient) -> proxyClient.injectCredentialsAndInvokeV2(
                        createRequest,
                        proxyClient.client()::createDBSnapshot
                ))
                .stabilize((createRequest, createResponse, proxyClient, model, context) -> isDBSnapshotStabilized(proxyClient, model))
                .handleError((createRequest, exception, proxyClient, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        DEFAULT_DB_SNAPSHOT_ERROR_RULE_SET
                ))
                .progress();
    }

    private ProgressEvent<ResourceModel, CallbackContext> copyDBSnapshot(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> client,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet tagSet
    ) {
        return proxy.initiate("rds::copy-db-snapshot", client, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(model -> Translator.copyDBSnapshotRequest(model, tagSet))
                .makeServiceCall((copyRequest, proxyClient) -> proxyClient.injectCredentialsAndInvokeV2(
                        copyRequest,
                        proxyClient.client()::copyDBSnapshot
                ))
                .stabilize((copyRequest, copyResponse, proxyClient, model, context) -> isDBSnapshotStabilized(proxyClient, model))
                .handleError((copyRequest, exception, proxyClient, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        DEFAULT_DB_SNAPSHOT_ERROR_RULE_SET
                ))
                .progress();
    }
}
