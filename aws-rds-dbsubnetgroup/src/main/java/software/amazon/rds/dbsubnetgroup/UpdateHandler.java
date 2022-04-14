package software.amazon.rds.dbsubnetgroup;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;

public class UpdateHandler extends BaseHandlerStd {

    public UpdateHandler() {
        this(HandlerConfig.builder().build());
    }

    public UpdateHandler(final HandlerConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger
    ) {
        final Tagging.TagSet previousTags = Tagging.TagSet.builder()
                .systemTags(Tagging.translateTagsToSdk(request.getPreviousSystemTags()))
                .stackTags(Tagging.translateTagsToSdk(request.getPreviousResourceTags()))
                .resourceTags(new LinkedHashSet<>(Translator.translateTagsToSdk(request.getPreviousResourceState().getTags())))
                .build();

        final Tagging.TagSet desiredTags = Tagging.TagSet.builder()
                .systemTags(Tagging.translateTagsToSdk(request.getSystemTags()))
                .stackTags(Tagging.translateTagsToSdk(request.getDesiredResourceTags()))
                .resourceTags(new LinkedHashSet<>(Translator.translateTagsToSdk(request.getDesiredResourceState().getTags())))
                .build();

        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                .then(progress -> modifyDBSubnetGroup(proxy, proxyClient, progress))
                .then(progress -> updateTags(proxyClient, progress, previousTags, desiredTags))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private ProgressEvent<ResourceModel, CallbackContext> modifyDBSubnetGroup(final AmazonWebServicesClientProxy proxy,
                                                                              final ProxyClient<RdsClient> proxyClient,
                                                                              final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        return proxy.initiate("rds::update-dbsubnet-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(Translator::modifyDbSubnetGroupRequest)
                .backoffDelay(config.getBackoff())
                .makeServiceCall((modifyDbSubnetGroupRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(modifyDbSubnetGroupRequest, proxyInvocation.client()::modifyDBSubnetGroup))
                .stabilize((modifyDbSubnetGroupRequest, modifyDbSubnetGroupResponse, proxyInvocation, resourceModel, context) -> isStabilized(resourceModel, proxyInvocation))
                .handleError((awsRequest, exception, client, resourceModel, context) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, context),
                        exception,
                        DEFAULT_DB_SUBNET_GROUP_ERROR_RULE_SET))
                .done((subnetGroupRequest, subnetGroupResponse, proxyInvocation, resourceModel, context) -> {
                    context.setDbSubnetGroupArn(subnetGroupResponse.dbSubnetGroup().dbSubnetGroupArn());
                    return ProgressEvent.progress(resourceModel, context);
                });
    }
}
