package software.amazon.rds.dbsubnetgroup;

import java.util.Map;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.Tagging;

public class UpdateHandler extends BaseHandlerStd {

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger) {

        final Map<String, String> previousTags = Tagging.mergeTags(
                request.getPreviousSystemTags(),
                request.getPreviousResourceTags()
        );
        final Map<String, String> desiredTags = Tagging.mergeTags(
                request.getSystemTags(),
                request.getDesiredResourceTags()
        );

        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                .then(progress -> proxy.initiate("rds::update-dbsubnet-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                        .translateToServiceRequest(Translator::modifyDbSubnetGroupRequest)
                        .backoffDelay(CONSTANT)
                        .makeServiceCall((modifyDbSubnetGroupRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(modifyDbSubnetGroupRequest, proxyInvocation.client()::modifyDBSubnetGroup))
                        .stabilize((modifyDbSubnetGroupRequest, modifyDbSubnetGroupResponse, proxyInvocation, resourceModel, context) -> isStabilized(resourceModel, proxyInvocation))
                        .handleError((awsRequest, exception, client, resourceModel, context) -> Commons.handleException(
                                ProgressEvent.progress(resourceModel, context),
                                exception,
                                DEFAULT_DB_SUBNET_GROUP_ERROR_RULE_SET))
                        .progress()
                )
                .then(progress -> tagResource(proxy, proxyClient, progress, previousTags, desiredTags))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }
}
