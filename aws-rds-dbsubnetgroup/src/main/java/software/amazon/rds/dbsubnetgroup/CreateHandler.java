package software.amazon.rds.dbsubnetgroup;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.resource.IdentifierUtils;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.Tagging;

public class CreateHandler extends BaseHandlerStd {
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger) {

        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                .then(progress -> {
                    final ResourceModel model = progress.getResourceModel();
                    if (StringUtils.isNullOrEmpty(model.getDBSubnetGroupName()))
                        model.setDBSubnetGroupName(IdentifierUtils.generateResourceIdentifier(request.getLogicalResourceIdentifier(), request.getClientRequestToken(), DB_SUBNET_GROUP_NAME_LENGTH).toLowerCase());
                    return ProgressEvent.progress(model, progress.getCallbackContext());
                })
                .then(progress -> proxy.initiate("rds::create-dbsubnet-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                        .translateToServiceRequest((resourceModel) -> Translator.createDbSubnetGroupRequest(
                                resourceModel,
                                Tagging.mergeTags(request.getSystemTags(), request.getDesiredResourceTags())))
                        .backoffDelay(CONSTANT)
                        .makeServiceCall((createDbSubnetGroupRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(createDbSubnetGroupRequest, proxyInvocation.client()::createDBSubnetGroup))
                        .stabilize(((createDbSubnetGroupRequest, createDbSubnetGroupResponse, proxyInvocation, resourceModel, context) -> isStabilized(resourceModel, proxyInvocation)))
                        .handleError((awsRequest, exception, client, resourceModel, context) -> Commons.handleException(
                                ProgressEvent.progress(resourceModel, context),
                                exception,
                                DEFAULT_DB_SUBNET_GROUP_ERROR_RULE_SET))
                        .progress())
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }
}
