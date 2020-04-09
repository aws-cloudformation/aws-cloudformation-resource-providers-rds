package software.amazon.rds.optiongroup;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.resource.IdentifierUtils;

public class CreateHandler extends BaseHandlerStd {
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<RdsClient> proxyClient,
        final Logger logger) {
        ResourceModel model = request.getDesiredResourceState();
        // setting up primary id if not there yet
        if (StringUtils.isNullOrEmpty(model.getId()))
            model.setId(IdentifierUtils.generateResourceIdentifier(
                request.getLogicalResourceIdentifier(),
                request.getClientRequestToken(), 255).toLowerCase());
        return  proxy.initiate("rds::create-option-group", proxyClient, model, callbackContext)
            .request((resourceModel) -> Translator.createOptionGroupRequest(resourceModel, request.getDesiredResourceTags()))
            .call((createOptionGroupRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(createOptionGroupRequest, proxyInvocation.client()::createOptionGroup))
            .progress()
            .then(progress -> updateOptionGroup(proxy, proxyClient, progress))
            .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }
}
