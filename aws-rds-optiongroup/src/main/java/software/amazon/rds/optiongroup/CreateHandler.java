package software.amazon.rds.optiongroup;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.OptionGroupAlreadyExistsException;
import software.amazon.cloudformation.proxy.*;

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
                    return ProgressEvent.progress(model, progress.getCallbackContext());
                })
                .then(progress -> {
                    return proxy.initiate("rds::create-option-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                            .translateToServiceRequest(resourceModel -> {
                                return Translator.createOptionGroupRequest(resourceModel, request.getDesiredResourceTags());
                            })
                            .backoffDelay(CONSTANT)
                            .makeServiceCall((createOptionGroupRequest, proxyInvocation) -> {
                                return proxyInvocation.injectCredentialsAndInvokeV2(createOptionGroupRequest, proxyInvocation.client()::createOptionGroup);
                            })
                            .handleError((createOptionGroupRequest, exception, client, resourceModel, ctx) -> {
                                if (exception instanceof OptionGroupAlreadyExistsException) {
                                    return ProgressEvent.defaultFailureHandler(exception, HandlerErrorCode.AlreadyExists);
                                }
                                throw exception;
                            })
                            .progress();
                })
                .then(progress -> {
                    return new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger);
                });
    }
}
