package software.amazon.rds.optiongroup;

import java.util.Optional;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.resource.IdentifierUtils;

public class CreateHandler extends BaseHandlerStd {

    public static final int MAX_LENGTH_OPTION_GROUP = 255;

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger
    ) {
        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                .then(progress -> {
                    final ResourceModel model = request.getDesiredResourceState();
                    if (StringUtils.isNullOrEmpty(model.getOptionGroupName())) {
                        model.setOptionGroupName(IdentifierUtils.generateResourceIdentifier(
                                request.getStackId(),
                                Optional.ofNullable(request.getLogicalResourceIdentifier()).orElse("optiongroup"),
                                request.getClientRequestToken(),
                                MAX_LENGTH_OPTION_GROUP
                        ).toLowerCase());
                    }
                    return ProgressEvent.progress(model, progress.getCallbackContext());
                })
                .then(progress -> proxy.initiate("rds::create-option-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                        .translateToServiceRequest(model -> Translator.createOptionGroupRequest(
                                model,
                                mergeMaps(request.getSystemTags(), request.getDesiredResourceTags())
                        ))
                        .backoffDelay(BACKOFF_DELAY)
                        .makeServiceCall((createRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                                createRequest,
                                proxyInvocation.client()::createOptionGroup
                        ))
                        .handleError((createRequest, exception, client, resourceModel, ctx) -> handleException(
                                ProgressEvent.progress(resourceModel, ctx),
                                exception
                        ))
                        .progress())
                .then(progress -> updateOptionGroup(proxy, proxyClient, progress))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }
}
