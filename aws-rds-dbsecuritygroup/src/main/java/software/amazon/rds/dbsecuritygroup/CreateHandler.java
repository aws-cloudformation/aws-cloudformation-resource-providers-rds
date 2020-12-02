package software.amazon.rds.dbsecuritygroup;

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

    public static final int MAX_LENGTH_SECURITY_GROUP = 255;

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger) {

        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                .then(progress -> {
                    final ResourceModel model = progress.getResourceModel();
                    if (StringUtils.isNullOrEmpty(model.getGroupName())) {
                        model.setGroupName(IdentifierUtils.generateResourceIdentifier(
                                request.getStackId(),
                                Optional.ofNullable(request.getLogicalResourceIdentifier()).orElse("dbsecuritygroup"),
                                request.getClientRequestToken(),
                                MAX_LENGTH_SECURITY_GROUP
                        ).toLowerCase());
                    }
                    return ProgressEvent.progress(model, progress.getCallbackContext());
                })
                .then(progress -> proxy.initiate("rds::create-db-security-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                        .translateToServiceRequest(model -> Translator.createDBSecurityGroupRequest(
                                model,
                                Translator.translateTagsFromRequest(
                                        request.getDesiredResourceTags()
                                )
                        ))
                        .backoffDelay(BACK_OFF)
                        .makeServiceCall((createRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                                createRequest,
                                proxyClient.client()::createDBSecurityGroup
                        ))
                        .stabilize((createRequest, createResponse, proxyInvocation, model, context) -> isDBSecurityGroupStabilized(
                                model,
                                proxyInvocation
                        ))
                        .handleError((createRequest, exception, client, model, context) -> handleException(
                                ProgressEvent.progress(model, context),
                                exception
                        ))
                        .progress())
                .then(progress -> authorizeIngresses(proxy, proxyClient, progress, progress.getResourceModel().getDBSecurityGroupIngress()))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }
}
