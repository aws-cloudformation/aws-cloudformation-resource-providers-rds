package software.amazon.rds.dbsubnetgroup;

import java.util.LinkedHashSet;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.resource.IdentifierUtils;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;

public class CreateHandler extends BaseHandlerStd {

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

        final ResourceModel desiredModel = request.getDesiredResourceState();

        final Tagging.TagSet systemTags = Tagging.TagSet.builder()
                .systemTags(Tagging.translateTagsToSdk(request.getSystemTags()))
                .build();

        final Tagging.TagSet extraTags = Tagging.TagSet.builder()
                .stackTags(Tagging.translateTagsToSdk(request.getDesiredResourceTags()))
                .resourceTags(new LinkedHashSet<>(Translator.translateTagsToSdk(desiredModel.getTags())))
                .build();

        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                .then(progress -> setDbSubnetGroupNameIfEmpty(request, progress))
                .then(progress -> createDbSubnetGroup(proxy, proxyClient, progress, systemTags))
                .then(progress -> updateTags(proxyClient, progress, Tagging.TagSet.emptySet(), extraTags))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private ProgressEvent<ResourceModel, CallbackContext> createDbSubnetGroup(final AmazonWebServicesClientProxy proxy,
                                                                              final ProxyClient<RdsClient> proxyClient,
                                                                              final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                              final Tagging.TagSet systemTags) {
        return proxy.initiate("rds::create-dbsubnet-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest((resourceModel) -> Translator.createDbSubnetGroupRequest(resourceModel, systemTags))
                .backoffDelay(config.getBackoff())
                .makeServiceCall((createDbSubnetGroupRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(createDbSubnetGroupRequest, proxyInvocation.client()::createDBSubnetGroup))
                .stabilize(((createDbSubnetGroupRequest, createDbSubnetGroupResponse, proxyInvocation, resourceModel, context) -> isStabilized(resourceModel, proxyInvocation)))
                .handleError((awsRequest, exception, client, resourceModel, context) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, context),
                        exception,
                        DEFAULT_DB_SUBNET_GROUP_ERROR_RULE_SET))
                .done((subnetGroupRequest, subnetGroupResponse, proxyInvocation, resourceModel, context) -> {
                    context.setDbSubnetGroupArn(subnetGroupResponse.dbSubnetGroup().dbSubnetGroupArn());
                    return ProgressEvent.progress(resourceModel, context);
                });
    }

    private ProgressEvent<ResourceModel, CallbackContext> setDbSubnetGroupNameIfEmpty(final ResourceHandlerRequest<ResourceModel> request,
                                                                                      final ProgressEvent<ResourceModel, CallbackContext> progress) {
        final ResourceModel model = progress.getResourceModel();
        if (StringUtils.isNullOrEmpty(model.getDBSubnetGroupName())){
            model.setDBSubnetGroupName(IdentifierUtils
                    .generateResourceIdentifier(request.getLogicalResourceIdentifier(), request.getClientRequestToken(), DB_SUBNET_GROUP_NAME_LENGTH)
                    .toLowerCase());
        }
        return ProgressEvent.progress(model, progress.getCallbackContext());
    }
}
