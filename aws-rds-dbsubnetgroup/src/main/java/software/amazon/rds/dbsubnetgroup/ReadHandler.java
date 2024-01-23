package software.amazon.rds.dbsubnetgroup;

import java.util.List;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBSubnetGroup;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;

public class ReadHandler extends BaseHandlerStd {

    public ReadHandler() {
        this(HandlerConfig.builder().build());
    }

    public ReadHandler(final HandlerConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient
    ) {
        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                .then(progress -> describeDbSubnetGroup(proxy, request, callbackContext, proxyClient))
                .then(progress -> readTags(proxyClient, progress));
    }

    private ProgressEvent<ResourceModel, CallbackContext> describeDbSubnetGroup(final AmazonWebServicesClientProxy proxy,
                                                                                final ResourceHandlerRequest<ResourceModel> request,
                                                                                final CallbackContext callbackContext,
                                                                                final ProxyClient<RdsClient> proxyClient
    ) {
        return proxy.initiate("rds::read-dbsubnet-group", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::describeDbSubnetGroupsRequest)
                .backoffDelay(config.getBackoff())
                .makeServiceCall((describeDbSubnetGroupRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(describeDbSubnetGroupRequest, proxyInvocation.client()::describeDBSubnetGroups))
                .handleError((awsRequest, exception, client, resourceModel, context) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, context),
                        exception,
                        DEFAULT_DB_SUBNET_GROUP_ERROR_RULE_SET, requestLogger))
                .done((describeDbSubnetGroupsRequest, describeDbSubnetGroupsResponse, proxyInvocation, model, context) -> {
                    final DBSubnetGroup dbSubnetGroup = describeDbSubnetGroupsResponse.dbSubnetGroups().stream().findFirst().get();
                    context.setDbSubnetGroupArn(dbSubnetGroup.dbSubnetGroupArn());
                    return ProgressEvent.progress(Translator.translateToModel(dbSubnetGroup), context);
                });
    }

    protected ProgressEvent<ResourceModel, CallbackContext> readTags(
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        ResourceModel model = progress.getResourceModel();
        CallbackContext context = progress.getCallbackContext();
        try {
            String arn = progress.getCallbackContext().getDbSubnetGroupArn();
            List<software.amazon.rds.dbsubnetgroup.Tag> resourceTags = Translator
                    .translateTags(Tagging.listTagsForResource(proxyClient, arn));
            model.setTags(resourceTags);
        } catch (Exception exception) {
            return Commons.handleException(
                    ProgressEvent.progress(model, context),
                    exception,
                    DEFAULT_DB_SUBNET_GROUP_ERROR_RULE_SET.extendWith(Tagging.IGNORE_LIST_TAGS_PERMISSION_DENIED_ERROR_RULE_SET),
                    requestLogger
            );
        }
        return ProgressEvent.success(model, context);
    }
}
