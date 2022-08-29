package software.amazon.rds.dbclusterparametergroup;

import java.util.List;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBClusterParameterGroup;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;

public class ReadHandler extends BaseHandlerStd {

    public ReadHandler() {
        this(DEFAULT_HANDLER_CONFIG);
    }

    public ReadHandler(final DefaultHandlerConfig config) {
        super(config);
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                          final ResourceHandlerRequest<ResourceModel> request,
                                                                          final CallbackContext callbackContext,
                                                                          final ProxyClient<RdsClient> proxyClient,
                                                                          final Logger logger) {
        return describeDbClusterParameterGroup(proxy, proxyClient, request.getDesiredResourceState(), callbackContext)
                .done((paramGroupRequest, paramGroupResponse, proxyInvocation, resourceModel, context) -> {
                    final DBClusterParameterGroup group = paramGroupResponse.dbClusterParameterGroups().stream().findFirst().get();
                    context.setDbClusterParameterGroupArn(group.dbClusterParameterGroupArn());
                    resourceModel.setDescription(group.description());
                    resourceModel.setFamily(group.dbParameterGroupFamily());
                    return ProgressEvent.progress(resourceModel, context);
                })
                .then(progress -> readTags(proxyClient, progress));

    }

    private ProgressEvent<ResourceModel, CallbackContext> readTags(
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress) {
        ResourceModel model = progress.getResourceModel();
        CallbackContext context = progress.getCallbackContext();
        try {
            String arn = progress.getCallbackContext().getDbClusterParameterGroupArn();
            List<software.amazon.rds.dbclusterparametergroup.Tag> resourceTags = Translator.translateTagsFromSdk(Tagging.listTagsForResource(proxyClient, arn));
            model.setTags(resourceTags);
        } catch (Exception exception) {
            return Commons.handleException(
                    ProgressEvent.progress(model, context),
                    exception,
                    DEFAULT_DB_CLUSTER_PARAMETER_GROUP_ERROR_RULE_SET.extendWith(Tagging.SOFT_FAIL_TAG_ERROR_RULE_SET)
            );
        }
        return ProgressEvent.success(model, context);
    }
}
