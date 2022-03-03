package software.amazon.rds.dbclusterparametergroup;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBClusterParameterGroup;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Tagging;

public class ReadHandler extends BaseHandlerStd {
    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                          final ResourceHandlerRequest<ResourceModel> request,
                                                                          final CallbackContext callbackContext,
                                                                          final ProxyClient<RdsClient> proxyClient,
                                                                          final Logger logger) {
        return describeDbClusterParameterGroup(proxy, proxyClient, request.getDesiredResourceState(), callbackContext)
                .done((paramGroupRequest, paramGroupResponse, proxyInvocation, resourceModel, context) -> {
                    final DBClusterParameterGroup group = paramGroupResponse.dbClusterParameterGroups().stream().findFirst().get();
                    resourceModel.setDescription(group.description());
                    resourceModel.setFamily(group.dbParameterGroupFamily());
                    resourceModel.setTags(Translator
                            .translateTagsFromSdk(Tagging
                                    .listTagsForResource(proxyInvocation, group.dbClusterParameterGroupArn())));
                    return ProgressEvent.defaultSuccessHandler(resourceModel);
                });
    }
}
