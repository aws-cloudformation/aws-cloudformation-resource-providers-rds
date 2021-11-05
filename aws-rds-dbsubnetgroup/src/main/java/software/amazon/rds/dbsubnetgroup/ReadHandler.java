package software.amazon.rds.dbsubnetgroup;

import static software.amazon.rds.dbsubnetgroup.Translator.translateTagsFromSdk;

import java.util.Set;
import java.util.stream.Collectors;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBSubnetGroup;
import software.amazon.awssdk.services.rds.model.Subnet;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.Tagging;

public class ReadHandler extends BaseHandlerStd {
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger) {
        return proxy.initiate("rds::read-dbsubnet-group", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::describeDbSubnetGroupsRequest)
                .backoffDelay(CONSTANT)
                .makeServiceCall((describeDbSubnetGroupRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(describeDbSubnetGroupRequest, proxyInvocation.client()::describeDBSubnetGroups))
                .handleError((awsRequest, exception, client, resourceModel, context) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, context),
                        exception,
                        DEFAULT_DB_SUBNET_GROUP_ERROR_RULE_SET))
                .done((describeDbSubnetGroupsRequest, describeDbSubnetGroupsResponse, proxyInvocation, model, context) -> {
                    try {
                        final DBSubnetGroup dbSubnetGroup = describeDbSubnetGroupsResponse.dbSubnetGroups().stream().findFirst().get();
                        final Set<Tag> tags = translateTagsFromSdk(Tagging.listTagsForResource(proxyInvocation, dbSubnetGroup.dbSubnetGroupArn()));

                        return ProgressEvent.defaultSuccessHandler(ResourceModel.builder()
                                .dBSubnetGroupName(dbSubnetGroup.dbSubnetGroupName())
                                .dBSubnetGroupDescription(dbSubnetGroup.dbSubnetGroupDescription())
                                .subnetIds(dbSubnetGroup.subnets().stream().map(Subnet::subnetIdentifier).collect(Collectors.toList()))
                                .tags(tags)
                                .build());
                    } catch (Exception exception) {
                        return Commons.handleException(
                                ProgressEvent.progress(model, context),
                                exception,
                                DEFAULT_DB_SUBNET_GROUP_ERROR_RULE_SET);
                    }
                });
    }
}
