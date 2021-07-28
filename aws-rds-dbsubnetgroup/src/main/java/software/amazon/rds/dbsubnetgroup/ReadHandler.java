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
        .handleError((awsRequest, exception, client, resourceModel, context) -> handleException(exception))
        .done((describeDbSubnetGroupsRequest, describeDbSubnetGroupsResponse, proxyInvocation, model, context) -> {
          final DBSubnetGroup dbSubnetGroup = describeDbSubnetGroupsResponse.dbSubnetGroups().stream().findFirst().get();
          final Set<Tag> tags = translateTagsFromSdk(proxyInvocation.injectCredentialsAndInvokeV2(Translator.listTagsForResourceRequest(dbSubnetGroup.dbSubnetGroupArn()), proxyInvocation.client()::listTagsForResource).tagList());

          return ProgressEvent.defaultSuccessHandler(ResourceModel.builder()
              .dBSubnetGroupName(dbSubnetGroup.dbSubnetGroupName())
              .dBSubnetGroupDescription(dbSubnetGroup.dbSubnetGroupDescription())
              .subnetIds(dbSubnetGroup.subnets().stream().map(Subnet::subnetIdentifier).collect(Collectors.toList()))
              .tags(tags)
              .build());
        });
    }
}
