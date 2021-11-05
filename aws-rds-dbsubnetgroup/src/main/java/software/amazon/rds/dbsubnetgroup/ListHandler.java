package software.amazon.rds.dbsubnetgroup;

import java.util.stream.Collectors;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeDbSubnetGroupsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;

public class ListHandler extends BaseHandlerStd {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger) {

        final DescribeDbSubnetGroupsResponse describeDbSubnetGroupsResponse;
        try {
            describeDbSubnetGroupsResponse =
                    proxy.injectCredentialsAndInvokeV2(Translator.describeDbSubnetGroupsRequest(request.getNextToken()),
                            proxyClient.client()::describeDBSubnetGroups);
        } catch (Exception exception) {
            return Commons.handleException(
                    ProgressEvent.progress(request.getPreviousResourceState(), callbackContext),
                    exception,
                    DEFAULT_DB_SUBNET_GROUP_ERROR_RULE_SET);
        }

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                .resourceModels(describeDbSubnetGroupsResponse.dbSubnetGroups()
                        .stream().map(dbSubnetGroup -> ResourceModel.builder().dBSubnetGroupName(dbSubnetGroup.dbSubnetGroupName()).build()).collect(Collectors.toList()))
                .nextToken(describeDbSubnetGroupsResponse.marker())
                .status(OperationStatus.SUCCESS)
                .build();
    }
}
