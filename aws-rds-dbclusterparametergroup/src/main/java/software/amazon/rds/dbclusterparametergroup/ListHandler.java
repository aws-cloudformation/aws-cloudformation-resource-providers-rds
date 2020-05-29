package software.amazon.rds.dbclusterparametergroup;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParameterGroupsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.stream.Collectors;

public class ListHandler extends BaseHandlerStd {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<RdsClient> proxyClient,
        final Logger logger) {

        final DescribeDbClusterParameterGroupsResponse describeDbClusterParameterGroupsResponse = proxy.injectCredentialsAndInvokeV2(Translator.describeDbClusterParameterGroupsRequest(request.getNextToken()), proxyClient.client()::describeDBClusterParameterGroups);

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                .resourceModels(describeDbClusterParameterGroupsResponse
                        .dbClusterParameterGroups()
                        .stream().map(dbClusterParameterGroup -> ResourceModel.builder().id(dbClusterParameterGroup.dbClusterParameterGroupName()).build())
                        .collect(Collectors.toList()))
                .nextToken(describeDbClusterParameterGroupsResponse.marker())
                .status(OperationStatus.SUCCESS)
                .build();
    }
}
