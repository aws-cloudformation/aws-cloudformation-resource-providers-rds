package software.amazon.rds.dbsecuritygroup;

import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeDbSecurityGroupsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class ListHandler extends BaseHandlerStd {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger) {

        final DescribeDbSecurityGroupsResponse describeDbSecurityGroupsResponse = proxy.injectCredentialsAndInvokeV2(
                Translator.describeDBSecurityGroupsRequest(request.getNextToken()),
                proxyClient.client()::describeDBSecurityGroups
        );

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                .resourceModels(
                        Optional.ofNullable(describeDbSecurityGroupsResponse.dbSecurityGroups())
                                .orElse(Collections.emptyList())
                                .stream()
                                .map(dbSecurityGroup -> ResourceModel.builder()
                                        .groupName(dbSecurityGroup.dbSecurityGroupName())
                                        .build()
                                ).collect(Collectors.toList())
                )
                .nextToken(describeDbSecurityGroupsResponse.marker())
                .status(OperationStatus.SUCCESS)
                .build();
    }
}
