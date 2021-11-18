package software.amazon.rds.dbparametergroup;

import java.util.stream.Collectors;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeDbParameterGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbParameterGroupsResponse;
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
        setLogger(logger);
        DescribeDbParameterGroupsResponse describeDBParameterGroupsResponse = null;
        DescribeDbParameterGroupsRequest describeDbParameterGroupsRequest = Translator.describeDbParameterGroupsRequest(request.getNextToken());
        try {
            describeDBParameterGroupsResponse = proxy.injectCredentialsAndInvokeV2(describeDbParameterGroupsRequest,
                    proxyClient.client()::describeDBParameterGroups);
        } catch (Exception exception) {
            return Commons.handleException(
                    ProgressEvent.progress(request.getDesiredResourceState(), callbackContext),
                    exception,
                    DEFAULT_DB_PARAMETER_GROUP_ERROR_RULE_SET);
        }

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                .resourceModels(
                        describeDBParameterGroupsResponse.dbParameterGroups()
                                .stream()
                                .map(dBParameterGroup -> ResourceModel.builder()
                                        .dBParameterGroupName(dBParameterGroup.dbParameterGroupName())
                                        .build()
                                ).collect(Collectors.toList())
                ).nextToken(describeDBParameterGroupsResponse.marker())
                .status(OperationStatus.SUCCESS)
                .build();
    }

}
