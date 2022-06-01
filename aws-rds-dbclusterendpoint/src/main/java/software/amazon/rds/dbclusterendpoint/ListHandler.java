package software.amazon.rds.dbclusterendpoint;

import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterEndpointsResponse;
import software.amazon.awssdk.services.rds.model.DescribeEventSubscriptionsResponse;
import software.amazon.cloudformation.proxy.*;
import software.amazon.rds.common.handler.Commons;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ListHandler extends BaseHandlerStd {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger) {

        DescribeDbClusterEndpointsResponse describeDbClusterEndpointsResponse;
        try {
            describeDbClusterEndpointsResponse = proxy.injectCredentialsAndInvokeV2(
                    Translator.describeDbClustersEndpointRequest(request.getNextToken()),
                    proxyClient.client()::describeDBClusterEndpoints);
        } catch (Exception e) {
            return Commons.handleException(
                    ProgressEvent.progress(request.getDesiredResourceState(), callbackContext),
                    e,
                    DEFAULT_DB_CLUSTER_ENDPOINT_ERROR_RULE_SET
            );
        }

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                .resourceModels(
                        describeDbClusterEndpointsResponse.dbClusterEndpoints()
                                .stream()
                                .map(dbClusterEndpoint -> ResourceModel.builder()
                                        .dbClusterEndpointIdentifier(dbClusterEndpoint.dbClusterEndpointIdentifier())
                                        .build()
                                ).collect(Collectors.toList())
                ).nextToken(describeDbClusterEndpointsResponse.marker())
                .status(OperationStatus.SUCCESS)
                .build();
    }
}
