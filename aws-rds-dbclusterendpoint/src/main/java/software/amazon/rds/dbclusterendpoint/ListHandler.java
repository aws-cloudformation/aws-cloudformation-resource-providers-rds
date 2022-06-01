package software.amazon.rds.dbclusterendpoint;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterEndpointsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;

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
                                        .dBClusterEndpointIdentifier(dbClusterEndpoint.dbClusterEndpointIdentifier())
                                        .build()
                                ).collect(Collectors.toList())
                ).nextToken(describeDbClusterEndpointsResponse.marker())
                .status(OperationStatus.SUCCESS)
                .build();
    }
}
