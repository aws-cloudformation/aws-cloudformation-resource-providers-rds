package software.amazon.rds.dbcluster;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersResponse;
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

        final DescribeDbClustersResponse describeDbClustersResponse = proxy.injectCredentialsAndInvokeV2(Translator.describeDbClustersRequest(request.getNextToken()), proxyClient.client()::describeDBClusters);

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                .resourceModels(describeDbClustersResponse.dbClusters()
                        .stream()
                        .map(dbCluster -> ResourceModel.builder()
                                .dBClusterIdentifier(dbCluster.dbClusterIdentifier()).build())
                        .collect(Collectors.toList()))
                .nextToken(describeDbClustersResponse.marker())
                .status(OperationStatus.SUCCESS)
                .build();
    }
}
