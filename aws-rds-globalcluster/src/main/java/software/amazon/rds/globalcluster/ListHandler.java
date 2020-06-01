package software.amazon.rds.globalcluster;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeGlobalClustersResponse;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.ProxyClient;

import java.util.stream.Collectors;

public class ListHandler extends BaseHandlerStd {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger) {

        final DescribeGlobalClustersResponse describeDbClustersResponse =
                proxy.injectCredentialsAndInvokeV2(Translator.describeGlobalClusterRequest(request.getNextToken()),
                        proxyClient.client()::describeGlobalClusters);

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                .resourceModels(describeDbClustersResponse.globalClusters()
                        .stream()
                        .map(globalCluster -> ResourceModel.builder()
                                .globalClusterIdentifier(globalCluster.globalClusterIdentifier()).build())
                        .collect(Collectors.toList()))
                .nextToken(describeDbClustersResponse.marker())
                .status(OperationStatus.SUCCESS)
                .build();
    }
}
