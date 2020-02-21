package software.amazon.rds.dbcluster;

import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ListHandler extends BaseHandler<CallbackContext> {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final Logger logger) {

        final DescribeDbClustersResponse dbClustersResponse = proxy.injectCredentialsAndInvokeV2(DescribeDbClustersRequest.builder().build(), ClientBuilder.getClient()::describeDBClusters);

        final List<ResourceModel> models = dbClustersResponse.dbClusters().stream()
                .map(dbCluster -> ResourceModel.builder().build())
                .collect(Collectors.toList());
        // TODO

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                .resourceModels(models)
                .nextToken(dbClustersResponse.marker())
                .status(OperationStatus.SUCCESS)
                .build();
    }
}
