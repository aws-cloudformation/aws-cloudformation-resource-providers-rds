package software.amazon.rds.globalcluster;

import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeGlobalClustersResponse;
import software.amazon.awssdk.services.rds.model.GlobalCluster;
import software.amazon.cloudformation.proxy.*;

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

        final DescribeGlobalClustersResponse describeDbClustersResponse =
                proxy.injectCredentialsAndInvokeV2(Translator.describeGlobalClusterRequest(request.getNextToken()),
                        ClientBuilder.getClient()::describeGlobalClusters);

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                .resourceModels(describeDbClustersResponse.globalClusters()
                        .stream()
                        .map(globalCluster -> ResourceModel.builder()
                                .engine(globalCluster.engine())
                                .engineVersion(globalCluster.engineVersion())
                                .databaseName(globalCluster.databaseName())
                                .deletionProtection(globalCluster.deletionProtection())
                                .storageEncrypted(globalCluster.storageEncrypted())
                                .globalClusterIdentifier(globalCluster.globalClusterIdentifier()).build())
                        .collect(Collectors.toList()))
                .nextToken(describeDbClustersResponse.marker())
                .status(OperationStatus.SUCCESS)
                .build();
    }
}
