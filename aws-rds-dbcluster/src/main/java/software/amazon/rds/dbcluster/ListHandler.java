package software.amazon.rds.dbcluster;

import java.util.stream.Collectors;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.request.ValidatedRequest;

public class ListHandler extends BaseHandlerStd {

    public ListHandler() {
        this(HandlerConfig.builder().build());
    }

    public ListHandler(final HandlerConfig config) {
        super(config);
    }

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ValidatedRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProxyClient<Ec2Client> ec2ProxyClient
    ) {
        final DescribeDbClustersResponse describeDbClustersResponse = proxy.injectCredentialsAndInvokeV2(
                Translator.describeDbClustersRequest(request.getNextToken()),
                rdsProxyClient.client()::describeDBClusters
        );

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                .resourceModels(describeDbClustersResponse.dbClusters()
                        .stream()
                        .map(Translator::translateDbClusterFromSdk)
                        .collect(Collectors.toList()))
                .nextToken(describeDbClustersResponse.marker())
                .status(OperationStatus.SUCCESS)
                .build();
    }
}
