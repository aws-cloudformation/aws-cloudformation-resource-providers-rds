package software.amazon.rds.dbclusterparametergroup;

import java.util.stream.Collectors;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParameterGroupsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;

public class ListHandler extends BaseHandlerStd {

    public ListHandler() {
        this(DEFAULT_HANDLER_CONFIG);
    }

    public ListHandler(final LocalHandlerConfig config) {
        super(config);
    }

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger) {

        DescribeDbClusterParameterGroupsResponse describeDbClusterParameterGroupsResponse;
        try {
            describeDbClusterParameterGroupsResponse = proxy
                    .injectCredentialsAndInvokeV2(Translator.describeDbClusterParameterGroupsRequest(request.getNextToken()),
                            proxyClient.client()::describeDBClusterParameterGroups);
        } catch (Exception exception) {
            return Commons.handleException(
                    ProgressEvent.progress(request.getDesiredResourceState(), callbackContext),
                    exception,
                    DEFAULT_DB_CLUSTER_PARAMETER_GROUP_ERROR_RULE_SET);
        }

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                .resourceModels(describeDbClusterParameterGroupsResponse
                        .dbClusterParameterGroups()
                        .stream().map(dbClusterParameterGroup -> ResourceModel.builder().dBClusterParameterGroupName(dbClusterParameterGroup.dbClusterParameterGroupName()).build())
                        .collect(Collectors.toList()))
                .nextToken(describeDbClusterParameterGroupsResponse.marker())
                .status(OperationStatus.SUCCESS)
                .build();
    }
}
