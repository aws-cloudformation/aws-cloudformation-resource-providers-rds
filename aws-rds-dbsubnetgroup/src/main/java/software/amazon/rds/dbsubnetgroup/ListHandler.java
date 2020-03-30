package software.amazon.rds.dbsubnetgroup;

import software.amazon.awssdk.services.rds.model.DescribeDbSubnetGroupsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
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

        final List<ResourceModel> models = new ArrayList<>();

        final DescribeDbSubnetGroupsResponse describeDbSubnetGroupsResponse =
            proxy.injectCredentialsAndInvokeV2(Translator.describeDbSubnetGroupsRequest(request.getNextToken()),
                ClientBuilder.getClient()::describeDBSubnetGroups);

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
            .resourceModels(describeDbSubnetGroupsResponse.dbSubnetGroups()
            .stream().map(dbSubnetGroup -> ResourceModel.builder()
                    .dBSubnetGroupName(dbSubnetGroup.dbSubnetGroupName()).build())
                .collect(Collectors.toList()))
            .nextToken(describeDbSubnetGroupsResponse.marker())
            .status(OperationStatus.SUCCESS)
            .build();
    }
}
