package software.amazon.rds.optiongroup;

import java.util.stream.Collectors;
import software.amazon.awssdk.services.rds.model.DescribeOptionGroupsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class ListHandler extends BaseHandler<CallbackContext> {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final Logger logger) {

        final DescribeOptionGroupsResponse describeOptionGroupsResponse = proxy.injectCredentialsAndInvokeV2(
            Translator.describeOptionGroupsRequest(request.getNextToken()),
            ClientBuilder.getClient()::describeOptionGroups);

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
            .resourceModels(
                describeOptionGroupsResponse.optionGroupsList()
                .stream().map(
                    optionGroup -> ResourceModel.builder()
                    .id(optionGroup.optionGroupName()).build()
                ).collect(Collectors.toList())
            )
            .nextToken(describeOptionGroupsResponse.marker())
            .status(OperationStatus.SUCCESS)
            .build();
    }
}
