package software.amazon.rds.optiongroup;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeOptionGroupsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
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

        final DescribeOptionGroupsResponse describeOptionGroupsResponse =
                proxy.injectCredentialsAndInvokeV2(Translator.describeOptionGroupsRequest(request.getNextToken()),
                        proxyClient.client()::describeOptionGroups);

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                .resourceModels(
                        describeOptionGroupsResponse.optionGroupsList()
                                .stream()
                                .map(optionGroup -> ResourceModel.builder()
                                        .optionGroupName(optionGroup.optionGroupName())
                                        .build()
                                ).collect(Collectors.toList())
                ).nextToken(describeOptionGroupsResponse.marker())
                .status(OperationStatus.SUCCESS)
                .build();
    }
}
