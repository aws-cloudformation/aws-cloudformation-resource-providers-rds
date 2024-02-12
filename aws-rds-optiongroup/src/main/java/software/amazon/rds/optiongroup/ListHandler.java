package software.amazon.rds.optiongroup;

import java.util.stream.Collectors;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeOptionGroupsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;

public class ListHandler extends BaseHandlerStd {

    public ListHandler() {
        this(HandlerConfig.builder()
                .backoff(BACKOFF_DELAY)
                .build());
    }

    public ListHandler(final HandlerConfig config) {
        super(config);
    }

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext) {

        DescribeOptionGroupsResponse describeOptionGroupsResponse;
        try {
            describeOptionGroupsResponse = proxy.injectCredentialsAndInvokeV2(
                    Translator.describeOptionGroupsRequest(request.getNextToken()),
                    proxyClient.client()::describeOptionGroups);
        } catch (Exception e) {
            return Commons.handleException(
                    ProgressEvent.progress(request.getDesiredResourceState(), callbackContext),
                    e,
                    DEFAULT_OPTION_GROUP_ERROR_RULE_SET,
                    requestLogger
            );
        }

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
