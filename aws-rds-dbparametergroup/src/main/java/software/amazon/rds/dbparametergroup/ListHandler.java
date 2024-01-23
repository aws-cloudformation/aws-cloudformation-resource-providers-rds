package software.amazon.rds.dbparametergroup;

import java.util.stream.Collectors;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeDbParameterGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbParameterGroupsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.logging.RequestLogger;

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
            final ProxyClient<RdsClient> proxyClient,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext
    ) {
        final DescribeDbParameterGroupsRequest describeRequest = Translator.describeDbParameterGroupsRequest(request.getNextToken());
        final DescribeDbParameterGroupsResponse describeResponse;
        try {
            describeResponse = proxy.injectCredentialsAndInvokeV2(describeRequest, proxyClient.client()::describeDBParameterGroups);
        } catch (Exception exception) {
            return Commons.handleException(
                    ProgressEvent.progress(request.getDesiredResourceState(), callbackContext),
                    exception,
                    DEFAULT_DB_PARAMETER_GROUP_ERROR_RULE_SET,
                    requestLogger
            );
        }

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                .resourceModels(
                        describeResponse.dbParameterGroups()
                                .stream()
                                .map(dBParameterGroup -> ResourceModel.builder()
                                        .dBParameterGroupName(dBParameterGroup.dbParameterGroupName())
                                        .build()
                                ).collect(Collectors.toList())
                ).nextToken(describeResponse.marker())
                .status(OperationStatus.SUCCESS)
                .build();
    }
}
