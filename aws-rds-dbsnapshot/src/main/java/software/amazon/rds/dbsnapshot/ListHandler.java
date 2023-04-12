package software.amazon.rds.dbsnapshot;

import java.util.stream.Collectors;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;

public class ListHandler extends BaseHandlerStd {

    public ListHandler() {
        this(DEFAULT_HANDLER_CONFIG);
    }

    public ListHandler(final HandlerConfig config) {
        super(config);
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> client,
            final Logger logger
    ) {
        try {
            final DescribeDbSnapshotsResponse response = proxy.injectCredentialsAndInvokeV2(
                    Translator.describeDBSnapshotRequest(request.getNextToken()),
                    client.client()::describeDBSnapshots
            );

            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                    .status(OperationStatus.SUCCESS)
                    .nextToken(response.marker())
                    .resourceModels(response.dbSnapshots().stream()
                            .map(Translator::translateDBSnapshotFromSdk)
                            .collect(Collectors.toList()))
                    .build();
        } catch (Exception exception) {
            return Commons.handleException(
                    ProgressEvent.progress(request.getDesiredResourceState(), callbackContext),
                    exception,
                    DEFAULT_DB_SNAPSHOT_ERROR_RULE_SET
            );
        }
    }
}
