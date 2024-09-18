package software.amazon.rds.dbshardgroup;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBShardGroup;
import software.amazon.awssdk.services.rds.model.DescribeDbShardGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbShardGroupsResponse;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;

import java.util.stream.Collectors;
import software.amazon.rds.common.handler.Tagging;

public class ListHandler extends BaseHandlerStd {

    /** Default constructor w/ default backoff */
    public ListHandler() {
    }

    /** Default constructor w/ custom config */
    public ListHandler(HandlerConfig config) {
        super(config);
    }

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext
    ) {
        final DescribeDbShardGroupsRequest describeDbShardGroupsRequest = Translator.describeDbShardGroupsRequest(request.getNextToken());
        DescribeDbShardGroupsResponse describeDbShardGroupsResponse;
        List<ResourceModel> models = new ArrayList<>();
        try {
            describeDbShardGroupsResponse = proxy.injectCredentialsAndInvokeV2(describeDbShardGroupsRequest, proxyClient.client()::describeDBShardGroups);
            for (DBShardGroup dbShardGroup : describeDbShardGroupsResponse.dbShardGroups()) {
                Set<Tag> tagSet = getTags(proxyClient, request, dbShardGroup);
                models.add(Translator.translateDbShardGroupFromSdk(dbShardGroup, tagSet));
            }
        } catch (Exception e) {
            return Commons.handleException(
                    ProgressEvent.progress(request.getDesiredResourceState(), callbackContext),
                    e,
                    DEFAULT_DB_SHARD_GROUP_ERROR_RULE_SET,
                    requestLogger
            );
        }
        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                .resourceModels(models)
                .nextToken(describeDbShardGroupsResponse.marker())
                .status(OperationStatus.SUCCESS)
                .build();
    }
}
