package software.amazon.rds.dbshardgroup;

import java.util.Set;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBShardGroup;
import software.amazon.awssdk.services.rds.model.DescribeDbShardGroupsRequest;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;

public class ReadHandler extends BaseHandlerStd {

    /** Default constructor w/ default backoff */
    public ReadHandler() {
    }

    /** Default constructor w/ custom config */
    public ReadHandler(HandlerConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext) {
        DescribeDbShardGroupsRequest describeDbShardGroupsRequest = Translator.describeDbShardGroupsRequest(request.getDesiredResourceState());
        ResourceModel model;
        try {
            DBShardGroup dbShardGroup = proxyClient.injectCredentialsAndInvokeV2(describeDbShardGroupsRequest, proxyClient.client()::describeDBShardGroups).dbShardGroups().get(0);
            Set<Tag> tagSet = getTags(proxyClient, request, dbShardGroup);
            model = Translator.translateDbShardGroupFromSdk(dbShardGroup, tagSet);
        } catch (Exception e) {
            return Commons.handleException(
                    ProgressEvent.progress(request.getDesiredResourceState(), callbackContext),
                    e,
                    DEFAULT_DB_SHARD_GROUP_ERROR_RULE_SET,
                    requestLogger
            );
        }
        return ProgressEvent.success(model, callbackContext);
    }
}
