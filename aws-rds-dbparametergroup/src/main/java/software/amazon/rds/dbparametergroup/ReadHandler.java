package software.amazon.rds.dbparametergroup;

import java.util.Set;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBParameterGroup;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.logging.RequestLogger;

public class ReadHandler extends BaseHandlerStd {

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final RequestLogger requestLogger
    ) {
        return proxy.initiate("rds::read-db-parameter-group", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::describeDbParameterGroupsRequest)
                .backoffDelay(CONSTANT)
                .makeServiceCall((describeDbParameterGroupsRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(describeDbParameterGroupsRequest, proxyInvocation.client()::describeDBParameterGroups))
                .handleError((describeDbParameterGroupsRequest, exception, client, resourceModel, ctx) ->
                        Commons.handleException(
                                ProgressEvent.progress(resourceModel, ctx),
                                exception,
                                DEFAULT_DB_PARAMETER_GROUP_ERROR_RULE_SET
                        ))
                .done((describeDbParameterGroupsRequest, describeDbParameterGroupsResponse, proxyInvocation, model, context) -> {
                    try {
                        final DBParameterGroup dBParameterGroup = describeDbParameterGroupsResponse.dbParameterGroups().stream().findFirst().get();
                        final Set<Tag> tags = Tagging.listTagsForResource(proxyInvocation, dBParameterGroup.dbParameterGroupArn());
                        return ProgressEvent.success(Translator.translateFromDBParameterGroup(dBParameterGroup, tags), context);
                    } catch (Exception exception) {
                        return Commons.handleException(
                                ProgressEvent.progress(model, context),
                                exception,
                                SOFT_FAIL_TAG_DB_PARAMETER_GROUP_ERROR_RULE_SET
                        );
                    }
                });
    }
}
