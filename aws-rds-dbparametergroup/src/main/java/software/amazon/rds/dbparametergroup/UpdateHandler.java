package software.amazon.rds.dbparametergroup;

import java.util.Map;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.logging.RequestLogger;

public class UpdateHandler extends BaseHandlerStd {

    protected ProgressEvent<ResourceModel, CallbackContext> tagResource(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Map<String, String> previousTags,
            final Map<String, String> desiredTags) {
        return proxy.initiate("rds::tag-db-parameter-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(Translator::describeDbParameterGroupsRequest)
                .makeServiceCall(((describeDbGroupsRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        describeDbGroupsRequest,
                        proxyInvocation.client()::describeDBParameterGroups)))
                .handleError((describeDbParameterGroupsRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DEFAULT_DB_PARAMETER_GROUP_ERROR_RULE_SET
                ))
                .done((describeDbParameterGroupsRequest, describeDbParameterGroupsResponse, invocation, resourceModel, context) -> {
                    final String arn = describeDbParameterGroupsResponse.dbParameterGroups().stream().findFirst().get().dbParameterGroupArn();
                    return Tagging.updateTags(
                            invocation,
                            arn,
                            ProgressEvent.progress(resourceModel, context),
                            previousTags,
                            desiredTags,
                            SOFT_FAIL_TAG_DB_PARAMETER_GROUP_ERROR_RULE_SET
                    );
                });
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final RequestLogger requestLogger
    ) {
        final Map<String, String> previousTags = Tagging.mergeTags(
                request.getPreviousSystemTags(),
                request.getPreviousResourceTags()
        );
        final Map<String, String> desiredTags = Tagging.mergeTags(
                request.getSystemTags(),
                request.getDesiredResourceTags()
        );

        final ResourceModel model = request.getDesiredResourceState();
        return ProgressEvent.progress(model, callbackContext)
                .then(progress -> applyParameters(proxy, proxyClient, progress.getResourceModel(), progress.getCallbackContext(), requestLogger))
                .then(progress -> tagResource(proxy, proxyClient, progress, previousTags, desiredTags))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, requestLogger));
    }
}
