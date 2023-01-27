package software.amazon.rds.dbparametergroup;

import java.util.List;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBParameterGroup;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.logging.RequestLogger;

public class ReadHandler extends BaseHandlerStd {

    public ReadHandler() {
        this(HandlerConfig.builder().build());
    }

    public ReadHandler(final HandlerConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final RequestLogger logger
    ) {
        return proxy.initiate("rds::read-db-parameter-group", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::describeDbParameterGroupsRequest)
                .backoffDelay(config.getBackoff())
                .makeServiceCall((describeRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(describeRequest, proxyInvocation.client()::describeDBParameterGroups))
                .handleError((describeRequest, exception, client, model, ctx) ->
                        Commons.handleException(
                                ProgressEvent.progress(model, ctx),
                                exception,
                                SOFT_FAIL_IN_PROGRESS_TAGGING_ERROR_RULE_SET
                        ))
                .done((describeRequest, describeResponse, proxyInvocation, model, context) -> {
                    try {
                        final DBParameterGroup dBParameterGroup = describeResponse.dbParameterGroups().stream().findFirst().get();
                        context.setDbParameterGroupArn(dBParameterGroup.dbParameterGroupArn());
                        return ProgressEvent.progress(Translator.translateFromDBParameterGroup(dBParameterGroup), context);
                    } catch (Exception exception) {
                        return Commons.handleException(
                                ProgressEvent.progress(model, context),
                                exception,
                                DEFAULT_DB_PARAMETER_GROUP_ERROR_RULE_SET
                        );
                    }
                })
                .then(progress -> readTags(proxyClient, progress));
    }

    protected ProgressEvent<ResourceModel, CallbackContext> readTags(
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        final ResourceModel model = progress.getResourceModel();
        final CallbackContext context = progress.getCallbackContext();
        try {
            final String arn = progress.getCallbackContext().getDbParameterGroupArn();
            final List<software.amazon.rds.dbparametergroup.Tag> resourceTags = Translator.translateTags(Tagging.listTagsForResource(proxyClient, arn));
            model.setTags(resourceTags);
        } catch (Exception exception) {
            return Commons.handleException(
                    ProgressEvent.progress(model, context),
                    exception,
                    DEFAULT_DB_PARAMETER_GROUP_ERROR_RULE_SET.extendWith(Tagging.SOFT_FAIL_TAG_ERROR_RULE_SET)
            );
        }
        return ProgressEvent.success(model, context);
    }
}
