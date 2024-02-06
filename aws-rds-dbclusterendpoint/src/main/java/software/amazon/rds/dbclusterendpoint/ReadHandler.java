package software.amazon.rds.dbclusterendpoint;

import java.util.Optional;
import java.util.Set;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBClusterEndpoint;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;

public class ReadHandler extends BaseHandlerStd {

    public ReadHandler() {
        this(HandlerConfig.builder()
                .backoff(BACKOFF_DELAY)
                .build());
    }

    public ReadHandler(HandlerConfig config) {
        super(config);
    }


    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext
    ) {
        return proxy.initiate("rds::describe-db-cluster-endpoint", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::describeDbClustersEndpointRequest)
                .backoffDelay(config.getBackoff())
                .makeServiceCall((describeRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        describeRequest,
                        proxyInvocation.client()::describeDBClusterEndpoints
                ))
                .handleError((describeRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DEFAULT_DB_CLUSTER_ENDPOINT_ERROR_RULE_SET,
                        requestLogger))
                .done((describeRequest, describeResponse, proxyInvocation, model, context) -> {
                    final Optional<DBClusterEndpoint> dbClusterEndpoint = describeResponse.dbClusterEndpoints().stream().findFirst();

                    if (!dbClusterEndpoint.isPresent()) {
                        return ProgressEvent.failed(model, context, HandlerErrorCode.NotFound,
                                "DBClusterEndpoint " + model.getDBClusterEndpointIdentifier() + " not found");
                    }

                    return ProgressEvent.progress(Translator.translateDbClusterEndpointFromSdk(dbClusterEndpoint.get()), context);
                })
                .then(progress -> readTags(proxyClient, progress));
    }

    protected ProgressEvent<ResourceModel, CallbackContext> readTags(
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress) {
        ResourceModel model = progress.getResourceModel();
        CallbackContext context = progress.getCallbackContext();
        try {
            String arn = model.getDBClusterEndpointArn();
            Set<software.amazon.rds.dbclusterendpoint.Tag> resourceTags = Translator.translateTagsFromSdk(Tagging.listTagsForResource(proxyClient, arn));
            model.setTags(resourceTags);
        } catch (Exception exception) {
            return Commons.handleException(
                    ProgressEvent.progress(model, context),
                    exception,
                    DEFAULT_DB_CLUSTER_ENDPOINT_ERROR_RULE_SET.extendWith(Tagging.IGNORE_LIST_TAGS_PERMISSION_DENIED_ERROR_RULE_SET),
                    requestLogger
            );
        }
        return ProgressEvent.success(model, context);
    }
}
