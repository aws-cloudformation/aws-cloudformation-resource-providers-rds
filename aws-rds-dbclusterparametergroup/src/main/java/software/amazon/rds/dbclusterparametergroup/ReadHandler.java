package software.amazon.rds.dbclusterparametergroup;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBClusterParameterGroup;
import software.amazon.awssdk.services.rds.model.Parameter;
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
        this(DEFAULT_HANDLER_CONFIG);
    }

    public ReadHandler(final HandlerConfig config) {
        super(config);
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final RequestLogger logger
    ) {
        final ResourceModel model = request.getDesiredResourceState();

        return describeDbClusterParameterGroup(proxy, proxyClient, model, callbackContext)
                .done((paramGroupRequest, paramGroupResponse, proxyInvocation, resourceModel, context) -> {
                    final DBClusterParameterGroup dbClusterParameterGroup = paramGroupResponse.dbClusterParameterGroups().stream().findFirst().get();
                    context.setDbClusterParameterGroupArn(dbClusterParameterGroup.dbClusterParameterGroupArn());
                    final ResourceModel currentModel = Translator.translateFromSdk(dbClusterParameterGroup);
                    if (model.getParameters() != null) {
                        currentModel.setParameters(model.getParameters());
                    }
                    return ProgressEvent.progress(currentModel, context);
                })
                .then(progress -> {
                    if (progress.getResourceModel().getParameters() == null) {
                        return readParameters(proxy, proxyClient, progress, logger);
                    }
                    return progress;
                })
                .then(progress -> readTags(proxyClient, progress));
    }

    private ProgressEvent<ResourceModel, CallbackContext> readParameters(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final RequestLogger logger
    ) {
        final Map<String, Parameter> engineDefaultClusterParameters = new HashMap<>();
        final Map<String, Parameter> currentDBClusterParameters = new HashMap<>();

        return progress
                .then(p -> describeEngineDefaultClusterParameters(proxy, proxyClient, p, null, engineDefaultClusterParameters, logger))
                .then(p -> describeCurrentDBClusterParameters(proxy, proxyClient, p, null, currentDBClusterParameters, logger))
                .then(p -> {
                    p.getResourceModel().setParameters(
                            Translator.translateParametersFromSdk(computeModifiedDBParameters(engineDefaultClusterParameters, currentDBClusterParameters))
                    );
                    return p;
                });
    }

    private ProgressEvent<ResourceModel, CallbackContext> readTags(
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        final ResourceModel model = progress.getResourceModel();
        final CallbackContext context = progress.getCallbackContext();
        try {
            final String arn = progress.getCallbackContext().getDbClusterParameterGroupArn();
            final List<software.amazon.rds.dbclusterparametergroup.Tag> resourceTags = Translator.translateTagsFromSdk(Tagging.listTagsForResource(proxyClient, arn));
            model.setTags(resourceTags);
        } catch (Exception exception) {
            return Commons.handleException(
                    ProgressEvent.progress(model, context),
                    exception,
                    DEFAULT_DB_CLUSTER_PARAMETER_GROUP_ERROR_RULE_SET.extendWith(Tagging.IGNORE_LIST_TAGS_PERMISSION_DENIED_ERROR_RULE_SET)
            );
        }
        return ProgressEvent.success(model, context);
    }
}
