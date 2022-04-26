package software.amazon.rds.globalcluster;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.util.IdentifierFactory;

public class CreateHandler extends BaseHandlerStd {

    private final static IdentifierFactory clusterIdentifierFactory = new IdentifierFactory(
            STACK_NAME,
            RESOURCE_IDENTIFIER,
            GLOBAL_CLUSTER_ID_MAX_LENGTH
    );

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                          final ResourceHandlerRequest<ResourceModel> request,
                                                                          final CallbackContext callbackContext,
                                                                          final ProxyClient<RdsClient> proxyClient,
                                                                          final Logger logger) {

        ResourceModel model = request.getDesiredResourceState();

        if (StringUtils.isNullOrEmpty(model.getGlobalClusterIdentifier())) {
            model.setGlobalClusterIdentifier(clusterIdentifierFactory.newIdentifier()
                    .withStackId(request.getStackId())
                    .withResourceId(request.getLogicalResourceIdentifier())
                    .withRequestToken(request.getClientRequestToken())
                    .toString());
        }

        return ProgressEvent.progress(model, callbackContext)
                .then(progress -> {
                    if (!validateSourceDBClusterIdentifier(progress.getResourceModel())) {
                        return createGlobalClusterWithSourceDBCluster(proxy, proxyClient, progress);
                    }
                    return progress;
                })
                .then(progress -> {
                    //check if source cluster identifier is null or is in arn format
                    if (validateSourceDBClusterIdentifier(progress.getResourceModel())) {
                        return createGlobalCluster(proxy, proxyClient, progress);
                    }
                    return progress;
                })
                .then(progress -> waitForGlobalClusterAvailableStatus(proxy, proxyClient, progress))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }
}
