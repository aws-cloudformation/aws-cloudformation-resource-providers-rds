package software.amazon.rds.globalcluster;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.resource.IdentifierUtils;

public class CreateHandler extends BaseHandlerStd {

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                          final ResourceHandlerRequest<ResourceModel> request,
                                                                          final CallbackContext callbackContext,
                                                                          final ProxyClient<RdsClient> proxyClient,
                                                                          final Logger logger) {

        ResourceModel model = request.getDesiredResourceState();

        if(StringUtils.isNullOrEmpty(model.getGlobalClusterIdentifier())) {
            model.setGlobalClusterIdentifier(IdentifierUtils.generateResourceIdentifier(request.getLogicalResourceIdentifier(),
                    request.getClientRequestToken(), GLOBAL_CLUSTER_ID_MAX_LENGTH).toLowerCase());
        }

        return ProgressEvent.progress(model, callbackContext)
                .then(progress -> {
                    if(!StringUtils.isNullOrEmpty(progress.getResourceModel().getSourceDBClusterIdentifier())
                            && !validateSourceDBClusterIdentifier(progress.getResourceModel())) {
                        return createGlobalClusterWithSourceDBCluster(proxy, proxyClient, progress);
                    }
                    return progress;
                })
                .then(progress -> {
                    //check if source cluster identifier is null or is in arn format
                    if(validateSourceDBClusterIdentifier(progress.getResourceModel())) {
                        return createGlobalCluster(proxy, proxyClient, progress);
                    }
                    return progress;
                })
                .then(progress -> waitForGlobalClusterAvailableStatus(proxy, proxyClient, progress))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }
}
