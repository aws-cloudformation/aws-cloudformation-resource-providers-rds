package software.amazon.rds.globalcluster;

import java.util.Objects;
import org.apache.commons.lang3.BooleanUtils;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.ProxyClient;

public class UpdateHandler extends BaseHandlerStd {
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                          final ResourceHandlerRequest<ResourceModel> request,
                                                                          final CallbackContext callbackContext,
                                                                          final ProxyClient<RdsClient> proxyClient,
                                                                          final Logger logger) {

        ResourceModel previousModel = request.getPreviousResourceState();
        ResourceModel desiredModel = request.getDesiredResourceState();

        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                .then(progress -> {
                    if(!Objects.equals(previousModel.getEngineLifecycleSupport(), desiredModel.getEngineLifecycleSupport())) {
                        throw new CfnInvalidRequestException("EngineLifecycleSupport cannot be modified.");
                    }
                    return progress;
                })
                .then(progress -> proxy.initiate("rds::update-global-cluster", proxyClient, request.getDesiredResourceState(), callbackContext)
                // request to update global cluster
                .translateToServiceRequest(model -> Translator.modifyGlobalClusterRequest(previousModel, desiredModel, BooleanUtils.isTrue(request.getRollback())))
                .backoffDelay(BACKOFF_STRATEGY)
                .makeServiceCall((modifyGlobalClusterRequest, proxyClient1) -> proxyClient1.injectCredentialsAndInvokeV2(modifyGlobalClusterRequest, proxyClient1.client()::modifyGlobalCluster))
                .stabilize(((modifyGlobalClusterRequest, modifyGlobalClusterResponse, proxyClient1, resourceModel, callbackContext1) ->
                        isGlobalClusterStabilized(proxyClient1, desiredModel)))
                .progress()
                .then(readProgress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger)));
    }
}
