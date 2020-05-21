package software.amazon.rds.globalcluster;

import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.SdkClient;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class DeleteHandler extends BaseHandlerStd {
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                          final ResourceHandlerRequest<ResourceModel> request,
                                                                          final CallbackContext callbackContext,
                                                                          final ProxyClient<RdsClient> proxyClient,
                                                                          final Logger logger) {
        return proxy.initiate("rds::delete-global-cluster", proxyClient, request.getDesiredResourceState(), callbackContext)
                // request to create global cluster
                .translateToServiceRequest(Translator::deleteGlobalClusterRequest)
                .makeServiceCall((deleteGlobalClusterRequest, proxyClient1) -> proxyClient1.injectCredentialsAndInvokeV2(deleteGlobalClusterRequest, proxyClient1.client()::deleteGlobalCluster))
                .stabilize(((deleteGlobalClusterRequest, deleteGlobalClusterResponse, proxyClient1, model, context) -> isDeleted(model, proxyClient1)))
                .success();
    }
}
