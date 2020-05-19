package software.amazon.rds.globalcluster;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.*;
import software.amazon.awssdk.services.rds.model.GlobalClusterNotFoundException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

// Placeholder for the functionality that could be shared across Create/Read/Update/Delete/List Handlers

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {
  protected static final int GLOBAL_CLUSTER_ID_MAX_LENGTH = 63;
  protected static final int PAUSE_TIME_SECONDS = 60;
  protected static final Constant BACKOFF_STRATEGY = Constant.of().timeout(Duration.ofMinutes(120L)).delay(Duration.ofSeconds(30L)).build();
  private static final String MESSAGE_FORMAT_FAILED_TO_STABILIZE = "GlobalCluster %s failed to stabilize.";
  protected static final BiFunction<ResourceModel, ProxyClient<RdsClient>, ResourceModel> EMPTY_CALL = (model, proxyClient) -> model;

  @Override
  public final ProgressEvent<ResourceModel, CallbackContext> handleRequest(
          final AmazonWebServicesClientProxy proxy,
          final ResourceHandlerRequest<ResourceModel> request,
          final CallbackContext callbackContext,
          final Logger logger) {
    return handleRequest(
            proxy,
            request,
            callbackContext != null ? callbackContext : new CallbackContext(),
            proxy.newProxy(ClientBuilder::getClient),
            logger
    );
  }

  protected abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(AmazonWebServicesClientProxy proxy,
                                                                                 ResourceHandlerRequest<ResourceModel> request,
                                                                                 CallbackContext callbackContext,
                                                                                 ProxyClient<RdsClient> proxyClient,
                                                                                 Logger logger);
  protected ProgressEvent<ResourceModel, CallbackContext> waitForGlobalClusterAvailableStatus(
          final AmazonWebServicesClientProxy proxy,
          final ProxyClient<RdsClient> proxyClient,
          final ProgressEvent<ResourceModel, CallbackContext> progress) {
    // this is a stabilizer for global cluster
    return proxy.initiate("rds::stabilize-global-cluster" + getClass().getSimpleName(), proxyClient, progress.getResourceModel(), progress.getCallbackContext())
            // only stabilization is necessary so this is a dummy call
            // Function.identity() takes ResourceModel as an input and returns (the same) ResourceModel
            // Function.identity() is roughly similar to `model -> model`
            .translateToServiceRequest(Function.identity())
            // this skips the call and goes directly to stabilization
            .makeServiceCall(EMPTY_CALL)
            .stabilize((resourceModel, response, proxyInvocation, model, callbackContext) ->
                    isGlobalClusterStabilized(proxyInvocation, resourceModel, GlobalClusterStatus.Available)).progress();
  }

  // Global Cluster Stabilization
  protected boolean isGlobalClusterStabilized(final ProxyClient<RdsClient> proxyClient,
                                              final ResourceModel model,
                                              final GlobalClusterStatus expectedStatus) {
    // describe status of a resource to make sure it's ready
    // describe db cluster
    try {
      final Optional<GlobalCluster> globalCluster =
              proxyClient.injectCredentialsAndInvokeV2(
                      Translator.describeGlobalClusterRequest(model),
                      proxyClient.client()::describeGlobalClusters).globalClusters().stream().findFirst();

      if (!globalCluster.isPresent())
        throw new CfnNotFoundException(ResourceModel.TYPE_NAME, model.getGlobalClusterIdentifier());

      return expectedStatus.equalsString(globalCluster.get().status());
    } catch (GlobalClusterNotFoundException e) {
      throw new CfnNotFoundException(ResourceModel.TYPE_NAME, e.getMessage());
    } catch (Exception e) {
      throw new CfnNotStabilizedException(MESSAGE_FORMAT_FAILED_TO_STABILIZE, model.getGlobalClusterIdentifier(), e);
    }
  }

  // Modify or Post Create
  protected ProgressEvent<ResourceModel, CallbackContext> modifyGlobalCluster(final AmazonWebServicesClientProxy proxy,
                                                                          final ProxyClient<RdsClient> proxyClient,
                                                                          final ProgressEvent<ResourceModel, CallbackContext> progress) {
    if (progress.getCallbackContext().isModified()) return progress;
    return proxy.initiate("rds::modify-global-cluster", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
            .translateToServiceRequest(Translator::modifyGlobalClusterRequest)
            .makeServiceCall((modifyGlobalClusterRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(modifyGlobalClusterRequest, proxyInvocation.client()::modifyGlobalCluster))
            .done((modifyGlobalClusterRequest, modifyGlobalClusterResponse, proxyInvocation, resourceModel, callbackContext) ->  {
              callbackContext.setModified(true);
              return ProgressEvent.defaultInProgressHandler(callbackContext, PAUSE_TIME_SECONDS, resourceModel);
            });
  }

  // Modify or Post Create
  protected ProgressEvent<ResourceModel, CallbackContext> removeFromGlobalCluster(final AmazonWebServicesClientProxy proxy,
                                                                              final ProxyClient<RdsClient> proxyClient,
                                                                              final ProgressEvent<ResourceModel, CallbackContext> progress) {
    if (progress.getCallbackContext().isModified()) return progress;
    return proxy.initiate("rds::remove-from-global-cluster", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
            .translateToServiceRequest(Translator::removeFromGlobalClusterRequest)
            .backoffDelay(BACKOFF_STRATEGY)
            .makeServiceCall((removeFromGlobalClusterRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(removeFromGlobalClusterRequest, proxyInvocation.client()::removeFromGlobalCluster))
            .done((removeFromGlobalClusterRequest, removeFromGlobalClusterResponse, proxyInvocation, resourceModel, callbackContext) ->  {
              callbackContext.setModified(true);
              return ProgressEvent.defaultInProgressHandler(callbackContext, PAUSE_TIME_SECONDS, resourceModel);
            });
  }

  protected boolean isDeleted(final ResourceModel model,
                              final ProxyClient<RdsClient> proxyClient) {
    try {
      proxyClient.injectCredentialsAndInvokeV2(
              Translator.describeGlobalClusterRequest(model),
              proxyClient.client()::describeGlobalClusters);
      return false;
    } catch (GlobalClusterNotFoundException e) {
      return true;
    }
  }

}
