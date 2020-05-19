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

  // Global Cluster Stabilization
  protected boolean isGlobalClusterStabilized(final ProxyClient<RdsClient> proxyClient,
                                              final ResourceModel model,
                                              final GlobalClusterStatus expectedStatus) {
    // describe status of a resource to make sure it's ready
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
