package software.amazon.rds.globalcluster;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.GlobalCluster;
import software.amazon.awssdk.services.rds.model.GlobalClusterNotFoundException;
import software.amazon.awssdk.services.rds.model.GlobalClusterAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DbClusterNotFoundException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;

import java.util.Optional;
import java.util.function.Function;

import java.time.Duration;
import java.util.function.BiFunction;
import java.util.List;

// Placeholder for the functionality that could be shared across Create/Read/Update/Delete/List Handlers

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {
  protected static final int GLOBAL_CLUSTER_ID_MAX_LENGTH = 63;
  protected static final int PAUSE_TIME_SECONDS = 60;
  protected static final String STACK_NAME = "rds";
  protected static final String RESOURCE_IDENTIFIER = "globalcluster";
  protected static final Constant BACKOFF_STRATEGY = Constant.of().timeout(Duration.ofMinutes(180L)).delay(Duration.ofSeconds(30L)).build();
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
                                              final ResourceModel model) {
    // describe status of a resource to make sure it's ready
    try {
      final List<GlobalCluster> globalClusters =
              proxyClient.injectCredentialsAndInvokeV2(
                      Translator.describeGlobalClustersRequest(model),
                      proxyClient.client()::describeGlobalClusters).globalClusters();
      if(globalClusters == null || globalClusters.size() == 0) {
        return false;
      }
      return GlobalClusterStatus.Available.equalsString(globalClusters.get(0).status());
    } catch (GlobalClusterNotFoundException e) {
      return false;
    } catch (Exception e) {
      throw new CfnNotStabilizedException(MESSAGE_FORMAT_FAILED_TO_STABILIZE, model.getGlobalClusterIdentifier(), e);
    }
  }


  protected boolean isDeleted(final ResourceModel model,
                              final ProxyClient<RdsClient> proxyClient) {
    try {
      proxyClient.injectCredentialsAndInvokeV2(
              Translator.describeGlobalClustersRequest(model),
              proxyClient.client()::describeGlobalClusters);
      return false;
    } catch (GlobalClusterNotFoundException e) {
      return true;
    }
  }

  protected boolean validateSourceDBClusterIdentifier(final ResourceModel model) {
    //if sourceDBClusterIdentifier is empty, create handler creates an empty global cluster, proceed with creation
    //only arn is allowed to have ':', use this to check if input is in arn format
    return StringUtils.isNullOrEmpty(model.getSourceDBClusterIdentifier()) || model.getSourceDBClusterIdentifier().contains(":");
  }

  protected boolean globalClusterContainsOnlyMaster(final ResourceModel model, final ProxyClient<RdsClient> proxyClient) {
      try {
        final List<GlobalCluster> globalClusters =
                proxyClient.injectCredentialsAndInvokeV2(
                        Translator.describeGlobalClustersRequest(model),
                        proxyClient.client()::describeGlobalClusters).globalClusters();

        return globalClusters != null && globalClusters.size() > 0 && globalClusters.get(0).globalClusterMembers().size() == 1;
      }catch (GlobalClusterNotFoundException e) {
        return false;
      }
  }

  // DBCluster Stabilization
  protected boolean isDBClusterStabilized(final ProxyClient<RdsClient> proxyClient,
                                          final ResourceModel model,
                                          final DBClusterStatus expectedStatus) {
    // describe status of a resource to make sure it's ready
    // describe db cluster
    try {
      final Optional<DBCluster> dbCluster =
              proxyClient.injectCredentialsAndInvokeV2(
                      Translator.describeDbClustersRequest(model),
                      proxyClient.client()::describeDBClusters).dbClusters().stream().findFirst();

      if (!dbCluster.isPresent())
        throw new CfnNotFoundException(ResourceModel.TYPE_NAME, model.getSourceDBClusterIdentifier());

      return expectedStatus.equalsString(dbCluster.get().status());
    } catch (DbClusterNotFoundException e) {
      throw new CfnNotFoundException(ResourceModel.TYPE_NAME, e.getMessage());
    } catch (Exception e) {
      throw new CfnNotStabilizedException(MESSAGE_FORMAT_FAILED_TO_STABILIZE, model.getSourceDBClusterIdentifier(), e);
    }
  }

  protected ProgressEvent<ResourceModel, CallbackContext> waitForDBClusterAvailableStatus(
          final AmazonWebServicesClientProxy proxy,
          final ProxyClient<RdsClient> proxyClient,
          final ProgressEvent<ResourceModel, CallbackContext> progress) {
    // this is a stabilizer for dbcluster
    if(StringUtils.isNullOrEmpty(progress.getResourceModel().getSourceDBClusterIdentifier())) {
      return progress;
    }

    return proxy.initiate("rds::stabilize-dbcluster" + getClass().getSimpleName(), proxyClient, progress.getResourceModel(), progress.getCallbackContext())
            // only stabilization is necessary so this is a dummy call
            // Function.identity() takes ResourceModel as an input and returns (the same) ResourceModel
            // Function.identity() is roughly similar to `model -> model`
            .translateToServiceRequest(Function.identity())
            // this skips the call and goes directly to stabilization
            .makeServiceCall(EMPTY_CALL)
            .stabilize((resourceModel, response, proxyInvocation, model, callbackContext) ->
                    isDBClusterStabilized(proxyInvocation, resourceModel, DBClusterStatus.Available)).progress();
  }

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
                    isGlobalClusterStabilized(proxyInvocation, model)).progress();
  }

  protected ProgressEvent<ResourceModel, CallbackContext> removeFromGlobalCluster(final AmazonWebServicesClientProxy proxy,
                                                                                  final ProxyClient<RdsClient> proxyClient,
                                                                                  final ProgressEvent<ResourceModel, CallbackContext> progress) {

    if(!globalClusterContainsOnlyMaster(progress.getResourceModel(), proxyClient) || progress.getCallbackContext().isRemoved()) return progress;
    //check if sourceDbCluster is not null and is in format of Identifier
    return proxy.initiate("rds::remove-from-global-cluster", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
            .translateToServiceRequest(Translator::describeDbClustersRequest)
            .backoffDelay(BACKOFF_STRATEGY)
            .makeServiceCall((describeDbClustersRequest, proxyClient1) -> proxyClient1.injectCredentialsAndInvokeV2(describeDbClustersRequest, proxyClient1.client()::describeDBClusters))
            .done((describeDbClusterRequest, describeDbClusterResponse, proxyClient2, resourceModel, callbackContext) -> {
              final String arn = describeDbClusterResponse.dbClusters().get(0).dbClusterArn();
              proxyClient2.injectCredentialsAndInvokeV2(Translator.removeFromGlobalClusterRequest(resourceModel, arn), proxyClient2.client()::removeFromGlobalCluster);
              callbackContext.setRemoved(true);
              return ProgressEvent.defaultInProgressHandler(callbackContext, PAUSE_TIME_SECONDS, resourceModel);
            });
  }


  protected ProgressEvent<ResourceModel, CallbackContext> createGlobalClusterWithSourceDBCluster(final AmazonWebServicesClientProxy proxy,
                                                                                                 final ProxyClient<RdsClient> proxyClient,
                                                                                                 final ProgressEvent<ResourceModel, CallbackContext> progress) {

    if(progress.getCallbackContext().isGlobalClusterCreated()) return progress;
    //check if sourceDbCluster is not null and is in format of Identifier
    return proxy.initiate("rds::create-global-cluster", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
            .translateToServiceRequest(Translator::describeDbClustersRequest)
            .backoffDelay(BACKOFF_STRATEGY)
            .makeServiceCall((describeDbClustersRequest, proxyClient1) -> proxyClient1.injectCredentialsAndInvokeV2(describeDbClustersRequest, proxyClient1.client()::describeDBClusters))
            .done((describeDbClusterRequest, describeDbClusterResponse, proxyClient2, resourceModel, callbackContext) -> {
              final String arn = describeDbClusterResponse.dbClusters().get(0).dbClusterArn();
              try {
                proxyClient2.injectCredentialsAndInvokeV2(Translator.createGlobalClusterRequest(resourceModel, arn), proxyClient2.client()::createGlobalCluster);
                callbackContext.setGlobalClusterCreated(true);
              } catch (GlobalClusterAlreadyExistsException e) {
                throw new CfnAlreadyExistsException(e);
              }
              return ProgressEvent.defaultInProgressHandler(callbackContext, PAUSE_TIME_SECONDS, resourceModel);
            });
   }

  protected ProgressEvent<ResourceModel, CallbackContext> createGlobalCluster(final AmazonWebServicesClientProxy proxy,
                                                                                                 final ProxyClient<RdsClient> proxyClient,
                                                                                                 final ProgressEvent<ResourceModel, CallbackContext> progress) {

      return proxy.initiate("rds::create-global-cluster", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
              // request to create global cluster
              .translateToServiceRequest(Translator::createGlobalClusterRequest)
              .backoffDelay(BACKOFF_STRATEGY)
              .makeServiceCall((createGlobalClusterRequest, proxyClient1) -> {
                try{
                  return proxyClient1.injectCredentialsAndInvokeV2(createGlobalClusterRequest, proxyClient1.client()::createGlobalCluster);
                } catch(GlobalClusterAlreadyExistsException e) {
                  throw new CfnAlreadyExistsException(e);
                }
              })
              .progress();
    }
}
