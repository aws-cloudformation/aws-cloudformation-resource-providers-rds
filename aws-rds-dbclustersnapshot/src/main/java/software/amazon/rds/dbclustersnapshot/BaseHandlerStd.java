package software.amazon.rds.dbclustersnapshot;

import software.amazon.awssdk.core.SdkClient;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBClusterSnapshot;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterSnapshotsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.error.ErrorRuleSet;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.logging.LoggingProxyClient;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.common.printer.FilteredJsonPrinter;

// Placeholder for the functionality that could be shared across Create/Read/Update/Delete/List Handlers

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {
  protected final HandlerConfig config;

  protected static final String AVAILABLE_STATE = "available";
  protected static final String STACK_NAME = "rds";
  protected static final String RESOURCE_IDENTIFIER = "dbclustersnapshot";
  protected static final int MAX_LENGTH_DB_SNAPSHOT = 255; // FIXME: Really?

  protected static final ErrorRuleSet DEFAULT_DB_SNAPSHOT_ERROR_RULE_SET = ErrorRuleSet
          .extend(Commons.DEFAULT_ERROR_RULE_SET).build(); // FIXME fr fr

  public BaseHandlerStd(final HandlerConfig config) {
    super();
    this.config = config;
  }

  private final FilteredJsonPrinter PARAMETERS_FILTER = new FilteredJsonPrinter();

  @Override
  public final ProgressEvent<ResourceModel, CallbackContext> handleRequest(
          final AmazonWebServicesClientProxy proxy,
          final ResourceHandlerRequest<ResourceModel> request,
          final CallbackContext callbackContext,
          final Logger logger) {
    return RequestLogger.handleRequest(
            logger,
            request,
            PARAMETERS_FILTER,
            requestLogger -> handleRequest(
                    proxy,
                    request,
                    callbackContext != null ? callbackContext : new CallbackContext(),
                    new LoggingProxyClient<>(requestLogger, proxy.newProxy(ClientBuilder::getClient)),
                    logger
            ));
  }

  protected abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(
          final AmazonWebServicesClientProxy proxy,
          final ResourceHandlerRequest<ResourceModel> request,
          final CallbackContext callbackContext,
          final ProxyClient<RdsClient> proxyClient,
          final Logger logger);

  protected ProgressEvent<ResourceModel, CallbackContext> updateTags(
          final AmazonWebServicesClientProxy proxy,
          final ProxyClient<RdsClient> rdsProxyClient,
          final ProgressEvent<ResourceModel, CallbackContext> progress,
          final Tagging.TagSet previousTags,
          final Tagging.TagSet desiredTags) {
    final Tagging.TagSet tagsToAdd = Tagging.exclude(desiredTags, previousTags);
    final Tagging.TagSet tagsToRemove = Tagging.exclude(previousTags, desiredTags);

    if (tagsToAdd.isEmpty() && tagsToRemove.isEmpty()) {
      return progress;
    }

    final String arn = progress.getResourceModel().getDBClusterSnapshotArn();

    try {
      Tagging.removeTags(rdsProxyClient, arn, Tagging.translateTagsToSdk(tagsToRemove));
      Tagging.addTags(rdsProxyClient, arn, Tagging.translateTagsToSdk(tagsToAdd));
    } catch (Exception exception) {
      return Commons.handleException(
              progress,
              exception,
              DEFAULT_DB_SNAPSHOT_ERROR_RULE_SET.extendWith(
                      Tagging.bestEffortErrorRuleSet(
                              tagsToAdd,
                              tagsToRemove,
                              Tagging.SOFT_FAIL_IN_PROGRESS_TAGGING_ERROR_RULE_SET,
                              Tagging.HARD_FAIL_TAG_ERROR_RULE_SET
                      )
              )
      );
    }

    return progress;
  }

  protected boolean isStabilized(final ResourceModel model, final ProxyClient<RdsClient> proxyClient) {
    DBClusterSnapshot dbClusterSnapshot = fetchDBClusterSnapshot(model, proxyClient);
    return dbClusterSnapshot.status().equals(AVAILABLE_STATE);
  }

  protected DBClusterSnapshot fetchDBClusterSnapshot(
          final ResourceModel model,
          final ProxyClient<RdsClient> proxyClient
  ) {
    final DescribeDbClusterSnapshotsResponse response = proxyClient.injectCredentialsAndInvokeV2(
            Translator.describeDbClusterSnapshotsRequest(model),
            proxyClient.client()::describeDBClusterSnapshots
    );

    return response.dbClusterSnapshots().stream().findFirst().get();
  }
}
