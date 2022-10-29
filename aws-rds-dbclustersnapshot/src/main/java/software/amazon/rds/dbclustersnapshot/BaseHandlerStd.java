package software.amazon.rds.dbclustersnapshot;

import software.amazon.awssdk.core.SdkClient;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.logging.LoggingProxyClient;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.common.printer.FilteredJsonPrinter;

// Placeholder for the functionality that could be shared across Create/Read/Update/Delete/List Handlers

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {
  protected final HandlerConfig config;

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
}
