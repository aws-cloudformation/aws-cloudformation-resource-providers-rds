package software.amazon.rds.optiongroup;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {
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

  protected abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(
    final AmazonWebServicesClientProxy proxy,
    final ResourceHandlerRequest<ResourceModel> request,
    final CallbackContext callbackContext,
    final ProxyClient<RdsClient> proxyClient,
    final Logger logger);

  /*
   * Common or abstract methods and functions should be in this class
   * */

  protected ProgressEvent<ResourceModel, CallbackContext> updateOptionGroup(
      final AmazonWebServicesClientProxy proxy,
      final ProxyClient<RdsClient> proxyClient,
      final ProgressEvent<ResourceModel, CallbackContext> progressEvent) {
    return proxy.initiate("rds::update-option-group", proxyClient, progressEvent.getResourceModel(), progressEvent.getCallbackContext())
        .request(Translator::modifyOptionGroupRequest)
        .call((modifyOptionGroupRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(modifyOptionGroupRequest, proxyInvocation.client()::modifyOptionGroup))
        .progress();
  }
}
