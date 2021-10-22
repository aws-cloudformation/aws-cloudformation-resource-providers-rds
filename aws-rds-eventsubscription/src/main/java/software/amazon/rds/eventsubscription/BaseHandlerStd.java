package software.amazon.rds.eventsubscription;

import static software.amazon.rds.eventsubscription.Translator.addTagsToResourceRequest;
import static software.amazon.rds.eventsubscription.Translator.listTagsForResourceRequest;
import static software.amazon.rds.eventsubscription.Translator.mapToTags;
import static software.amazon.rds.eventsubscription.Translator.removeTagsFromResourceRequest;

import com.amazonaws.util.StringUtils;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import software.amazon.awssdk.core.exception.RetryableException;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.EventSubscriptionQuotaExceededException;
import software.amazon.awssdk.services.rds.model.InvalidEventSubscriptionStateException;
import software.amazon.awssdk.services.rds.model.SubscriptionAlreadyExistException;
import software.amazon.awssdk.services.rds.model.SubscriptionNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {
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

  protected abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(
      final AmazonWebServicesClientProxy proxy,
      final ResourceHandlerRequest<ResourceModel> request,
      final CallbackContext callbackContext,
      final ProxyClient<RdsClient> proxyClient,
      final Logger logger);


  protected boolean isStabilized(final ResourceModel model, final ProxyClient<RdsClient> proxyClient) {
    final String status = proxyClient.injectCredentialsAndInvokeV2(
        Translator.describeEventSubscriptionsRequest(model),
        proxyClient.client()::describeEventSubscriptions)
        .eventSubscriptionsList().stream().findFirst().get().status();
    return status.equals("active");
  }

  protected ProgressEvent<ResourceModel, CallbackContext> waitForEventSubscription(
      final AmazonWebServicesClientProxy proxy,
      final ProxyClient<RdsClient> proxyClient,
      final ProgressEvent<ResourceModel, CallbackContext> progress) {
    return proxy.initiate("rds::stabilize-event-subscription" + getClass().getSimpleName(), proxyClient, progress.getResourceModel(), progress.getCallbackContext())
        // only stabilization is necessary so this is a dummy call
        // Function.identity() takes ResourceModel as an input and returns (the same) ResourceModel
        // Function.identity() is roughly similar to `model -> model`
        .translateToServiceRequest(Function.identity())
        // this skips the call and goes directly to stabilization
        .makeServiceCall(EMPTY_CALL)
        .stabilize((resourceModel, response, proxyInvocation, model, callbackContext) -> isStabilized(resourceModel, proxyInvocation)).progress();
  }

  protected ProgressEvent<ResourceModel, CallbackContext> tagResource(
      final AmazonWebServicesClientProxy proxy,
      final ProxyClient<RdsClient> proxyClient,
      final ProgressEvent<ResourceModel, CallbackContext> progress,
      final Map<String, String> tags) {
    return proxy.initiate("rds::tag-event-subscription", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
        .translateToServiceRequest(Translator::describeEventSubscriptionsRequest)
        .makeServiceCall((describeEventSubscriptionsRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(describeEventSubscriptionsRequest, proxyInvocation.client()::describeEventSubscriptions))
        .done((describeEventSubscriptionsRequest, describeEventSubscriptionsResponse, proxyInvocation, resourceModel, context) -> {
          final String arn = describeEventSubscriptionsResponse.eventSubscriptionsList().stream().findFirst().get().eventSubscriptionArn();

          final Set<Tag> currentTags = new HashSet<>(Optional.ofNullable(mapToTags(tags)).orElse(Collections.emptySet()));

          final Set<Tag> existingTags = Translator.translateTagsFromSdk(proxyInvocation.injectCredentialsAndInvokeV2(listTagsForResourceRequest(arn), proxyInvocation.client()::listTagsForResource).tagList());

          final Set<Tag> tagsToRemove = Sets.difference(existingTags, currentTags);
          final Set<Tag> tagsToAdd = Sets.difference(currentTags, existingTags);

          proxyInvocation.injectCredentialsAndInvokeV2(removeTagsFromResourceRequest(arn, tagsToRemove), proxyInvocation.client()::removeTagsFromResource);
          proxyInvocation.injectCredentialsAndInvokeV2(addTagsToResourceRequest(arn, tagsToAdd), proxyInvocation.client()::addTagsToResource);
          return ProgressEvent.progress(resourceModel, context);
        });
  }

  protected ProgressEvent<ResourceModel, CallbackContext> handleException(
      final ProgressEvent<ResourceModel, CallbackContext> progress,
      final Exception e
  ) {
    if (e instanceof SubscriptionAlreadyExistException) {
      return ProgressEvent.defaultFailureHandler(e, HandlerErrorCode.AlreadyExists);
    } else if (e instanceof SubscriptionNotFoundException) {
      return ProgressEvent.defaultFailureHandler(e, HandlerErrorCode.NotFound);
    } else if (e instanceof EventSubscriptionQuotaExceededException) {
      return ProgressEvent.defaultFailureHandler(e, HandlerErrorCode.ServiceLimitExceeded);
    } else if (e instanceof InvalidEventSubscriptionStateException) {
      throw RetryableException.builder().cause(e).build();
    } else if (!StringUtils.isNullOrEmpty(e.getMessage()) && e.getMessage().equals("Rate exceeded")) {
      return ProgressEvent.defaultFailureHandler(e, HandlerErrorCode.Throttling); //Request throttled from rds service
    }
    return ProgressEvent.defaultFailureHandler(e, HandlerErrorCode.InternalFailure);
  }

  protected <K, V> Map<K, V> mergeMaps(Map<K, V> m1, Map<K, V> m2) {
    final Map<K, V> result = new HashMap<>();
    if (m1 != null) {
      result.putAll(m1);
    }
    if (m2 != null) {
      result.putAll(m2);
    }
    return result;
  }
}
