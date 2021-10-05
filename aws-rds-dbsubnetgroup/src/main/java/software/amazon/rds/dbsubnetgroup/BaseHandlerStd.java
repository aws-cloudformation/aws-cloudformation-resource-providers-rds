package software.amazon.rds.dbsubnetgroup;

import static software.amazon.rds.dbsubnetgroup.Translator.addTagsToResourceRequest;
import static software.amazon.rds.dbsubnetgroup.Translator.listTagsForResourceRequest;
import static software.amazon.rds.dbsubnetgroup.Translator.mapToTags;
import static software.amazon.rds.dbsubnetgroup.Translator.removeTagsFromResourceRequest;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.Sets;
import software.amazon.awssdk.core.exception.RetryableException;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DbSubnetGroupAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DbSubnetGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.DbSubnetGroupQuotaExceededException;
import software.amazon.awssdk.services.rds.model.InvalidDbSubnetGroupStateException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {
    protected static final int DB_SUBNET_GROUP_NAME_LENGTH = 255;
    protected static final String DB_SUBNET_GROUP_STATUS_COMPLETE = "Complete";
    protected static final Constant CONSTANT = Constant.of().timeout(Duration.ofMinutes(120L))
            .delay(Duration.ofSeconds(30L)).build();

    @Override
    public final ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                             final ResourceHandlerRequest<ResourceModel> request,
                                                                             final CallbackContext callbackContext,
                                                                             final Logger logger) {
        return handleRequest(
                proxy,
                request,
                callbackContext != null ? callbackContext : new CallbackContext(),
                proxy.newProxy(ClientBuilder::getClient),
                logger);
    }

    protected abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            AmazonWebServicesClientProxy proxy,
            ResourceHandlerRequest<ResourceModel> request,
            CallbackContext callbackContext,
            ProxyClient<RdsClient> proxyClient,
            Logger logger);

    protected boolean isStabilized(final ResourceModel model, final ProxyClient<RdsClient> proxyClient) {
        final String status = proxyClient.injectCredentialsAndInvokeV2(
                Translator.describeDbSubnetGroupsRequest(model),
                proxyClient.client()::describeDBSubnetGroups)
                .dbSubnetGroups().stream().findFirst().get().subnetGroupStatus();
        return status.equals(DB_SUBNET_GROUP_STATUS_COMPLETE);
    }

    public ProgressEvent<ResourceModel, CallbackContext> handleException(final Exception e) {
        if (e instanceof DbSubnetGroupAlreadyExistsException) {
            return ProgressEvent.defaultFailureHandler(e, HandlerErrorCode.AlreadyExists);
        } else if (e instanceof DbSubnetGroupQuotaExceededException) {
            return ProgressEvent.defaultFailureHandler(e, HandlerErrorCode.ServiceLimitExceeded);
        } else if (e instanceof DbSubnetGroupNotFoundException) {
            return ProgressEvent.defaultFailureHandler(e, HandlerErrorCode.NotFound);
        } else if (e instanceof InvalidDbSubnetGroupStateException) {
            throw RetryableException.builder().cause(e).build(); // current behaviour is retry on InvalidDbSubnetGroupState exception
        } else if (e.getMessage().equals("Rate exceeded")) {
            return ProgressEvent.defaultFailureHandler(e, HandlerErrorCode.Throttling); //Request throttled from rds service
        }
        throw new CfnGeneralServiceException(e);
    }

    protected boolean isDeleted(final ResourceModel model,
                                final ProxyClient<RdsClient> proxyClient) {
        try {
            proxyClient.injectCredentialsAndInvokeV2(
                    Translator.describeDbSubnetGroupsRequest(model),
                    proxyClient.client()::describeDBSubnetGroups);
            return false;
        } catch (DbSubnetGroupNotFoundException e) {
            return true;
        }
    }

    protected ProgressEvent<ResourceModel, CallbackContext> tagResource(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Map<String, String> tags) {
        return proxy.initiate("rds::tag-dbsubnet-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(Translator::describeDbSubnetGroupsRequest)
                .makeServiceCall((describeDbSubnetGroupsRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(describeDbSubnetGroupsRequest, proxyInvocation.client()::describeDBSubnetGroups))
                .done((describeDbSubnetGroupsRequest, describeDbSubnetGroupsResponse, proxyInvocation, resourceModel, context) -> {
                    final String arn = describeDbSubnetGroupsResponse.dbSubnetGroups().stream().findFirst().get().dbSubnetGroupArn();

                    final Set<Tag> currentTags = mapToTags(tags);

                    final Set<Tag> existingTags = Translator.translateTagsFromSdk(proxyInvocation.injectCredentialsAndInvokeV2(listTagsForResourceRequest(arn), proxyInvocation.client()::listTagsForResource).tagList());

                    final Set<Tag> tagsToRemove = Sets.difference(existingTags, currentTags);
                    final Set<Tag> tagsToAdd = Sets.difference(currentTags, existingTags);

                    proxyInvocation.injectCredentialsAndInvokeV2(removeTagsFromResourceRequest(arn, tagsToRemove), proxyInvocation.client()::removeTagsFromResource);
                    proxyInvocation.injectCredentialsAndInvokeV2(addTagsToResourceRequest(arn, tagsToAdd), proxyInvocation.client()::addTagsToResource);
                    return ProgressEvent.progress(resourceModel, context);
                });
    }

    protected <K, V> Map<K, V> mergeMaps(Map<K, V> m1, Map<K, V> m2) {
        final Map<K, V> result = new HashMap<>();
        result.putAll(Optional.ofNullable(m1).orElse(Collections.emptyMap()));
        result.putAll(Optional.ofNullable(m2).orElse(Collections.emptyMap()));
        return result;
    }
}
