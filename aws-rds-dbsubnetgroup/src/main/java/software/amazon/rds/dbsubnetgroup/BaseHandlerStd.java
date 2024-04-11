package software.amazon.rds.dbsubnetgroup;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DbSubnetGroupAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DbSubnetGroupDoesNotCoverEnoughAZsException;
import software.amazon.awssdk.services.rds.model.DbSubnetGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.DbSubnetGroupQuotaExceededException;
import software.amazon.awssdk.services.rds.model.InvalidDbSubnetGroupStateException;
import software.amazon.awssdk.services.rds.model.InvalidSubnetException;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.error.ErrorRuleSet;
import software.amazon.rds.common.error.ErrorStatus;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.logging.LoggingProxyClient;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.common.printer.FilteredJsonPrinter;

import java.time.Instant;
import java.util.Collection;

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {
    protected static final int DB_SUBNET_GROUP_NAME_LENGTH = 255;
    protected static final String DB_SUBNET_GROUP_STATUS_COMPLETE = "Complete";
    protected static final String STACK_NAME = "rds";
    protected static final String RESOURCE_IDENTIFIER = "dbsubnetgroup";
    protected static final String DB_SUBNET_GROUP_REQUEST_STARTED_AT = "dbsubnetgroup-request-started-at";
    protected static final String DB_SUBNET_GROUP_REQUEST_IN_PROGRESS_AT = "dbsubnetgroup-request-in-progress-at";
    protected static final String DB_SUBNET_GROUP_STABILIZATION_TIME = "dbsubnetgroup-stabilization-time";

    protected static final ErrorRuleSet DEFAULT_DB_SUBNET_GROUP_ERROR_RULE_SET = ErrorRuleSet
            .extend(Commons.DEFAULT_ERROR_RULE_SET)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    DbSubnetGroupAlreadyExistsException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    DbSubnetGroupNotFoundException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ServiceLimitExceeded),
                    DbSubnetGroupQuotaExceededException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
                    InvalidDbSubnetGroupStateException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
                    DbSubnetGroupDoesNotCoverEnoughAZsException.class,
                    InvalidSubnetException.class)
            .build();

    protected HandlerConfig config;

    protected RequestLogger requestLogger;

    private final FilteredJsonPrinter PARAMETERS_FILTER = new FilteredJsonPrinter();

    public BaseHandlerStd(final HandlerConfig config) {
        super();
        this.config = config;
    }

    @Override
    public final ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger) {
        final CallbackContext context = callbackContext != null ? callbackContext : new CallbackContext();
        context.setDbSubnetGroupArn(Translator.buildParameterGroupArn(request).toString());
        return RequestLogger.handleRequest(
                logger,
                request,
                PARAMETERS_FILTER,
                requestLogger -> handleRequest(
                        proxy,
                        request,
                        context,
                        new LoggingProxyClient<>(requestLogger, proxy.newProxy(new ClientProvider()::getClient)),
                        requestLogger
                ));
    }

    protected abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            AmazonWebServicesClientProxy proxy,
            ResourceHandlerRequest<ResourceModel> request,
            CallbackContext callbackContext,
            ProxyClient<RdsClient> proxyClient);

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            AmazonWebServicesClientProxy proxy,
            ResourceHandlerRequest<ResourceModel> request,
            CallbackContext callbackContext,
            ProxyClient<RdsClient> proxyClient,
            final RequestLogger requestLogger
    ) {
        this.requestLogger = requestLogger;
        resourceStabilizationTime(callbackContext);
        return handleRequest(proxy, request, callbackContext, proxyClient);
    }

    protected boolean isStabilized(final ResourceModel model, final ProxyClient<RdsClient> proxyClient) {
        final String status = proxyClient.injectCredentialsAndInvokeV2(
                        Translator.describeDbSubnetGroupsRequest(model),
                        proxyClient.client()::describeDBSubnetGroups)
                .dbSubnetGroups().stream().findFirst().get().subnetGroupStatus();
        return status.equals(DB_SUBNET_GROUP_STATUS_COMPLETE);
    }

    private void resourceStabilizationTime(final CallbackContext callbackContext) {
        callbackContext.timestampOnce(DB_SUBNET_GROUP_REQUEST_STARTED_AT, Instant.now());
        callbackContext.timestamp(DB_SUBNET_GROUP_REQUEST_IN_PROGRESS_AT, Instant.now());
        callbackContext.calculateTimeDelta(DB_SUBNET_GROUP_STABILIZATION_TIME,
                callbackContext.getTimestamp(DB_SUBNET_GROUP_REQUEST_STARTED_AT),
                callbackContext.getTimestamp(DB_SUBNET_GROUP_REQUEST_IN_PROGRESS_AT));
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

    protected ProgressEvent<ResourceModel, CallbackContext> updateTags(
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet previousTags,
            final Tagging.TagSet desiredTags
    ) {
        final Collection<Tag> effectivePreviousTags = Tagging.translateTagsToSdk(previousTags);
        final Collection<Tag> effectiveDesiredTags = Tagging.translateTagsToSdk(desiredTags);

        final Collection<Tag> tagsToRemove = Tagging.exclude(effectivePreviousTags, effectiveDesiredTags);
        final Collection<Tag> tagsToAdd = Tagging.exclude(effectiveDesiredTags, effectivePreviousTags);

        if (tagsToAdd.isEmpty() && tagsToRemove.isEmpty()) {
            return progress;
        }

        final Tagging.TagSet rulesetTagsToAdd = Tagging.exclude(desiredTags, previousTags);
        final Tagging.TagSet rulesetTagsToRemove = Tagging.exclude(previousTags, desiredTags);

        if (tagsToAdd.isEmpty() && tagsToRemove.isEmpty()) {
            return progress;
        }

        try {
            String arn = progress.getCallbackContext().getDbSubnetGroupArn();
            Tagging.removeTags(rdsProxyClient, arn, Tagging.translateTagsToSdk(tagsToRemove));
            Tagging.addTags(rdsProxyClient, arn, Tagging.translateTagsToSdk(tagsToAdd));
        } catch (Exception exception) {
            return Commons.handleException(
                    progress,
                    exception,
                    DEFAULT_DB_SUBNET_GROUP_ERROR_RULE_SET.extendWith(
                            Tagging.getUpdateTagsAccessDeniedRuleSet(
                                    rulesetTagsToAdd,
                                    rulesetTagsToRemove
                            )
                    ),
                    requestLogger
            );
        }

        return progress;
    }
}
