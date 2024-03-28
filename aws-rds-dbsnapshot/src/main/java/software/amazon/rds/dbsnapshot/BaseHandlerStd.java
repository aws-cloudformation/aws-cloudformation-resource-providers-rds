package software.amazon.rds.dbsnapshot;

import java.util.function.BiFunction;
import java.util.function.Function;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBSnapshot;
import software.amazon.awssdk.services.rds.model.DbInstanceNotFoundException;
import software.amazon.awssdk.services.rds.model.DbSnapshotAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DbSnapshotNotFoundException;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsResponse;
import software.amazon.awssdk.services.rds.model.InvalidDbInstanceStateException;
import software.amazon.awssdk.services.rds.model.InvalidDbSnapshotStateException;
import software.amazon.awssdk.services.rds.model.SnapshotQuotaExceededException;
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

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {
    protected static final BiFunction<ResourceModel, ProxyClient<RdsClient>, ResourceModel> EMPTY_CALL = (model, proxyClient) -> model;

    protected static final String AVAILABLE_STATE = "available";
    protected static final String STACK_NAME = "rds";
    protected static final String RESOURCE_IDENTIFIER = "dbsnapshot";
    protected static final int MAX_LENGTH_DB_SNAPSHOT = 255;

    protected static final ErrorRuleSet DEFAULT_DB_SNAPSHOT_ERROR_RULE_SET = ErrorRuleSet.builder()
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    DbSnapshotAlreadyExistsException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    DbSnapshotNotFoundException.class,
                    DbInstanceNotFoundException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ServiceLimitExceeded),
                    SnapshotQuotaExceededException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
                    InvalidDbSnapshotStateException.class,
                    InvalidDbInstanceStateException.class)
            .build()
            .orElse(Commons.DEFAULT_ERROR_RULE_SET);

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


    protected boolean isStabilized(final ResourceModel model, final ProxyClient<RdsClient> proxyClient) {
        DBSnapshot dbSnapshot = fetchDBSnapshot(model, proxyClient);
        return dbSnapshot.status().equals(AVAILABLE_STATE);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> waitForDbSnapshot(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress) {
        return proxy.initiate("rds::stabilize-db-snapshot" + getClass().getSimpleName(), proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                // only stabilization is necessary so this is a dummy call
                // Function.identity() takes ResourceModel as an input and returns (the same) ResourceModel
                // Function.identity() is roughly similar to `model -> model`
                .translateToServiceRequest(Function.identity())
                // this skips the call and goes directly to stabilization
                .makeServiceCall(EMPTY_CALL)
                .stabilize((resourceModel, response, proxyInvocation, model, callbackContext) -> isStabilized(resourceModel, proxyInvocation)).progress();
    }

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

        final String arn = progress.getResourceModel().getDBSnapshotArn();

        try {
            Tagging.removeTags(rdsProxyClient, arn, Tagging.translateTagsToSdk(tagsToRemove));
            Tagging.addTags(rdsProxyClient, arn, Tagging.translateTagsToSdk(tagsToAdd));
        } catch (Exception exception) {
            return Commons.handleException(
                    progress,
                    exception,
                    Tagging.bestEffortErrorRuleSet(tagsToAdd, tagsToRemove, Tagging.SOFT_FAIL_IN_PROGRESS_TAGGING_ERROR_RULE_SET, Tagging.HARD_FAIL_TAG_ERROR_RULE_SET)
                            .orElse(DEFAULT_DB_SNAPSHOT_ERROR_RULE_SET)
            );
        }

        return progress;
    }

    protected DBSnapshot fetchDBSnapshot(
            final ResourceModel model,
            final ProxyClient<RdsClient> proxyClient
    ) {
        final DescribeDbSnapshotsResponse response = proxyClient.injectCredentialsAndInvokeV2(
                Translator.describeDbSnapshotsRequest(model),
                proxyClient.client()::describeDBSnapshots
        );

        return response.dbSnapshots().stream().findFirst().get();
    }

}
