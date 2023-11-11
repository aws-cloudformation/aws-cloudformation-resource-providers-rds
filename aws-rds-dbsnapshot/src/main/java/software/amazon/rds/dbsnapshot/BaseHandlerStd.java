package software.amazon.rds.dbsnapshot;

import java.time.Duration;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBSnapshot;
import software.amazon.awssdk.services.rds.model.DbSnapshotAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DbSnapshotNotFoundException;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.error.ErrorRuleSet;
import software.amazon.rds.common.error.ErrorStatus;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.logging.LoggingProxyClient;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.common.printer.FilteredJsonPrinter;

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {

    protected static final String RESOURCE_IDENTIFIER = "dbsnapshot";
    protected static final String STACK_NAME = "rds";
    protected static final int RESOURCE_ID_MAX_LENGTH = 63;

    private final FilteredJsonPrinter PARAMETERS_FILTER = new FilteredJsonPrinter();

    protected final static HandlerConfig DEFAULT_HANDLER_CONFIG = HandlerConfig.builder()
            .backoff(Constant.of().delay(Duration.ofSeconds(30)).timeout(Duration.ofMinutes(180)).build())
            .build();

    protected final static ErrorRuleSet DEFAULT_DB_SNAPSHOT_ERROR_RULE_SET = ErrorRuleSet.extend(Commons.DEFAULT_ERROR_RULE_SET)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    ErrorCode.DBSnapshotAlreadyExists)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    ErrorCode.DBSnapshotNotFound)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    DbSnapshotAlreadyExistsException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    DbSnapshotNotFoundException.class)
            .build();

    protected final HandlerConfig config;

    public BaseHandlerStd(final HandlerConfig config) {
        super();
        this.config = config;
    }

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger
    ) {
        final CallbackContext context = callbackContext != null ? callbackContext : new CallbackContext();
        return RequestLogger.handleRequest(
                logger,
                request,
                PARAMETERS_FILTER,
                requestLogger -> handleRequest(proxy,
                        request,
                        context,
                        new LoggingProxyClient<>(requestLogger, proxy.newProxy(new ClientProvider()::getClient)),
                        logger));
    }

    protected abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            AmazonWebServicesClientProxy proxy,
            ResourceHandlerRequest<ResourceModel> request,
            CallbackContext callbackContext,
            ProxyClient<RdsClient> client,
            Logger logger
    );

    protected ProgressEvent<ResourceModel, CallbackContext> updateDBSnapshot(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> client,
            final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        return proxy.initiate("rds::modify-db-snapshot", client, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(Translator::modifyDBSnapshotRequest)
                .makeServiceCall((modifyRequest, proxyClient) -> proxyClient.injectCredentialsAndInvokeV2(
                        modifyRequest,
                        proxyClient.client()::modifyDBSnapshot
                ))
                .stabilize((modifyRequest, modifyResponse, proxyClient, model, context) -> isDBSnapshotStabilized(proxyClient, model))
                .handleError((modifyRequest, exception, proxyClient, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        DEFAULT_DB_SNAPSHOT_ERROR_RULE_SET
                ))
                .progress();
    }

    protected boolean shouldUpdateAfterCreate(final ResourceModel model) {
        return StringUtils.hasValue(model.getOptionGroupName()) ||
                StringUtils.hasValue(model.getEngineVersion());
    }

    protected boolean isDBSnapshotStabilized(
            final ProxyClient<RdsClient> proxyClient,
            final ResourceModel model
    ) {
        final DBSnapshot dbSnapshot = fetchDBSnapshot(proxyClient, model);

        return isDBSnapshotAvailable(dbSnapshot);
    }

    protected boolean isDBSnapshotAvailable(final DBSnapshot dbSnapshot) {
        return "available".equalsIgnoreCase(dbSnapshot.status());
    }

    protected DBSnapshot fetchDBSnapshot(
            final ProxyClient<RdsClient> proxyClient,
            final ResourceModel model
    ) {
        final DescribeDbSnapshotsResponse response = proxyClient.injectCredentialsAndInvokeV2(
                Translator.describeDBSnapshotRequest(model),
                proxyClient.client()::describeDBSnapshots
        );
        return response.dbSnapshots().get(0);
    }

    protected Boolean isDBSnapshotDeleted(
            final ProxyClient<RdsClient> client,
            final ResourceModel model
    ) {
        try {
            fetchDBSnapshot(client, model);
            return false;
        } catch (DbSnapshotNotFoundException e) {
            return true;
        }
    }
}
