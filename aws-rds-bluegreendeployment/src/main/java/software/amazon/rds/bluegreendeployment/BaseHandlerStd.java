package software.amazon.rds.bluegreendeployment;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.BlueGreenDeployment;
import software.amazon.awssdk.services.rds.model.BlueGreenDeploymentAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.BlueGreenDeploymentNotFoundException;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DescribeBlueGreenDeploymentsResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.common.error.ErrorRuleSet;
import software.amazon.rds.common.error.ErrorStatus;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.logging.LoggingProxyClient;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.common.printer.FilteredJsonPrinter;
import software.amazon.rds.common.printer.JsonPrinter;

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {
    public static final String RESOURCE_IDENTIFIER = "bluegreendeployment";
    public static final String STACK_NAME = "rds";
    protected static final int RESOURCE_ID_MAX_LENGTH = 63;

    private final JsonPrinter PARAMETERS_FILTER = new FilteredJsonPrinter();

    protected static final ErrorRuleSet DEFAULT_BLUE_GREEN_DEPLOYMENT_ERROR_RULE_SET = ErrorRuleSet
            .extend(Commons.DEFAULT_ERROR_RULE_SET)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
                    BlueGreenDeploymentAlreadyExistsException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
                    BlueGreenDeploymentNotFoundException.class)
            .build();

    protected final static HandlerConfig DEFAULT_HANDLER_CONFIG = HandlerConfig.builder()
            .probingEnabled(true)
            .backoff(Constant.of().delay(Duration.ofSeconds(30)).timeout(Duration.ofMinutes(180)).build())
            .build();

    protected HandlerConfig config;

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
                        requestLogger));
    }

    protected abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            AmazonWebServicesClientProxy proxy,
            ResourceHandlerRequest<ResourceModel> request,
            CallbackContext callbackContext,
            ProxyClient<RdsClient> client,
            RequestLogger logger
    );

    protected boolean isBlueGreenDeploymentStabilized(
            final ProxyClient<RdsClient> client,
            final ResourceModel model
    ) {
        final BlueGreenDeployment blueGreenDeployment = fetchBlueGreenDeployment(client, model);

        if (!isBlueGreenDeploymentAvailable(blueGreenDeployment)) {
            return false;
        }

        if (!isBlueGreenDeploymentTasksComplete(blueGreenDeployment)) {
            return false;
        }

        if (blueGreenDeployment.source() != null) {
            if (!isDBInstanceAvailable(client, blueGreenDeployment.source())) {
                return false;
            }
        }

        if (blueGreenDeployment.target() != null) {
            if (!isDBInstanceAvailable(client, blueGreenDeployment.target())) {
                return false;
            }
        }

        return true;
    }

    protected boolean isBlueGreenDeploymentTasksComplete(final BlueGreenDeployment blueGreenDeployment) {
        return Optional.ofNullable(blueGreenDeployment.tasks()).orElse(Collections.emptyList())
                .stream()
                .allMatch(task -> BlueGreenDeploymentTaskStatus.Complete.equalsString(task.status()));
    }

    protected boolean isDBInstanceAvailable(
            final ProxyClient<RdsClient> client,
            final String dbInstanceIdentifier
    ) {
        final DBInstance dbInstance = fetchDBInstance(client, dbInstanceIdentifier);
        return DBInstanceStatus.Available.equalsString(dbInstance.dbInstanceStatus());
    }

    protected boolean isBlueGreenDeploymentAvailable(final BlueGreenDeployment blueGreenDeployment) {
        final String status = blueGreenDeployment.status();
        return BlueGreenDeploymentStatus.Available.equalsString(status) ||
                BlueGreenDeploymentStatus.SwitchoverCompleted.equalsString(status);
    }

    protected boolean isBlueGreenDeploymentDeleted(
            final ProxyClient<RdsClient> client,
            final ResourceModel model
    ) {
        try {
            fetchBlueGreenDeployment(client, model);
            return false;
        } catch (BlueGreenDeploymentNotFoundException exception) {
            return true;
        }
    }

    protected BlueGreenDeployment fetchBlueGreenDeployment(
            final ProxyClient<RdsClient> client,
            final ResourceModel model
    ) {
        final DescribeBlueGreenDeploymentsResponse response = client.injectCredentialsAndInvokeV2(
                Translator.describeBlueGreenDeploymentsRequest(model),
                client.client()::describeBlueGreenDeployments
        );
        return response.blueGreenDeployments().get(0);
    }

    protected DBInstance fetchDBInstance(
            final ProxyClient<RdsClient> client,
            final String dbInstanceIdentifier
    ) {
        final DescribeDbInstancesRequest request = DescribeDbInstancesRequest.builder()
                .dbInstanceIdentifier(dbInstanceIdentifier)
                .build();
        final DescribeDbInstancesResponse response = client.injectCredentialsAndInvokeV2(
                request,
                client.client()::describeDBInstances
        );
        return response.dbInstances().get(0);
    }
}
