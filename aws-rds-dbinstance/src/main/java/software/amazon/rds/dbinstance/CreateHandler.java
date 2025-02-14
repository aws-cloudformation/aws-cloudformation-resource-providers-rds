package software.amazon.rds.dbinstance;

import com.amazonaws.util.StringUtils;
import org.apache.commons.lang3.BooleanUtils;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.SourceType;
import software.amazon.awssdk.utils.ImmutableMap;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.Events;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.HandlerMethod;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.logging.LoggingProxyClient;
import software.amazon.rds.common.request.RequestValidationException;
import software.amazon.rds.common.request.ValidatedRequest;
import software.amazon.rds.common.request.Validations;
import software.amazon.rds.common.util.IdentifierFactory;
import software.amazon.rds.dbinstance.client.ApiVersion;
import software.amazon.rds.dbinstance.client.RdsClientProvider;
import software.amazon.rds.dbinstance.client.VersionedProxyClient;
import software.amazon.rds.dbinstance.common.ErrorRuleSets;
import software.amazon.rds.dbinstance.common.create.DBInstanceFactory;
import software.amazon.rds.dbinstance.common.create.DBInstanceFactoryFactory;
import software.amazon.rds.dbinstance.util.ResourceModelHelper;
import software.amazon.rds.dbinstance.validators.OracleCustomSystemId;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;

public class CreateHandler extends BaseHandlerStd {

    private static final IdentifierFactory instanceIdentifierFactory = new IdentifierFactory(
        STACK_NAME,
        RESOURCE_IDENTIFIER,
        RESOURCE_ID_MAX_LENGTH
    );

    public CreateHandler() {
        this(DB_INSTANCE_HANDLER_CONFIG_36H);
    }

    public CreateHandler(final HandlerConfig config) {
        super(config);
    }

    final String handlerOperation = "CREATE";

    @Override
    protected void validateRequest(final ResourceHandlerRequest<ResourceModel> request) throws RequestValidationException {
        super.validateRequest(request);
        validateDeletionPolicyForClusterInstance(request);
        Validations.validateTimestamp(request.getDesiredResourceState().getRestoreTime());

        OracleCustomSystemId.validateRequest(request.getDesiredResourceState());
    }

    private void validateDeletionPolicyForClusterInstance(final ResourceHandlerRequest<ResourceModel> request) throws RequestValidationException {
        if (DBInstancePredicates.isDBClusterMember(request.getDesiredResourceState()) && BooleanUtils.isTrue(request.getSnapshotRequested())) {
            throw new RequestValidationException(ILLEGAL_DELETION_POLICY_ERROR);
        }
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ValidatedRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final VersionedProxyClient<RdsClient> rdsProxyClient,
        final VersionedProxyClient<Ec2Client> ec2ProxyClient
    ) {
        final ResourceModel model = request.getDesiredResourceState();
        final Collection<DBInstanceRole> desiredRoles = model.getAssociatedRoles();
        callbackContext.setCurrentRegion(request.getRegion());

        if (StringUtils.isNullOrEmpty(model.getDBInstanceIdentifier())) {
            model.setDBInstanceIdentifier(instanceIdentifierFactory.newIdentifier()
                .withStackId(request.getStackId())
                .withResourceId(request.getLogicalResourceIdentifier())
                .withRequestToken(request.getClientRequestToken())
                .toString());
        }

        final Tagging.TagSet allTags = Tagging.TagSet.builder()
            .systemTags(Tagging.translateTagsToSdk(request.getSystemTags()))
            .stackTags(Tagging.translateTagsToSdk(request.getDesiredResourceTags()))
            .resourceTags(Translator.translateTagsToSdk(request.getDesiredResourceState().getTags()))
            .build();

        final DBInstanceFactoryFactory factoryFactory = new DBInstanceFactoryFactory(
            proxy, rdsProxyClient, allTags, requestLogger, config, getApiVersionDispatcher()
        );

        final DBInstanceFactory dbInstanceFactory = factoryFactory.createFactory(model);
        return ProgressEvent.progress(model, callbackContext)
            .then(progress -> {
                if (StringUtils.isNullOrEmpty(progress.getResourceModel().getEngine())) {
                    try {
                        model.setEngine(fetchEngine(rdsProxyClient.defaultClient(), progress, proxy));
                    } catch (Exception e) {
                        return Commons.handleException(progress, e, ErrorRuleSets.DB_INSTANCE_FETCH_ENGINE, requestLogger);
                    }
                }
                return progress;
            })
            .then(progress -> Commons.execOnce(
                progress,
                () -> dbInstanceFactory.create(progress),
                CallbackContext::isCreated,
                CallbackContext::setCreated
            ))
            .then(progress -> Commons.execOnce(progress, () -> {
                final Tagging.TagSet extraTags = Tagging.TagSet.builder()
                    .stackTags(allTags.getStackTags())
                    .resourceTags(allTags.getResourceTags())
                    .build();
                return updateTags(proxy, rdsProxyClient.defaultClient(), progress, Tagging.TagSet.emptySet(), extraTags);
            }, CallbackContext::isAddTagsComplete, CallbackContext::setAddTagsComplete))
            .then(progress -> ensureEngineSet(rdsProxyClient.defaultClient(), progress))
            .then(progress -> {
                final DBInstance dbInstance = fetchDBInstance(rdsProxyClient.defaultClient(), model);
                if (ResourceModelHelper.shouldUpdateAfterCreate(progress.getResourceModel(), dbInstance.engine())) {
                    return Commons.execOnce(progress, () -> {
                                progress.getCallbackContext().timestampOnce(RESOURCE_UPDATED_AT, Instant.now());
                                return versioned(proxy, rdsProxyClient, progress, null, ImmutableMap.of(
                                    ApiVersion.V12, (pxy, pcl, prg, tgs) -> updateDbInstanceAfterCreateV12(pxy, request, pcl, prg),
                                    ApiVersion.DEFAULT, (pxy, pcl, prg, tgs) -> updateDbInstanceAfterCreate(pxy, request, pcl, prg)
                                )).then(p ->
                                    Events.checkFailedEvents(
                                        rdsProxyClient.defaultClient(),
                                        p.getResourceModel().getDBInstanceIdentifier(),
                                        SourceType.DB_INSTANCE,
                                        p.getCallbackContext().getTimestamp(RESOURCE_UPDATED_AT),
                                        p,
                                        this::isFailureEvent,
                                        requestLogger
                                    ));
                            },
                            CallbackContext::isUpdated, CallbackContext::setUpdated)
                        .then(p -> Commons.execOnce(p, () -> {
                            if (ResourceModelHelper.shouldReboot(p.getResourceModel())) {
                                return rebootAwait(proxy, rdsProxyClient.defaultClient(), p);
                            }
                            return p;
                        }, CallbackContext::isRebooted, CallbackContext::setRebooted));
                }
                return progress;
            })
            .then(progress -> Commons.execOnce(progress, () ->
                    updateAssociatedRoles(proxy, rdsProxyClient.defaultClient(), progress, Collections.emptyList(), desiredRoles),
                CallbackContext::isUpdatedRoles, CallbackContext::setUpdatedRoles))
            .then(progress -> Commons.execOnce(progress, () -> {
                if (ResourceModelHelper.shouldStartAutomaticBackupReplication(request.getPreviousResourceState(), request.getDesiredResourceState())) {
                    final DBInstance dbInstance = fetchDBInstance(rdsProxyClient.defaultClient(), progress.getResourceModel());
                    if (StringUtils.isNullOrEmpty(callbackContext.getDbInstanceArn())) {
                        callbackContext.setDbInstanceArn(dbInstance.dbInstanceArn());
                    }
                    if (StringUtils.isNullOrEmpty(callbackContext.getKmsKeyId())) {
                        callbackContext.setKmsKeyId(dbInstance.kmsKeyId());
                    }
                }
                return progress;
            }, (m) -> !StringUtils.isNullOrEmpty(callbackContext.getDbInstanceArn()), (v, c) -> {
            }))
            .then(progress -> Commons.execOnce(progress, () -> {
                    if (ResourceModelHelper.shouldStartAutomaticBackupReplication(request.getPreviousResourceState(), request.getDesiredResourceState())) {
                        return startAutomaticBackupReplicationInRegion(
                            callbackContext.getDbInstanceArn(),
                            progress.getResourceModel().getAutomaticBackupReplicationKmsKeyId(),
                            proxy,
                            progress,
                            rdsProxyClient.defaultClient(),
                            ResourceModelHelper.getAutomaticBackupReplicationRegion(request.getDesiredResourceState())
                        );
                    }
                    return progress;
                },
                CallbackContext::isAutomaticBackupReplicationStarted, CallbackContext::setAutomaticBackupReplicationStarted))
            .then(progress -> {
                model.setTags(Translator.translateTagsFromSdk(Tagging.translateTagsToSdk(allTags)));
                return Commons.reportResourceDrift(
                    model,
                    new ReadHandler().handleRequest(proxy, request, progress.getCallbackContext(), rdsProxyClient, ec2ProxyClient, requestLogger),
                    resourceTypeSchema,
                    requestLogger,
                    handlerOperation
                );
            });
    }

    private HandlerMethod<ResourceModel, CallbackContext> safeAddTags(final HandlerMethod<ResourceModel, CallbackContext> handlerMethod) {
        return (proxy, rdsProxyClient, progress, tagSet) -> progress.then(p -> Tagging.createWithTaggingFallback(proxy, rdsProxyClient, handlerMethod, progress, tagSet));
    }

    private String fetchEngine(final ProxyClient<RdsClient> client,
                               final ProgressEvent<ResourceModel, CallbackContext> progress,
                               final AmazonWebServicesClientProxy proxy) {
        final ResourceModel model = progress.getResourceModel();
        final String currentRegion = progress.getCallbackContext().getCurrentRegion();

        if (ResourceModelHelper.isRestoreFromSnapshot(model)) {
            return fetchDBSnapshot(client, model).engine();
        }
        if (ResourceModelHelper.isRestoreFromClusterSnapshot(model)) {
            return fetchDBClusterSnapshot(client, model).engine();
        }

        if (ResourceModelHelper.isDBInstanceReadReplica(model)) {
            final String sourceDBInstanceArn = model.getSourceDBInstanceIdentifier();
            final String sourceDBInstanceIdOrArn = ResourceModelHelper.isValidArn(sourceDBInstanceArn) ?
                ResourceModelHelper.getResourceNameFromArn(sourceDBInstanceArn) : sourceDBInstanceArn;
            if (ResourceModelHelper.isCrossRegionDBInstanceReadReplica(model, currentRegion)) {
                final String sourceRegion = ResourceModelHelper.getRegionFromArn(sourceDBInstanceArn);
                final ProxyClient<RdsClient> sourceRegionClient = new LoggingProxyClient<>(requestLogger,
                    proxy.newProxy(() -> new RdsClientProvider().getClientForRegion(sourceRegion)));
                return fetchDBInstance(sourceRegionClient, sourceDBInstanceIdOrArn).engine();
            } else {
                return fetchDBInstance(client, sourceDBInstanceIdOrArn).engine();
            }
        }
        if (ResourceModelHelper.isDBClusterReadReplica(model)) {
            final String sourceDBClusterArn = model.getSourceDBClusterIdentifier();
            final String sourceDBClusterIdOrArn = ResourceModelHelper.isValidArn(sourceDBClusterArn) ?
                ResourceModelHelper.getResourceNameFromArn(sourceDBClusterArn) : sourceDBClusterArn;
            if (ResourceModelHelper.isCrossRegionDBClusterReadReplica(model, currentRegion)) {
                final String sourceRegion = ResourceModelHelper.getRegionFromArn(sourceDBClusterArn);
                final ProxyClient<RdsClient> sourceRegionClient = proxy.newProxy(() -> new RdsClientProvider().getClientForRegion(sourceRegion));
                return fetchDBCluster(sourceRegionClient, sourceDBClusterIdOrArn).engine();
            } else {
                return fetchDBCluster(client, sourceDBClusterIdOrArn).engine();
            }
        }

        if (ResourceModelHelper.isRestoreToPointInTime(model)) {
            if (StringUtils.hasValue(model.getSourceDBInstanceIdentifier())) {
                return fetchDBInstance(client, model.getSourceDBInstanceIdentifier()).engine();
            }
            if (StringUtils.hasValue(model.getSourceDbiResourceId())) {
                return fetchDBInstanceByResourceId(client, model.getSourceDbiResourceId()).engine();
            }
            if (StringUtils.hasValue(model.getSourceDBInstanceAutomatedBackupsArn())) {
                return fetchAutomaticBackup(client, model.getSourceDBInstanceAutomatedBackupsArn()).engine();
            }
        }

        throw new CfnInvalidRequestException("Cannot fetch the engine based on current template. Please add the Engine parameter to the template and try again.");
    }

    protected ProgressEvent<ResourceModel, CallbackContext> updateDbInstanceAfterCreateV12(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final ProxyClient<RdsClient> rdsProxyClient,
        final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        requestLogger.log("UpdateDbInstanceAfterCreateAPIv12Invoked");
        requestLogger.log("API version 12 modify after create detected",
            "This indicates that the customer is using DBSecurityGroup, which may result in certain features not" +
                " functioning properly. Please refer to the API model for supported parameters");
        return proxy.initiate("rds::modify-db-instance-v12", rdsProxyClient, progress.getResourceModel(), progress.getCallbackContext())
            .translateToServiceRequest(resourceModel -> Translator.modifyDbInstanceAfterCreateRequestV12(request.getDesiredResourceState()))
            .backoffDelay(config.getBackoff())
            .makeServiceCall((modifyRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                modifyRequest,
                proxyInvocation.client()::modifyDBInstance
            ))
            .stabilize((modifyRequest, response, proxyInvocation, model, context) -> isDBInstanceStabilizedAfterMutate(proxyInvocation, model, context))
            .handleError((modifyRequest, exception, client, model, context) -> Commons.handleException(
                ProgressEvent.progress(model, context),
                exception,
                software.amazon.rds.dbinstance.common.ErrorRuleSets.MODIFY_DB_INSTANCE,
                requestLogger
            ))
            .progress();
    }

    protected ProgressEvent<ResourceModel, CallbackContext> updateDbInstanceAfterCreate(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final ProxyClient<RdsClient> rdsProxyClient,
        final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        return proxy.initiate("rds::modify-db-instance", rdsProxyClient, progress.getResourceModel(), progress.getCallbackContext())
            .translateToServiceRequest(resourceModel -> Translator.modifyDbInstanceAfterCreateRequest(request.getDesiredResourceState()))
            .backoffDelay(config.getBackoff())
            .makeServiceCall((modifyRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                modifyRequest,
                proxyInvocation.client()::modifyDBInstance
            ))
            .stabilize((modifyRequest, response, proxyInvocation, model, context) -> isDBInstanceStabilizedAfterMutate(proxyInvocation, model, context))
            .handleError((modifyRequest, exception, client, model, context) -> Commons.handleException(
                ProgressEvent.progress(model, context),
                exception,
                software.amazon.rds.dbinstance.common.ErrorRuleSets.MODIFY_DB_INSTANCE,
                requestLogger
            ))
            .progress();
    }
}
