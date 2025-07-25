package software.amazon.rds.dbcluster;

import java.time.Instant;
import java.util.HashSet;

import org.apache.commons.lang3.BooleanUtils;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.ClusterScalabilityType;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBClusterAutomatedBackup;
import software.amazon.awssdk.services.rds.model.DBClusterSnapshot;
import software.amazon.awssdk.services.rds.model.ModifyDbClusterRequest;
import software.amazon.awssdk.services.rds.model.RestoreDbClusterFromSnapshotRequest;
import software.amazon.awssdk.services.rds.model.RestoreDbClusterToPointInTimeRequest;
import software.amazon.awssdk.services.rds.model.SourceType;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.CallChain;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.Events;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.request.RequestValidationException;
import software.amazon.rds.common.request.ValidatedRequest;
import software.amazon.rds.common.request.Validations;
import software.amazon.rds.common.util.ArnHelper;
import software.amazon.rds.common.util.IdempotencyHelper;
import software.amazon.rds.common.util.IdentifierFactory;
import software.amazon.rds.common.validation.ValidationAccessException;
import software.amazon.rds.common.validation.ValidationUtils;
import software.amazon.rds.dbcluster.util.ResourceModelHelper;
import software.amazon.rds.dbcluster.validators.ClusterScalabilityTypeValidator;

public class CreateHandler extends BaseHandlerStd {

    private static String DB_CLUSTER_STORAGE_ENC_NO_KMS_VALIDATION_MSG = "You can't create an encrypted DB cluster from an unencrypted DB snapshot without an AWS KMS key. " +
        "Specify a valid KMS key ID for encryption, then try again. To use the default key, specify 'alias/aws/rds'.";
    private static String DB_CLUSTER_RESTORE_ENC_NO_STORAGE_ENC_PROPERTY_VALIDATION_MSG = "Encryption must be enabled when restoring a DB cluster snapshot. " +
        "Either enable encryption or remove the encryption parameter from the template.";
    private static String DB_CLUSTER_KMS_KEY_NO_STORAGE_ENC_VALIDATION_MSG = "If you specify an AWS KMS key when restoring a DB snapshot, encryption must enabled. " +
        "Either enable encryption or remove the encryption parameter from the template.";
    private static String DB_CLUSTER_SOURCE_IDENTIFIERS_VALIDATION_MSG = "You must enter either the SourceDbClusterIdentifier or SourceDbClusterResourceId.";
    private static String LIMITLESS_DB_CLUSTER_AUTOMATED_BACKUP_RESTORE_VALIDATION_MSG = "Automated backup retention isn't supported with Aurora Limitless Database clusters. " +
        "If you set this property to limitless, you cannot set DeleteAutomatedBackups to false. To create a backup, use manual snapshots instead.";
    public final static String DB_CLUSTER_VALIDATION_MISSING_PERMISSIONS_METRIC = "DBClusterValidationMissingPermissions";
    public final static String LIMITLESS_ENGINE_VERSION_SUFFIX = "limitless";
    public final static String ENGINE_VERSION_SEPERATOR = "-";

    private static final IdentifierFactory dbClusterIdentifierFactory = new IdentifierFactory(
            STACK_NAME,
            RESOURCE_IDENTIFIER,
            RESOURCE_ID_MAX_LENGTH
    );

    public CreateHandler() {
        this(DB_CLUSTER_HANDLER_CONFIG_36H);
    }

    final String handlerOperation = "CREATE";

    public CreateHandler(final HandlerConfig config) {
        super(config);
    }

    @Override
    protected void validateRequest(final ResourceHandlerRequest<ResourceModel> request) throws RequestValidationException {
        super.validateRequest(request);
        Validations.validateTimestamp(request.getDesiredResourceState().getRestoreToTime());
        ClusterScalabilityTypeValidator.validateRequest(request.getDesiredResourceState());
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ValidatedRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProxyClient<Ec2Client> ec2ProxyClient
    ) {
        final ResourceModel model = ModelAdapter.setDefaults(request.getDesiredResourceState());

        if (StringUtils.isNullOrEmpty(model.getDBClusterIdentifier())) {
            model.setDBClusterIdentifier(dbClusterIdentifierFactory.newIdentifier()
                    .withStackId(request.getStackId())
                    .withResourceId(request.getLogicalResourceIdentifier())
                    .withRequestToken(request.getClientRequestToken())
                    .toString());
        }

        final Tagging.TagSet allTags = Tagging.TagSet.builder()
                .systemTags(Tagging.translateTagsToSdk(request.getSystemTags()))
                .stackTags(Tagging.translateTagsToSdk(request.getDesiredResourceTags()))
                .resourceTags(new HashSet<>(Translator.translateTagsToSdk(request.getDesiredResourceState().getTags())))
                .build();

        if(ResourceModelHelper.isRestoreFromSnapshot(model)) {
            ClusterScalabilityType clusterScalabilityType = getClusterScalabilityTypeFromSnapshot(rdsProxyClient, model);
            callbackContext.setClusterScalabilityType(clusterScalabilityType);
        }
        if(ResourceModelHelper.isRestoreToPointInTime(model)) {
            if (StringUtils.hasValue(model.getSourceDBClusterIdentifier()) && StringUtils.hasValue(model.getSourceDbClusterResourceId())) {
                throw new CfnInvalidRequestException(DB_CLUSTER_SOURCE_IDENTIFIERS_VALIDATION_MSG);
            }
            ClusterScalabilityType clusterScalabilityType = getClusterScalabilityTypeFromSourceDBCluster(extractAwsAccountId(request), rdsProxyClient, model);
            callbackContext.setClusterScalabilityType(clusterScalabilityType);
        }

        return ProgressEvent.progress(model, callbackContext)
                .then(progress ->
                    IdempotencyHelper.safeCreate(
                        m -> fetchDBCluster(rdsProxyClient, m),
                        p -> {
                            if (ResourceModelHelper.isRestoreToPointInTime(model)) {
                                validateRestoreDBClusterToPointInTime(extractAwsAccountId(request), rdsProxyClient, model);
                                return Tagging.createWithTaggingFallback(proxy, rdsProxyClient, this::restoreDbClusterToPointInTime, p, allTags);
                            } else if (ResourceModelHelper.isRestoreFromSnapshot(model)) {
                                return Tagging.createWithTaggingFallback(proxy, rdsProxyClient, this::restoreDbClusterFromSnapshot, p, allTags);
                            }
                            return Tagging.createWithTaggingFallback(proxy, rdsProxyClient, this::createDbCluster, p, allTags);
                        }, ResourceModel.TYPE_NAME, model.getDBClusterIdentifier(), progress, requestLogger))
                .then(progress -> Commons.execOnce(progress, () -> {
                    final Tagging.TagSet extraTags = Tagging.TagSet.builder()
                            .stackTags(allTags.getStackTags())
                            .resourceTags(allTags.getResourceTags())
                            .build();
                    return updateTags(proxy, rdsProxyClient, progress, Tagging.TagSet.emptySet(), extraTags);
                }, CallbackContext::isAddTagsComplete, CallbackContext::setAddTagsComplete))
                .then(progress -> {
                    if (ResourceModelHelper.shouldUpdateAfterCreate(progress.getResourceModel())) {
                        return Commons.execOnce(
                                progress,
                                () -> {
                                    progress.getCallbackContext().timestampOnce(RESOURCE_UPDATED_AT, Instant.now());
                                    return modifyDBCluster(proxy, rdsProxyClient, progress)
                                            .then(p -> {
                                                if (ResourceModelHelper.shouldEnableHttpEndpointV2AfterCreate(progress.getResourceModel())) {
                                                    return enableHttpEndpointV2(proxy, rdsProxyClient, progress);
                                                }
                                                return p;
                                            })
                                            .then(p -> Events.checkFailedEvents(
                                                    rdsProxyClient,
                                                    p.getResourceModel().getDBClusterIdentifier(),
                                                    SourceType.DB_CLUSTER,
                                                    p.getCallbackContext().getTimestamp(RESOURCE_UPDATED_AT),
                                                    p,
                                                    this::isFailureEvent,
                                                    requestLogger
                                            ));
                                },
                                CallbackContext::isModified,
                                CallbackContext::setModified
                        );
                    }
                    return progress;
                })
                .then(progress -> addAssociatedRoles(proxy, rdsProxyClient, progress, progress.getResourceModel().getAssociatedRoles(), false))
                .then(progress -> {
                    model.setTags(Translator.translateTagsFromSdk(Tagging.translateTagsToSdk(allTags)));
                    return Commons.reportResourceDrift(
                            model,
                            new ReadHandler().handleRequest(proxy, request, progress.getCallbackContext(), rdsProxyClient, ec2ProxyClient),
                            resourceTypeSchema,
                            requestLogger,
                            handlerOperation
                    );
                });
    }

    private ProgressEvent<ResourceModel, CallbackContext> createDbCluster(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet tagSet
    ) {
        return proxy.initiate("rds::create-dbcluster", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(model -> Translator.createDbClusterRequest(model, tagSet))
                .backoffDelay(config.getBackoff())
                .makeServiceCall((dbClusterRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        dbClusterRequest,
                        proxyInvocation.client()::createDBCluster
                ))
                .stabilize((modifyRequest, modifyResponse, proxyInvocation, model, context) -> {
                    return isDBClusterStabilized(proxyInvocation, model);
                })
                .handleError((request, exception, client, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        DEFAULT_DB_CLUSTER_ERROR_RULE_SET,
                        requestLogger
                ))
                .progress();
    }

    private ProgressEvent<ResourceModel, CallbackContext> restoreDbClusterToPointInTime(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet tagSet
    ) {
        CallChain.RequestMaker<RdsClient, ResourceModel, CallbackContext> requestMaker = proxy.initiate("rds::restore-dbcluster-to-point-in-time", proxyClient, progress.getResourceModel(), progress.getCallbackContext());
        CallChain.Caller<RestoreDbClusterToPointInTimeRequest, RdsClient, ResourceModel, CallbackContext> caller = null;
        if(progress.getCallbackContext().getClusterScalabilityType().equals(ClusterScalabilityType.LIMITLESS)) {
            caller = requestMaker.translateToServiceRequest(model -> Translator.restoreLimitlessDbClusterToPointInTimeRequest(model, tagSet));
        } else {
            caller = requestMaker.translateToServiceRequest(model -> Translator.restoreDbClusterToPointInTimeRequest(model, tagSet));
        }
        return caller.backoffDelay(config.getBackoff())
                .makeServiceCall((dbClusterRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        dbClusterRequest,
                        proxyInvocation.client()::restoreDBClusterToPointInTime
                ))
                .stabilize((modifyRequest, modifyResponse, proxyInvocation, model, context) -> {
                    return isDBClusterStabilized(proxyInvocation, model);
                })
                .handleError((request, exception, client, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        DEFAULT_DB_CLUSTER_ERROR_RULE_SET,
                        requestLogger
                ))
                .progress();
    }

    private ProgressEvent<ResourceModel, CallbackContext> restoreDbClusterFromSnapshot(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final Tagging.TagSet tagSet
    ) {
        CallChain.RequestMaker<RdsClient, ResourceModel ,CallbackContext> requestMaker = proxy.initiate("rds::restore-dbcluster-from-snapshot", proxyClient, progress.getResourceModel(), progress.getCallbackContext());
        CallChain.Caller<RestoreDbClusterFromSnapshotRequest, RdsClient, ResourceModel, CallbackContext> caller = null;
        if(progress.getCallbackContext().getClusterScalabilityType().equals(ClusterScalabilityType.LIMITLESS)) {
            caller = requestMaker.translateToServiceRequest(model -> Translator.restoreLimitlessDbClusterFromSnapshotRequest(model, tagSet));
        } else {
            caller = requestMaker.translateToServiceRequest(model -> Translator.restoreDbClusterFromSnapshotRequest(model, tagSet));
        }

        return caller.backoffDelay(config.getBackoff())
                .makeServiceCall((dbClusterRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        dbClusterRequest,
                        proxyInvocation.client()::restoreDBClusterFromSnapshot
                ))
                .stabilize((modifyRequest, modifyResponse, proxyInvocation, model, context) -> {
                    return isDBClusterStabilized(proxyInvocation, model);
                })
                .handleError((request, exception, client, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        DEFAULT_DB_CLUSTER_ERROR_RULE_SET,
                        requestLogger
                ))
                .progress();
    }

    protected ProgressEvent<ResourceModel, CallbackContext> modifyDBCluster(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        ClusterScalabilityType clusterScalabilityType = progress.getCallbackContext().getClusterScalabilityType();
        CallChain.RequestMaker<RdsClient, ResourceModel, CallbackContext> callContext = proxy.initiate("rds::modify-dbcluster", proxyClient, progress.getResourceModel(), progress.getCallbackContext());
        CallChain.Caller<ModifyDbClusterRequest, RdsClient, ResourceModel, CallbackContext> caller = null;

        if (clusterScalabilityType.equals(ClusterScalabilityType.LIMITLESS)) {
            caller = callContext.translateToServiceRequest(Translator::modifyLimitlessDbClusterAfterCreateRequest);
        }
        else {
            caller = callContext.translateToServiceRequest(Translator::modifyDbClusterAfterCreateRequest);
        }

        return caller.backoffDelay(config.getBackoff())
                .makeServiceCall((dbClusterModifyRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        dbClusterModifyRequest,
                        proxyInvocation.client()::modifyDBCluster
                ))
                .stabilize((modifyRequest, modifyResponse, proxyInvocation, model, context) -> {
                    return isDBClusterStabilized(proxyInvocation, model);
                })
                .handleError((createRequest, exception, client, resourceModel, callbackCtxt) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, callbackCtxt),
                        exception,
                        DEFAULT_DB_CLUSTER_ERROR_RULE_SET,
                        requestLogger
                ))
                .progress();
    }

    private String extractAwsAccountId(ValidatedRequest<ResourceModel> request) {
        if (request != null && StringUtils.hasValue(request.getAwsAccountId())) {
            return request.getAwsAccountId();
        }
        return null;
    }

    private void validateRestoreDBClusterToPointInTime(final String awsAccountId,
                                                       final ProxyClient<RdsClient> rdsProxyClient,
                                                       final ResourceModel desiredModel
    ) {
        if (!StringUtils.hasValue(desiredModel.getSourceDBClusterIdentifier()) && !StringUtils.hasValue(desiredModel.getSourceDbClusterResourceId())) {
            throw new CfnInvalidRequestException(DB_CLUSTER_SOURCE_IDENTIFIERS_VALIDATION_MSG);
        }
        try {
            Boolean physicalStorageEncrypted = null;
            if (StringUtils.hasValue(desiredModel.getSourceDBClusterIdentifier())) {
                DBCluster dbCluster = ValidationUtils.fetchResourceForValidation(
                        () -> fetchSourceDBCluster(awsAccountId, rdsProxyClient, desiredModel),
                        "DescribeDBClusters");
                physicalStorageEncrypted = dbCluster.storageEncrypted();
            } else if (StringUtils.hasValue(desiredModel.getSourceDbClusterResourceId())) {
                DBClusterAutomatedBackup dbClusterAutomatedBackup = ValidationUtils.fetchResourceForValidation(
                        () -> fetchSourceDBClusterAutomatedBackup(rdsProxyClient, desiredModel),
                        "DescribeDBClusterAutomatedBackups");
                physicalStorageEncrypted = dbClusterAutomatedBackup.storageEncrypted();
            }
            final Boolean desiredStorageEncrypted = desiredModel.getStorageEncrypted();
            final String desiredKMSKey = desiredModel.getKmsKeyId();
            validateStorageEncryption(physicalStorageEncrypted, desiredStorageEncrypted, desiredKMSKey);
        } catch (ValidationAccessException ex) {
            ValidationUtils.emitMetric(requestLogger, DB_CLUSTER_VALIDATION_MISSING_PERMISSIONS_METRIC, ex);
        }
    }

    protected ClusterScalabilityType getClusterScalabilityTypeFromSnapshot(final ProxyClient<RdsClient> rdsProxyClient, final ResourceModel resourceModel) {
        // Source SnapshotIdentifier might belong to either DBClusterSnapshot or DBSnapshot.
        // Instance snapshot must use ARN format. If the format is not an ARN, treat this as a cluster snapshot
        try {
            if (ArnHelper.isValidArn(resourceModel.getSnapshotIdentifier())
                && (ArnHelper.getResourceType(resourceModel.getSnapshotIdentifier()) == ArnHelper.ResourceType.DB_INSTANCE_SNAPSHOT)) {
                return ClusterScalabilityType.STANDARD;
            }
            else {
                final DBClusterSnapshot dbClusterSnapshot = ValidationUtils.fetchResourceForValidation(() ->
                    fetchDBClusterSnapshot(rdsProxyClient, resourceModel), "DescribeDBClusterSnapshots");
                return getClusterScalabilityTypeFromEngineVersion(dbClusterSnapshot.engineVersion());
            }
        }
        catch (ValidationAccessException e) {
            ValidationUtils.emitMetric(requestLogger, DB_CLUSTER_VALIDATION_MISSING_PERMISSIONS_METRIC, e);
            return ClusterScalabilityType.STANDARD;
        }
    }

    protected ClusterScalabilityType getClusterScalabilityTypeFromEngineVersion(final String snapshotEngineVersion) {
        if (StringUtils.isNullOrEmpty(snapshotEngineVersion)) {
            return ClusterScalabilityType.STANDARD;
        }
        // we are using the engine version suffix until clusterScalabilityType is returned as part of describe snapshot API
        String[] snapshotEngineVersionParts = snapshotEngineVersion.split(ENGINE_VERSION_SEPERATOR);
        if(snapshotEngineVersionParts.length > 1 && snapshotEngineVersionParts[1].equals(LIMITLESS_ENGINE_VERSION_SUFFIX)) {
            return ClusterScalabilityType.LIMITLESS;
        }

        return ClusterScalabilityType.STANDARD;
    }

    private static void validateStorageEncryption(final Boolean physicalStorageEncrypted,
                                                  final Boolean desiredStorageEncrypted,
                                                  final String desiredKMSKey) {
        if (isOptingIntoEncryption(physicalStorageEncrypted, desiredStorageEncrypted)) {
            if (StringUtils.isNullOrEmpty(desiredKMSKey)) {
                throw new CfnInvalidRequestException(DB_CLUSTER_STORAGE_ENC_NO_KMS_VALIDATION_MSG);
            }
        }

        if (isOptingOutOfEncryption(physicalStorageEncrypted, desiredStorageEncrypted)) {
            throw new CfnInvalidRequestException(DB_CLUSTER_RESTORE_ENC_NO_STORAGE_ENC_PROPERTY_VALIDATION_MSG);
        }

        //Added validation as RestoreDbClusterToPointInTime/RestoreFromDBClusterSnapshot are supporting KMS Key
        //encryption but not supporting StorageEncrypted flag.
        if (BooleanUtils.isFalse(desiredStorageEncrypted) && StringUtils.hasValue(desiredKMSKey)) {
            throw new CfnInvalidRequestException(DB_CLUSTER_KMS_KEY_NO_STORAGE_ENC_VALIDATION_MSG);
        }
    }

    private static boolean isOptingIntoEncryption(final Boolean physicalStorageEncrypted,
                                                  final Boolean desiredStorageEncrypted) {
        return BooleanUtils.isFalse(physicalStorageEncrypted) && BooleanUtils.isTrue(desiredStorageEncrypted);
    }

    private static boolean isOptingOutOfEncryption(final Boolean physicalStorageEncrypted,
                                                   final Boolean desiredStorageEncrypted) {
        return BooleanUtils.isTrue(physicalStorageEncrypted) &&
                BooleanUtils.isFalse(desiredStorageEncrypted);
    }

    protected ClusterScalabilityType getClusterScalabilityTypeFromSourceDBCluster(final String AwsAccountId, final ProxyClient<RdsClient> rdsProxyClient, final ResourceModel resourceModel) {
        if (StringUtils.hasValue(resourceModel.getSourceDBClusterIdentifier())) {
            DBCluster cluster = fetchSourceDBCluster(AwsAccountId, rdsProxyClient, resourceModel);
            return cluster.clusterScalabilityType() != null ? cluster.clusterScalabilityType() : ClusterScalabilityType.STANDARD;
        } else if (StringUtils.hasValue(resourceModel.getSourceDbClusterResourceId())) {
            DBClusterAutomatedBackup backup = fetchSourceDBClusterAutomatedBackup(rdsProxyClient, resourceModel);
            ClusterScalabilityType scalabilityType = getClusterScalabilityTypeFromEngineVersion(backup.engineVersion());
            if ("retained".equals(backup.status()) && scalabilityType.equals(ClusterScalabilityType.LIMITLESS)) {
                throw new CfnInvalidRequestException(LIMITLESS_DB_CLUSTER_AUTOMATED_BACKUP_RESTORE_VALIDATION_MSG);
            }
            return scalabilityType;
        }
        throw new CfnInvalidRequestException(DB_CLUSTER_SOURCE_IDENTIFIERS_VALIDATION_MSG);
    }
}
