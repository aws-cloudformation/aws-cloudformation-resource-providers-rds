package software.amazon.rds.integration;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.json.JSONObject;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeIntegrationsRequest;
import software.amazon.awssdk.services.rds.model.DescribeIntegrationsResponse;
import software.amazon.awssdk.services.rds.model.Integration;
import software.amazon.awssdk.services.rds.model.IntegrationStatus;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Credentials;
import software.amazon.cloudformation.proxy.LoggerProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.test.common.core.AbstractTestBase;
import software.amazon.rds.test.common.core.HandlerName;
import software.amazon.rds.test.common.core.TestUtils;
import software.amazon.rds.test.common.verification.AccessPermissionVerificationMode;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public abstract class AbstractHandlerTest extends AbstractTestBase<Integration, ResourceModel, CallbackContext> {

    protected static final String LOGICAL_RESOURCE_IDENTIFIER = "integrationresource";

    protected static final Credentials MOCK_CREDENTIALS;
    protected static final org.slf4j.Logger delegate;
    protected static final LoggerProxy logger;

    static final Set<Tag> TAG_LIST;
    static final Set<Tag> TAG_LIST_EMPTY;
    static final Set<Tag> TAG_LIST_ALTER;
    static final Tagging.TagSet TAG_SET;

    // use an accelerated backoff for faster unit testing
    protected final HandlerConfig TEST_HANDLER_CONFIG = HandlerConfig.builder()
            .probingEnabled(false)
                        .backoff(Constant.of().delay(Duration.ofMillis(1))
            .timeout(Duration.ofSeconds(120))
            .build())
            .build();

    static {
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "HH:mm:ss:SSS Z");
        MOCK_CREDENTIALS = new Credentials("accessKey", "secretKey", "token");

        delegate = LoggerFactory.getLogger("testing");
        logger = new LoggerProxy();


        TAG_LIST_EMPTY = ImmutableSet.of();

        TAG_LIST = ImmutableSet.of(
                Tag.builder().key("k1").value("kv1").build()
        );

        TAG_LIST_ALTER = ImmutableSet.of(
                Tag.builder().key("k1").value("kv2").build(),
                Tag.builder().key("k2").value("kv2").build(),
                Tag.builder().key("k3").value("kv3").build()
        );

        TAG_SET = Tagging.TagSet.builder()
                .systemTags(ImmutableSet.of(
                        software.amazon.awssdk.services.rds.model.Tag.builder().key("system-tag-1").value("system-tag-value1").build(),
                        software.amazon.awssdk.services.rds.model.Tag.builder().key("system-tag-2").value("system-tag-value2").build(),
                        software.amazon.awssdk.services.rds.model.Tag.builder().key("system-tag-3").value("system-tag-value3").build()
                )).stackTags(ImmutableSet.of(
                        software.amazon.awssdk.services.rds.model.Tag.builder().key("stack-tag-1").value("stack-tag-value1").build(),
                        software.amazon.awssdk.services.rds.model.Tag.builder().key("stack-tag-2").value("stack-tag-value2").build(),
                        software.amazon.awssdk.services.rds.model.Tag.builder().key("stack-tag-3").value("stack-tag-value3").build()
                )).resourceTags(ImmutableSet.of(
                        software.amazon.awssdk.services.rds.model.Tag.builder().key("resource-tag-1").value("resource-tag-value1").build(),
                        software.amazon.awssdk.services.rds.model.Tag.builder().key("resource-tag-2").value("resource-tag-value2").build(),
                        software.amazon.awssdk.services.rds.model.Tag.builder().key("resource-tag-3").value("resource-tag-value3").build()
                )).build();
    }

    protected static final String INTEGRATION_NAME = "integration-identifier-1";
    protected static final String INTEGRATION_NAME_ALTER = "integration-identifier-2";
    protected static final String INTEGRATION_ARN = "arn:aws:rds:us-east-1:123456789012:integration:de4b78a2-0bff-4e93-814a-bacd3f81b383";
    protected static final String SOURCE_ARN = "arn:aws:rds:us-east-1:123456789012:cluster:cfn-integ-test-prov-5-rdsdbcluster-ozajchztpipc";
    protected static final String TARGET_ARN = "arn:aws:redshift:us-east-1:123456789012:namespace:ad99c581-dbac-4a1b-9602-d5c5e7f77b24";
    protected static final String KMS_KEY_ID = "arn:aws:kms:us-east-1:123456789012:key/9d67ba2d-daca-4e3c-ac23-16342062ede3";
    protected static final Map<String, String> ADDITIONAL_ENCRYPTION_CONTEXT = ImmutableMap.of("eck1", "ecv1", "eck2", "ecv2");
    protected static final String DESCRIPTION = "integration description";
    protected static final String DATA_FILTER = "include: *.*";
    protected static final String DESCRIPTION_ALTER = "integration description 2";
    protected static final String DATA_FILTER_ALTER = "include: ";

    static final Integration INTEGRATION_ACTIVE = Integration.builder()
            .integrationArn(INTEGRATION_ARN)
            .integrationName(INTEGRATION_NAME)
            .sourceArn(SOURCE_ARN)
            .targetArn(TARGET_ARN)
            .kmsKeyId(KMS_KEY_ID)
            .status(IntegrationStatus.ACTIVE)
            .createTime(Instant.ofEpochMilli(1699489854712L))
            .additionalEncryptionContext(ADDITIONAL_ENCRYPTION_CONTEXT)
            .tags(toAPITags(TAG_LIST))
            .dataFilter(DATA_FILTER)
            .description(DESCRIPTION)
            .build();


    protected static final Integration INTEGRATION_CREATING = INTEGRATION_ACTIVE.toBuilder()
            .status(IntegrationStatus.CREATING)
            .build();

    protected static final Integration INTEGRATION_FAILED = INTEGRATION_ACTIVE.toBuilder()
            .status(IntegrationStatus.FAILED)
            .build();

    protected static final Integration INTEGRATION_DELETING = INTEGRATION_ACTIVE.toBuilder()
            .status(IntegrationStatus.DELETING)
            .build();

    protected static final ResourceModel INTEGRATION_ACTIVE_MODEL = ResourceModel.builder()
            .integrationArn(INTEGRATION_ARN)
            .integrationName(INTEGRATION_NAME)
            .sourceArn(SOURCE_ARN)
            .targetArn(TARGET_ARN)
            .kMSKeyId(KMS_KEY_ID)
            .createTime("2023-11-09T00:30:54.712000+00:00")
            .additionalEncryptionContext(ADDITIONAL_ENCRYPTION_CONTEXT)
            .tags(TAG_LIST)
            .dataFilter(DATA_FILTER)
            .description(DESCRIPTION)
            .build();

    protected static final ResourceModel INTEGRATION_MODEL_WITH_NO_NAME = INTEGRATION_ACTIVE_MODEL.toBuilder()
            .integrationName(null)
            .build();


    static <ClientT> ProxyClient<ClientT> MOCK_PROXY(final AmazonWebServicesClientProxy proxy, final ClientT client) {
        return new BaseProxyClient<>(proxy, client);
    }

    protected abstract BaseHandlerStd getHandler();

    protected abstract AmazonWebServicesClientProxy getProxy();

    protected abstract ProxyClient<RdsClient> getRdsProxy();

    public abstract HandlerName getHandlerName();

    private static final JSONObject resourceSchema = new Configuration().resourceSchemaJsonObject();

    public void verifyAccessPermissions(final Object mock) {
        new AccessPermissionVerificationMode()
                .withDefaultPermissions()
                .withSchemaPermissions(resourceSchema, getHandlerName())
                .verify(TestUtils.getVerificationData(mock));
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> invokeHandleRequest(
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext context
    ) {
        return getHandler().handleRequest(getProxy(), getRdsProxy(), request, context);
    }

    @Override
    protected String getLogicalResourceIdentifier() {
        return LOGICAL_RESOURCE_IDENTIFIER;
    }

    @Override
    protected void expectResourceSupply(Supplier<Integration> supplier) {
        when(getRdsProxy().client().describeIntegrations(any(DescribeIntegrationsRequest.class)))
                .then((req) ->
                        DescribeIntegrationsResponse.builder()
                                .integrations(supplier.get())
                                .build());
    }

    protected static Collection<software.amazon.awssdk.services.rds.model.Tag> toAPITags(Collection<Tag> tags) {
        return tags.stream().map(t -> software.amazon.awssdk.services.rds.model.Tag.builder()
                .key(t.getKey())
                .value(t.getValue())
                .build())
                .collect(Collectors.toSet());
    }

    public static AwsServiceException makeAwsServiceException(ErrorCode errorCode) {
        return AwsServiceException.builder()
                .awsErrorDetails(
                        AwsErrorDetails.builder()
                                .errorCode(errorCode.toString())
                                .build())
                .build();
    }
}
