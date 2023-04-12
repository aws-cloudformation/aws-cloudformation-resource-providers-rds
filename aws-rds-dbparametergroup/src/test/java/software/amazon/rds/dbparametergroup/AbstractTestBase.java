package software.amazon.rds.dbparametergroup;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.json.JSONObject;
import org.mockito.internal.util.collections.Sets;
import org.mockito.stubbing.OngoingStubbing;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBParameterGroup;
import software.amazon.awssdk.services.rds.model.DescribeDbParametersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbParametersResponse;
import software.amazon.awssdk.services.rds.model.DescribeEngineDefaultParametersRequest;
import software.amazon.awssdk.services.rds.model.DescribeEngineDefaultParametersResponse;
import software.amazon.awssdk.services.rds.model.EngineDefaults;
import software.amazon.awssdk.services.rds.model.Parameter;
import software.amazon.cloudformation.proxy.Credentials;
import software.amazon.cloudformation.proxy.LoggerProxy;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.test.common.core.HandlerName;
import software.amazon.rds.test.common.core.TestUtils;
import software.amazon.rds.test.common.verification.AccessPermissionAlias;
import software.amazon.rds.test.common.verification.AccessPermissionVerificationMode;

public abstract class AbstractTestBase {
    protected static final Credentials MOCK_CREDENTIALS;
    protected static final LoggerProxy logger;

    protected static final ResourceModel RESOURCE_MODEL;
    protected static final ResourceModel RESET_RESOURCE_MODEL;
    protected static final ResourceModel RESOURCE_MODEL_WITH_TAGS;
    protected static final DBParameterGroup DB_PARAMETER_GROUP_ACTIVE;
    protected static final Set<Tag> TAG_SET;
    protected static final String LOGICAL_RESOURCE_IDENTIFIER;
    protected static final Map<String, Object> PARAMS;
    protected static final Map<String, Object> RESET_PARAMS;
    protected static final RequestLogger EMPTY_REQUEST_LOGGER;

    static {
        MOCK_CREDENTIALS = new Credentials("accessKey", "secretKey", "token");
        logger = new LoggerProxy();
        EMPTY_REQUEST_LOGGER = new RequestLogger(logger, ResourceHandlerRequest.builder().build(), null);
        LOGICAL_RESOURCE_IDENTIFIER = "db-parameter-group";

        PARAMS = new HashMap<>();
        PARAMS.put("param1", "value");
        PARAMS.put("param2", "value");

        RESET_PARAMS = new HashMap<>(PARAMS);
        RESET_PARAMS.remove("param1");

        RESOURCE_MODEL = ResourceModel.builder()
                .dBParameterGroupName("testDBParameterGroup")
                .description("test DB Parameter group description")
                .family("testFamily")
                .tags(Collections.emptyList())
                .parameters(PARAMS)
                .build();

        RESET_RESOURCE_MODEL = ResourceModel.builder()
                .dBParameterGroupName("testDBParameterGroup")
                .description("test DB Parameter group description")
                .family("testFamily")
                .tags(Collections.emptyList())
                .parameters(RESET_PARAMS)
                .build();

        RESOURCE_MODEL_WITH_TAGS = ResourceModel.builder()
                .dBParameterGroupName("testDBParameterGroup2")
                .description("test DB Parameter group description")
                .family("testFamily2")
                .tags(Collections.singletonList(Tag.builder().key("Key").value("Value").build()))
                .parameters(PARAMS)
                .build();

        DB_PARAMETER_GROUP_ACTIVE = DBParameterGroup.builder()
                .dbParameterGroupArn("arn")
                .dbParameterGroupName("testDBParameterGroup")
                .description("test DB Parameter group description")
                .dbParameterGroupFamily("testFamily")
                .build();

        TAG_SET = Sets.newSet(Tag.builder().key("key").value("value").build());
    }

    public abstract HandlerName getHandlerName();

    private static final JSONObject resourceSchema = new Configuration().resourceSchemaJsonObject();

    public void verifyAccessPermissions(final Object mock, final AccessPermissionAlias... aliases) {
        new AccessPermissionVerificationMode()
                .withDefaultPermissions()
                .withSchemaPermissions(resourceSchema, getHandlerName())
                .withAliases(aliases)
                .verify(TestUtils.getVerificationData(mock));
    }

    static Map<String, String> translateTagsToMap(final Set<Tag> tags) {
        return tags.stream()
                .collect(Collectors.toMap(Tag::getKey, Tag::getValue));
    }

    static String getClientRequestToken() {
        return UUID.randomUUID().toString();
    }

    protected void expectEmptyDescribeParametersResponse(final ProxyClient<RdsClient> proxyClient) {
        when(proxyClient.client().describeDBParameters(any(DescribeDbParametersRequest.class)))
                .thenReturn(DescribeDbParametersResponse.builder().build());
        when(proxyClient.client().describeEngineDefaultParameters(any(DescribeEngineDefaultParametersRequest.class)))
                .thenReturn(DescribeEngineDefaultParametersResponse.builder().build());
    }

    void mockDescribeDbParametersResponse(
            final ProxyClient<RdsClient> proxyClient,
            final String firstParamApplyType,
            final String secondParamApplyType,
            final boolean isModifiable,
            final boolean mockDescribeParameters,
            final boolean isPaginated
    ) {
        mockDescribeDbParametersResponse(proxyClient, firstParamApplyType, secondParamApplyType, isModifiable, mockDescribeParameters, isPaginated, 1);
    }

    void mockDescribeDbParametersResponse(
            final ProxyClient<RdsClient> proxyClient,
            final String firstParamApplyType,
            final String secondParamApplyType,
            final boolean isModifiable,
            final boolean mockDescribeParameters,
            final boolean isPaginated,
            final int nTimes
    ) {
        Parameter param1 = Parameter.builder()
                .parameterName("param1")
                .parameterValue("system_value")
                .isModifiable(isModifiable)
                .applyType(firstParamApplyType)
                .applyMethod("pending-reboot")
                .build();
        Parameter defaultParam1 = param1.toBuilder()
                .parameterValue("default_value")
                .applyMethod("")
                .build();
        Parameter param2 = Parameter.builder()
                .parameterName("param2")
                .parameterValue("system_value")
                .isModifiable(isModifiable)
                .applyType(secondParamApplyType)
                .build();
        //Adding parameter to current parameters and not adding it to default. Expected behaviour is to ignore it
        Parameter param3 = Parameter.builder()
                .parameterName("param3")
                .parameterValue("system_value")
                .isModifiable(isModifiable)
                .applyType(secondParamApplyType)
                .build();
        //Adding parameter to default parameters and not adding it to current. Expected behaviour is to ignore it
        Parameter param4 = Parameter.builder()
                .parameterName("param4")
                .parameterValue("system_value")
                .isModifiable(isModifiable)
                .applyType(secondParamApplyType)
                .build();

        final DescribeEngineDefaultParametersResponse describeEngineDefaultParametersResponse = DescribeEngineDefaultParametersResponse.builder()
                .engineDefaults(EngineDefaults.builder()
                        .parameters(defaultParam1, param2, param4)
                        .build()
                ).build();

        if (!isPaginated) {
            repeatedly(
                    when(proxyClient.client().describeEngineDefaultParameters(any(DescribeEngineDefaultParametersRequest.class))),
                    (st) -> st.thenReturn(describeEngineDefaultParametersResponse),
                    nTimes
            );
        } else {
            final DescribeEngineDefaultParametersResponse firstPage = DescribeEngineDefaultParametersResponse.builder()
                    .engineDefaults(EngineDefaults.builder()
                            .parameters(defaultParam1, param2, param4)
                            .marker("marker")
                            .build()
                    ).build();
            repeatedly(
                    when(proxyClient.client().describeEngineDefaultParameters(any(DescribeEngineDefaultParametersRequest.class))),
                    (st) -> st.thenReturn(firstPage).thenReturn(describeEngineDefaultParametersResponse),
                    nTimes
            );
        }
        if (!mockDescribeParameters) {
            return;
        }

        final DescribeDbParametersResponse describeDbParametersResponse = DescribeDbParametersResponse.builder()
                .marker(null)
                .parameters(param1, param2, param3)
                .build();

        if (!isPaginated) {
            repeatedly(
                    when(proxyClient.client().describeDBParameters(any(DescribeDbParametersRequest.class))),
                    (st) -> st.thenReturn(describeDbParametersResponse),
                    nTimes
            );
        } else {
            final DescribeDbParametersResponse firstPage = DescribeDbParametersResponse.builder()
                    .marker("marker")
                    .parameters(param1, param2, param3)
                    .build();
            repeatedly(
                    when(proxyClient.client().describeDBParameters(any(DescribeDbParametersRequest.class))),
                    (st) -> st.thenReturn(firstPage).thenReturn(describeDbParametersResponse),
                    nTimes
            );
        }
    }

    private <T> OngoingStubbing<T> repeatedly(
            final OngoingStubbing<T> base,
            final Function<OngoingStubbing<T>, OngoingStubbing<T>> then,
            final int nTimes
    ) {
        OngoingStubbing<T> cur = base;
        for (int it = 0; it < nTimes; it++) {
            cur = then.apply(cur);
        }
        return cur;
    }

    protected final <T> Iterator<T> responseIterator(final T response) {
        return Collections.singletonList(response).iterator();
    }
}
