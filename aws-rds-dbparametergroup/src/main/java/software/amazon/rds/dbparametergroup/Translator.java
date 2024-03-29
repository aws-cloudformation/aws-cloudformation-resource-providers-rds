package software.amazon.rds.dbparametergroup;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.amazonaws.arn.Arn;
import com.amazonaws.util.CollectionUtils;
import lombok.NonNull;
import software.amazon.awssdk.services.rds.model.ApplyMethod;
import software.amazon.awssdk.services.rds.model.CreateDbParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.DBParameterGroup;
import software.amazon.awssdk.services.rds.model.DeleteDbParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbParameterGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbParametersRequest;
import software.amazon.awssdk.services.rds.model.DescribeEngineDefaultParametersRequest;
import software.amazon.awssdk.services.rds.model.Filter;
import software.amazon.awssdk.services.rds.model.ModifyDbParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.Parameter;
import software.amazon.awssdk.services.rds.model.ResetDbParameterGroupRequest;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Tagging;

public class Translator {

    public static final String RDS = "rds";
    public static final String RESOURCE_PREFIX = "pg:";
    public static final String FILTER_PARAMETER_NAME = "parameter-name";

    static CreateDbParameterGroupRequest createDbParameterGroupRequest(
            final ResourceModel model,
            final Tagging.TagSet systemTags
    ) {
        return CreateDbParameterGroupRequest.builder()
                .dbParameterGroupName(model.getDBParameterGroupName())
                .description(model.getDescription())
                .dbParameterGroupFamily(model.getFamily())
                .tags(Tagging.translateTagsToSdk(systemTags))
                .build();
    }

    static DescribeDbParameterGroupsRequest describeDbParameterGroupsRequest(final ResourceModel model) {
        return DescribeDbParameterGroupsRequest.builder()
                .dbParameterGroupName(model.getDBParameterGroupName())
                .build();
    }

    static DescribeDbParameterGroupsRequest describeDbParameterGroupsRequest(final String nextToken) {
        return DescribeDbParameterGroupsRequest.builder()
                .marker(nextToken)
                .build();
    }

    static Filter filterByParameterNames(final Collection<String> paramNames) {
        return Filter.builder()
                .name(FILTER_PARAMETER_NAME)
                .values(paramNames)
                .build();
    }

    public static DescribeDbParametersRequest describeDbParametersRequestWithFilters(
            final String dbParameterGroupName,
            final Filter[] filters,
            final String marker
    ) {
        return DescribeDbParametersRequest.builder()
                .dbParameterGroupName(dbParameterGroupName)
                .filters(filters)
                .marker(marker)
                .build();
    }

    public static DescribeEngineDefaultParametersRequest describeEngineDefaultParametersRequestWithFilters(
            final String dbParameterGroupFamily,
            final Filter[] filters,
            final String marker
    ) {
        return DescribeEngineDefaultParametersRequest.builder()
                .dbParameterGroupFamily(dbParameterGroupFamily)
                .filters(filters)
                .marker(marker)
                .build();
    }

    static ModifyDbParameterGroupRequest modifyDbParameterGroupRequest(
            final ResourceModel model,
            final Collection<Parameter> parameters
    ) {
        return ModifyDbParameterGroupRequest.builder()
                .dbParameterGroupName(model.getDBParameterGroupName())
                .parameters(parameters.stream()
                        .map(p -> p.toBuilder().applyMethod(getParameterApplyMethod(p)).build())
                        .collect(Collectors.toList()))
                .build();
    }

    static ResetDbParameterGroupRequest resetDbParametersRequest(
            final ResourceModel model,
            final Collection<Parameter> parameters
    ) {
        return ResetDbParameterGroupRequest.builder()
                .dbParameterGroupName(model.getDBParameterGroupName())
                .parameters(parameters.stream()
                        .map(p -> p.toBuilder().applyMethod(getParameterApplyMethod(p)).build())
                        .collect(Collectors.toList()))
                .build();
    }

    static DeleteDbParameterGroupRequest deleteDbParameterGroupRequest(final ResourceModel model) {
        return DeleteDbParameterGroupRequest.builder()
                .dbParameterGroupName(model.getDBParameterGroupName())
                .build();
    }

    static Parameter buildParameterWithNewValue(
            final String newValue,
            final Parameter parameter
    ) {
        return parameter.toBuilder()
                .parameterValue(newValue)
                .applyMethod(getParameterApplyMethod(parameter))
                .build();
    }

    public static List<software.amazon.awssdk.services.rds.model.Tag> translateTagsToSdk(final Collection<Tag> tags) {
        return Optional.ofNullable(tags).orElse(Collections.emptyList())
                .stream()
                .map(tag -> software.amazon.awssdk.services.rds.model.Tag.builder()
                        .key(tag.getKey())
                        .value(tag.getValue())
                        .build()
                )
                .collect(Collectors.toList());
    }

    static ResourceModel translateFromDBParameterGroup(final DBParameterGroup dbParameterGroup) {
        return ResourceModel.builder()
                .dBParameterGroupName(dbParameterGroup.dbParameterGroupName())
                .description(dbParameterGroup.description())
                .family(dbParameterGroup.dbParameterGroupFamily())
                .build();
    }

    static List<Tag> translateTags(final Collection<software.amazon.awssdk.services.rds.model.Tag> rdsTags) {
        return CollectionUtils.isNullOrEmpty(rdsTags) ? null
                : rdsTags.stream()
                .map(tag -> Tag.builder()
                        .key(tag.key())
                        .value(tag.value())
                        .build())
                .collect(Collectors.toList());
    }

    static Arn buildParameterGroupArn(final ResourceHandlerRequest<ResourceModel> request) {
        String resource = RESOURCE_PREFIX + request.getDesiredResourceState().getDBParameterGroupName();
        return Arn.builder()
                .withPartition(request.getAwsPartition())
                .withRegion(request.getRegion())
                .withService(RDS)
                .withAccountId(request.getAwsAccountId())
                .withResource(resource)
                .build();
    }

    static Map<String, Object> translateParametersFromSdk(@NonNull final Map<String, Parameter> parameters) {
        final Map<String, Object> result = new HashMap<>();
        for (final Map.Entry<String, Parameter> kv : parameters.entrySet()) {
            result.put(kv.getKey(), kv.getValue().parameterValue());
        }

        return result;
    }

    private static ApplyMethod getParameterApplyMethod(final Parameter parameter) {
        if (ParameterType.Dynamic.equalsIgnoreCase(parameter.applyType())) {
            return ApplyMethod.IMMEDIATE;
        }
        return ApplyMethod.PENDING_REBOOT;
    }
}
