package software.amazon.rds.dbparametergroup;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.amazonaws.util.CollectionUtils;
import software.amazon.awssdk.services.rds.model.ApplyMethod;
import software.amazon.awssdk.services.rds.model.CreateDbParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.DBParameterGroup;
import software.amazon.awssdk.services.rds.model.DeleteDbParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbParameterGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbParametersRequest;
import software.amazon.awssdk.services.rds.model.DescribeEngineDefaultParametersRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.Parameter;
import software.amazon.awssdk.services.rds.model.ResetDbParameterGroupRequest;
import software.amazon.rds.common.handler.Tagging;

public class Translator {

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

    static DescribeDbParametersRequest describeDbParametersRequest(final ResourceModel model) {
        return DescribeDbParametersRequest.builder()
                .dbParameterGroupName(model.getDBParameterGroupName())
                .build();
    }

    public static DescribeEngineDefaultParametersRequest describeEngineDefaultParametersRequest(ResourceModel model) {
        return DescribeEngineDefaultParametersRequest.builder()
                .dbParameterGroupFamily(model.getFamily())
                .build();
    }

    static ModifyDbParameterGroupRequest modifyDbParameterGroupRequest(
            final ResourceModel model,
            final Collection<Parameter> parameters
    ) {
        return ModifyDbParameterGroupRequest.builder()
                .dbParameterGroupName(model.getDBParameterGroupName())
                .parameters(parameters)
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
        final Parameter.Builder param = parameter.toBuilder()
                .parameterValue(newValue)
                .applyMethod(getParameterApplyMethod(parameter));
        return param.build();
    }

    static Map<String, String> translateTagsFromSdk(Collection<Tag> tags) {
        return Optional.ofNullable(tags).orElse(Collections.emptyList())
                .stream()
                .collect(Collectors.toMap(Tag::getKey, Tag::getValue));
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

    private static ApplyMethod getParameterApplyMethod(final Parameter parameter) {
        if (ParameterType.Dynamic.equalsIgnoreCase(parameter.applyType())) {
            return ApplyMethod.IMMEDIATE;
        }
        return ApplyMethod.PENDING_REBOOT;
    }
}
