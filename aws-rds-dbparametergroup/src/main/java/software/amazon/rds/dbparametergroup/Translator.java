package software.amazon.rds.dbparametergroup;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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

public class Translator {

    static CreateDbParameterGroupRequest createDbParameterGroupRequest(
            final ResourceModel model,
            final Map<String, String> tags
    ) {
        return CreateDbParameterGroupRequest.builder()
                .dbParameterGroupName(model.getDBParameterGroupName())
                .description(model.getDescription())
                .dbParameterGroupFamily(model.getFamily())
                .tags(translateTagsToSdk(tags))
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
                .parameters(parameters)
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
                .parameterValue(newValue);

        if (parameter.applyType().equalsIgnoreCase(ParameterType.Static.toString()))  // If the parameter is STATIC, flag for pending reboot
            param.applyMethod(ApplyMethod.PENDING_REBOOT);
        else if (parameter.applyType().equalsIgnoreCase(ParameterType.Dynamic.toString()))   // If the parameter is DYNAMIC, we can apply now
            param.applyMethod(ApplyMethod.IMMEDIATE).build();

        return param.build();
    }


    static Set<software.amazon.awssdk.services.rds.model.Tag> translateTagsToSdk(final Map<String, String> tags) {
        return Optional.ofNullable(tags.entrySet()).orElse(Collections.emptySet())
                .stream()
                .map(tag -> {
                    return software.amazon.awssdk.services.rds.model.Tag.builder()
                            .key(tag.getKey())
                            .value(tag.getValue())
                            .build();
                })
                .collect(Collectors.toSet());
    }

    static ResourceModel translateFromDBParameterGroup(
            final DBParameterGroup dbParameterGroup,
            final Set<software.amazon.awssdk.services.rds.model.Tag> rdsTags
    ) {
        List<Tag> tags = CollectionUtils.isNullOrEmpty(rdsTags) ? null
                : rdsTags.stream()
                .map(tag -> Tag.builder()
                        .key(tag.key())
                        .value(tag.value())
                        .build())
                .collect(Collectors.toList());

        return ResourceModel.builder()
                .dBParameterGroupName(dbParameterGroup.dbParameterGroupName())
                .description(dbParameterGroup.description())
                .family(dbParameterGroup.dbParameterGroupFamily())
                .tags(tags)
                .build();
    }
}
