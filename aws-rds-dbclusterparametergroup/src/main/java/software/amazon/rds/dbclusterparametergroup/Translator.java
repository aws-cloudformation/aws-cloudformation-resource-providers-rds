package software.amazon.rds.dbclusterparametergroup;

import software.amazon.awssdk.services.rds.model.ApplyMethod;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.CreateDbClusterParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParameterGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParametersRequest;
import software.amazon.awssdk.services.rds.model.DeleteDbClusterParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbClusterParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.ResetDbClusterParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceRequest;
import software.amazon.awssdk.services.rds.model.Parameter;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.TerminalException;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class Translator {
    private static final int MAX_RECORDS_TO_DESCRIBE = 20;
    private static final String STATIC_TYPE = "static";
    private static final String DYNAMIC_TYPE = "dynamic";
    private static final ApplyMethod IMMEDIATE_APPLY_METHOD = ApplyMethod.IMMEDIATE;;
    private static final ApplyMethod PENDING_REBOOT_APPLY_METHOD = ApplyMethod.PENDING_REBOOT;

    static CreateDbClusterParameterGroupRequest createDbClusterParameterGroupRequest(final ResourceModel model,
                                                                                     final Map<String, String> tags) {
        return CreateDbClusterParameterGroupRequest.builder()
                .dbClusterParameterGroupName(model.getId())
                .dbParameterGroupFamily(model.getFamily())
                .description(model.getDescription())
                .tags(translateTagsToSdk(tags))
                .build();
    }

    static DescribeDbClusterParametersRequest describeDbClusterParametersRequest(final ResourceModel model,
                                                                                 final String nextToken) {
        return DescribeDbClusterParametersRequest.builder()
                .dbClusterParameterGroupName(model.getId())
                .marker(nextToken)
                .maxRecords(MAX_RECORDS_TO_DESCRIBE)
                .build();
    }

    static DeleteDbClusterParameterGroupRequest deleteDbClusterParameterGroupRequest(final ResourceModel model) {
        return DeleteDbClusterParameterGroupRequest.builder()
                .dbClusterParameterGroupName(model.getId())
                .build();
    }

    static DescribeDbClusterParameterGroupsRequest describeDbClusterParameterGroupsRequest(final ResourceModel model) {
        return DescribeDbClusterParameterGroupsRequest.builder()
                .dbClusterParameterGroupName(model.getId())
                .build();
    }

    static DescribeDbClusterParameterGroupsRequest describeDbClusterParameterGroupsRequest(final String nextToken) {
        return DescribeDbClusterParameterGroupsRequest.builder()
                .marker(nextToken)
                .build();
    }

    static ModifyDbClusterParameterGroupRequest modifyDbClusterParameterGroupRequest(final ResourceModel model,
                                                                                     final Set<Parameter> parameters) {
        return ModifyDbClusterParameterGroupRequest.builder()
                .dbClusterParameterGroupName(model.getId())
                .parameters(parameters)
                .build();
    }

    static ResetDbClusterParameterGroupRequest resetDbClusterParameterGroupRequest(final ResourceModel model) {
        return ResetDbClusterParameterGroupRequest.builder()
                .dbClusterParameterGroupName(model.getId())
                .resetAllParameters(true)
                .build();
    }

    static ListTagsForResourceRequest listTagsForResourceRequest(final String dbClusterParameterGroupArn) {
        return ListTagsForResourceRequest.builder()
                .resourceName(dbClusterParameterGroupArn)
                .build();
    }

    static AddTagsToResourceRequest addTagsToResourceRequest(final String dbClusterParameterGroupArn,
                                                             final Set<software.amazon.rds.dbclusterparametergroup.Tag> tags) {
        return AddTagsToResourceRequest.builder()
                .resourceName(dbClusterParameterGroupArn)
                .tags(translateTagsToSdk(tags))
                .build();
    }

    static RemoveTagsFromResourceRequest removeTagsFromResourceRequest(final String dbClusterParameterGroupArn,
                                                                       final Set<software.amazon.rds.dbclusterparametergroup.Tag> tags) {
        return RemoveTagsFromResourceRequest.builder()
                .resourceName(dbClusterParameterGroupArn)
                .tagKeys(tags
                        .stream()
                        .map(software.amazon.rds.dbclusterparametergroup.Tag::getKey)
                        .collect(Collectors.toSet()))
                .build();
    }


    protected static Set<Parameter> getParametersToModify(final ResourceModel model,
                                                          final List<Parameter> parameters) {
        return parameters.stream()
                .filter(parameter -> model.getParameters().containsKey(parameter.parameterName()))
                .map(parameter -> modifyParameter(model.getParameters(), parameter))
                .collect(Collectors.toSet());
    }

    protected static Parameter modifyParameter(final Map<String, Object> parameters,
                                               final Parameter parameter) {
        if (!parameter.isModifiable()) throw new CfnInvalidRequestException("Unmodifiable DB Parameter: " + parameter.parameterName());

        final Parameter.Builder param = Parameter.builder()
                .parameterName(parameter.parameterName()) // char set etc
                .parameterValue(String.valueOf(parameters.get(parameter.parameterName()))) // utf32
                .applyType(parameter.applyType()) //
                .isModifiable(parameter.isModifiable()); //

        // If the parameter is STATIC, flag for pending reboot
        if (parameter.applyType().equalsIgnoreCase(STATIC_TYPE))
            return param.applyMethod(PENDING_REBOOT_APPLY_METHOD).build(); // pending reboot

        // If the parameter is DYNAMIC, we can apply now
        else if (parameter.applyType().equalsIgnoreCase(DYNAMIC_TYPE)) {
            return param.applyMethod(IMMEDIATE_APPLY_METHOD).build();
        }

        return param.build();
    }

    // Translate tags
    static Set<Tag> translateTagsToSdk(final Collection<software.amazon.rds.dbclusterparametergroup.Tag> tags) {
        return Optional.ofNullable(tags).orElse(Collections.emptySet())
                .stream()
                .map(tag -> Tag.builder().key(tag.getKey()).value(tag.getValue()).build())
                .collect(Collectors.toSet());
    }

    // Translate tags
    static Set<Tag> translateTagsToSdk(final Map<String, String> tags) {
        return Optional.of(tags.entrySet()).orElse(Collections.emptySet())
                .stream()
                .map(tag -> Tag.builder().key(tag.getKey()).value(tag.getValue()).build())
                .collect(Collectors.toSet());
    }

    static Set<software.amazon.rds.dbclusterparametergroup.Tag> mapToTags(final Map<String, String> tags) {
        return Optional.of(tags.entrySet()).orElse(Collections.emptySet())
                .stream()
                .map(entry -> software.amazon.rds.dbclusterparametergroup.Tag.builder().key(entry.getKey()).value(entry.getValue()).build())
                .collect(Collectors.toSet());
    }

    static Set<software.amazon.rds.dbclusterparametergroup.Tag> translateTagsFromSdk(final Collection<Tag> tags) {
        return Optional.ofNullable(tags).orElse(Collections.emptySet())
                .stream()
                .map(tag -> software.amazon.rds.dbclusterparametergroup.Tag.builder()
                        .key(tag.key())
                        .value(tag.value()).build())
                .collect(Collectors.toSet());
    }
}
