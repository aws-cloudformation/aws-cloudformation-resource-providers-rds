package software.amazon.rds.dbparametergroup;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.ApplyMethod;
import software.amazon.awssdk.services.rds.model.CreateDbParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.DeleteDbParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbParameterGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbParametersRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.Parameter;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceRequest;
import software.amazon.awssdk.services.rds.model.ResetDbParameterGroupRequest;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;

public class Translator {

    static CreateDbParameterGroupRequest createDbParameterGroupRequest(final ResourceModel model, final Map<String, String> tags) {
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

    static DescribeDbParametersRequest describeDbParameterGroupsRequest(final ResourceModel model, final String nextToken, int recordsPerPage) {
        return DescribeDbParametersRequest.builder()
                .dbParameterGroupName(model.getDBParameterGroupName())
                .marker(nextToken)
                .maxRecords(recordsPerPage)
                .build();
    }

    static ModifyDbParameterGroupRequest modifyDbParameterGroupRequest(final ResourceModel model,
                                                                       final Collection<Parameter> parameters) {
        return ModifyDbParameterGroupRequest.builder()
                .dbParameterGroupName(model.getDBParameterGroupName())
                .parameters(parameters)
                .build();
    }

    static DeleteDbParameterGroupRequest deleteDbParameterGroupRequest(final ResourceModel model) {
        return DeleteDbParameterGroupRequest.builder()
                .dbParameterGroupName(model.getDBParameterGroupName())
                .build();
    }

    static ResetDbParameterGroupRequest resetDbParameterGroupRequest(final ResourceModel model) {
        return ResetDbParameterGroupRequest.builder()
                .dbParameterGroupName(model.getDBParameterGroupName())
                .resetAllParameters(true)
                .build();
    }

    static Set<Tag> translateTagsFromSdk(final Collection<software.amazon.awssdk.services.rds.model.Tag> tags) {
        return Optional.ofNullable(tags).orElse(Collections.emptySet())
                .stream()
                .map(tag -> {
                    return Tag.builder()
                            .key(tag.key())
                            .value(tag.value())
                            .build();
                })
                .collect(Collectors.toSet());
    }

    protected static Set<Parameter> getParametersToModify(final ResourceModel model,
                                                          final List<Parameter> parameters) {
        return parameters.stream()
                .filter(parameter -> model.getParameters().containsKey(parameter.parameterName()))
                .map(parameter -> modifyParameter(model.getParameters(), parameter))
                .collect(Collectors.toSet());
    }

    private static Parameter modifyParameter(final Map<String, Object> parameters,
                                             final Parameter parameter) {
        if (!parameter.isModifiable())
            throw new CfnInvalidRequestException("Unmodifiable DB Parameter: " + parameter.parameterName());

        final Parameter.Builder param = Parameter.builder()
                .parameterName(parameter.parameterName())
                .parameterValue(String.valueOf(parameters.get(parameter.parameterName())))
                .applyType(parameter.applyType())
                .isModifiable(parameter.isModifiable());

        if (parameter.applyType().equalsIgnoreCase(ParameterType.Static.toString()))  // If the parameter is STATIC, flag for pending reboot
            param.applyMethod(ApplyMethod.PENDING_REBOOT);
        else if (parameter.applyType().equalsIgnoreCase(ParameterType.Dynamic.toString()))   // If the parameter is DYNAMIC, we can apply now
            param.applyMethod(ApplyMethod.IMMEDIATE).build();

        return param.build();
    }


    static Set<software.amazon.awssdk.services.rds.model.Tag> translateTagsToSdk(final Map<String, String> tags) {
        System.out.println(tags);
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

    static Set<software.amazon.awssdk.services.rds.model.Tag> translateTagsToSdk(final Collection<software.amazon.rds.dbparametergroup.Tag> tags) {
        return Optional.ofNullable(tags).orElse(Collections.emptySet())
                .stream()
                .map(tag -> software.amazon.awssdk.services.rds.model.Tag.builder().key(tag.getKey()).value(tag.getValue()).build())
                .collect(Collectors.toSet());
    }

    static Set<Tag> translateTagsToModelResource(final Map<String, String> tags) {
        return Optional.ofNullable(tags).orElse(Collections.emptyMap())
                .entrySet()
                .stream()
                .map(entry -> {
                    return Tag.builder()
                            .key(entry.getKey())
                            .value(entry.getValue())
                            .build();
                })
                .collect(Collectors.toSet());
    }

    static AddTagsToResourceRequest addTagsToResourceRequest(final String arn, final Set<Tag> tags) {
        return AddTagsToResourceRequest.builder()
                .resourceName(arn)
                .tags(translateTagsToSdk(tags))
                .build();
    }

    static RemoveTagsFromResourceRequest removeTagsFromResourceRequest(final String arn, final Set<Tag> tags) {
        return RemoveTagsFromResourceRequest.builder()
                .resourceName(arn)
                .tagKeys(tags.stream().map(Tag::getKey).collect(Collectors.toSet()))
                .build();
    }

    static ListTagsForResourceRequest listTagsForResourceRequest(final String arn) {
        return ListTagsForResourceRequest.builder()
                .resourceName(arn)
                .build();
    }

    private static <T> Stream<T> streamOfOrEmpty(final Collection<T> collection) {
        return Optional.ofNullable(collection)
                .map(Collection::stream)
                .orElseGet(Stream::empty);
    }
}
