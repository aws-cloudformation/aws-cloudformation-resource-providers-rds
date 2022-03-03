package software.amazon.rds.dbclusterparametergroup;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import software.amazon.awssdk.services.rds.model.ApplyMethod;
import software.amazon.awssdk.services.rds.model.CreateDbClusterParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.DeleteDbClusterParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParameterGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParametersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeEngineDefaultClusterParametersRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbClusterParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.Parameter;
import software.amazon.awssdk.services.rds.model.ResetDbClusterParameterGroupRequest;
import software.amazon.rds.common.handler.Tagging;

public class Translator {

    static CreateDbClusterParameterGroupRequest createDbClusterParameterGroupRequest(final ResourceModel model,
                                                                                     final Map<String, String> tags) {
        return CreateDbClusterParameterGroupRequest.builder()
                .dbClusterParameterGroupName(model.getDBClusterParameterGroupName())
                .dbParameterGroupFamily(model.getFamily())
                .description(model.getDescription())
                .tags(Tagging.translateTagsToSdk(tags))
                .build();
    }

    static DescribeDbClusterParametersRequest describeDbClusterParametersRequest(final ResourceModel model) {
        return DescribeDbClusterParametersRequest.builder()
                .dbClusterParameterGroupName(model.getDBClusterParameterGroupName())
                .build();
    }

    static DescribeDbClustersRequest describeDbClustersRequest() {
        return DescribeDbClustersRequest.builder().build();
    }

    static DeleteDbClusterParameterGroupRequest deleteDbClusterParameterGroupRequest(final ResourceModel model) {
        return DeleteDbClusterParameterGroupRequest.builder()
                .dbClusterParameterGroupName(model.getDBClusterParameterGroupName())
                .build();
    }

    static DescribeDbClusterParameterGroupsRequest describeDbClusterParameterGroupsRequest(final ResourceModel model) {
        return DescribeDbClusterParameterGroupsRequest.builder()
                .dbClusterParameterGroupName(model.getDBClusterParameterGroupName())
                .build();
    }

    static DescribeDbClusterParameterGroupsRequest describeDbClusterParameterGroupsRequest(final String nextToken) {
        return DescribeDbClusterParameterGroupsRequest.builder()
                .marker(nextToken)
                .build();
    }

    static ModifyDbClusterParameterGroupRequest modifyDbClusterParameterGroupRequest(final ResourceModel model,
                                                                                     final Collection<Parameter> parameters) {
        return ModifyDbClusterParameterGroupRequest.builder()
                .dbClusterParameterGroupName(model.getDBClusterParameterGroupName())
                .parameters(parameters)
                .build();
    }

    static ResetDbClusterParameterGroupRequest resetDbClusterParameterGroupRequest(final ResourceModel model,
                                                                                   final Collection<Parameter> parameters) {
        return ResetDbClusterParameterGroupRequest.builder()
                .dbClusterParameterGroupName(model.getDBClusterParameterGroupName())
                .parameters(parameters)
                .build();
    }

    static List<Tag> translateTagsFromSdk(final Collection<software.amazon.awssdk.services.rds.model.Tag> tags) {
        return Optional.ofNullable(tags).orElse(Collections.emptySet())
                .stream()
                .map(tag -> software.amazon.rds.dbclusterparametergroup.Tag.builder()
                        .key(tag.key())
                        .value(tag.value()).build())
                .collect(Collectors.toList());
    }

    public static DescribeEngineDefaultClusterParametersRequest describeEngineDefaultClusterParametersRequest(final ResourceModel resourceModel,
                                                                                                              String marker,
                                                                                                              int maxRecords) {
        return DescribeEngineDefaultClusterParametersRequest.builder()
                .dbParameterGroupFamily(resourceModel.getFamily())
                .marker(marker)
                .maxRecords(maxRecords)
                .build();
    }

    public static Parameter buildParameterWithNewValue(final String newValue,
                                                       final Parameter parameter) {
        final Parameter.Builder param = parameter.toBuilder()
                .parameterValue(newValue);

        if (parameter.applyType().equalsIgnoreCase(ParameterType.Static.toString()))  // If the parameter is STATIC, flag for pending reboot
            param.applyMethod(ApplyMethod.PENDING_REBOOT);
        else if (parameter.applyType().equalsIgnoreCase(ParameterType.Dynamic.toString()))   // If the parameter is DYNAMIC, we can apply now
            param.applyMethod(ApplyMethod.IMMEDIATE).build();

        return param.build();
    }
}
