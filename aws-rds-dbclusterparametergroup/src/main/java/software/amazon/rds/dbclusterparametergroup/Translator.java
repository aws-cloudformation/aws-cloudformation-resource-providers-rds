package software.amazon.rds.dbclusterparametergroup;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;

import com.amazonaws.arn.Arn;
import software.amazon.awssdk.services.rds.model.ApplyMethod;
import software.amazon.awssdk.services.rds.model.CreateDbClusterParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.DeleteDbClusterParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParameterGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParametersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.Filter;
import software.amazon.awssdk.services.rds.model.ModifyDbClusterParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.Parameter;
import software.amazon.awssdk.services.rds.model.ResetDbClusterParameterGroupRequest;
import software.amazon.cloudformation.exceptions.CfnInternalFailureException;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Tagging;

public class Translator {

    public static final String RDS = "rds";
    public static final String RESOURCE_PREFIX = "cluster-pg:";
    public static final String FILTER_PARAMETER_NAME = "parameter-name";

    static CreateDbClusterParameterGroupRequest createDbClusterParameterGroupRequest(
            final ResourceModel model,
            final Tagging.TagSet tags
    ) {
        return CreateDbClusterParameterGroupRequest.builder()
                .dbClusterParameterGroupName(model.getDBClusterParameterGroupName())
                .dbParameterGroupFamily(model.getFamily())
                .description(model.getDescription())
                .tags(Tagging.translateTagsToSdk(tags))
                .build();
    }

    static DescribeDbClusterParametersRequest describeDbClusterParametersFilteredRequest(
            final ResourceModel model,
            final List<String> parameterNames,
            final String marker
    ) {
        if (CollectionUtils.isEmpty(parameterNames)) {
            throw new CfnInternalFailureException(new Exception("Parameter names should not be empty"));
        }
        return DescribeDbClusterParametersRequest.builder()
                .dbClusterParameterGroupName(model.getDBClusterParameterGroupName())
                .filters(Filter.builder()
                        .name(FILTER_PARAMETER_NAME)
                        .values(parameterNames)
                        .build())
                .marker(marker)
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

    static ModifyDbClusterParameterGroupRequest modifyDbClusterParameterGroupRequest(
            final ResourceModel model,
            final Collection<Parameter> parameters
    ) {
        return ModifyDbClusterParameterGroupRequest.builder()
                .dbClusterParameterGroupName(model.getDBClusterParameterGroupName())
                .parameters(parameters)
                .build();
    }

    static ResetDbClusterParameterGroupRequest resetDbClusterParameterGroupRequest(final ResourceModel model) {
        return ResetDbClusterParameterGroupRequest.builder()
                .dbClusterParameterGroupName(model.getDBClusterParameterGroupName())
                .resetAllParameters(true)
                .build();
    }

    static List<Tag> translateTagsFromSdk(final Collection<software.amazon.awssdk.services.rds.model.Tag> tags) {
        return streamOfOrEmpty(tags)
                .map(tag -> software.amazon.rds.dbclusterparametergroup.Tag.builder()
                        .key(tag.key())
                        .value(tag.value()).build())
                .collect(Collectors.toList());
    }

    public static Set<software.amazon.awssdk.services.rds.model.Tag> translateTagsToSdk(final Collection<Tag> tags) {
        return streamOfOrEmpty(tags)
                .map(tag -> software.amazon.awssdk.services.rds.model.Tag.builder()
                        .key(tag.getKey())
                        .value(tag.getValue())
                        .build()
                )
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    public static Parameter buildParameterWithNewValue(final String newValue, final Parameter parameter) {
        final Parameter.Builder param = parameter.toBuilder()
                .parameterValue(newValue);

        if (parameter.applyType().equalsIgnoreCase(ParameterType.Static.toString()))  // If the parameter is STATIC, flag for pending reboot
            param.applyMethod(ApplyMethod.PENDING_REBOOT);
        else if (parameter.applyType().equalsIgnoreCase(ParameterType.Dynamic.toString()))   // If the parameter is DYNAMIC, we can apply now
            param.applyMethod(ApplyMethod.IMMEDIATE).build();

        return param.build();
    }

    private static <T> Stream<T> streamOfOrEmpty(final Collection<T> collection) {
        return Optional.ofNullable(collection)
                .map(Collection::stream)
                .orElseGet(Stream::empty);
    }

    public static Arn buildClusterParameterGroupArn(final ResourceHandlerRequest<ResourceModel> request) {
        final String resource = RESOURCE_PREFIX + request.getDesiredResourceState().getDBClusterParameterGroupName();
        return Arn.builder()
                .withPartition(request.getAwsPartition())
                .withRegion(request.getRegion())
                .withService(RDS)
                .withAccountId(request.getAwsAccountId())
                .withResource(resource)
                .build();
    }
}
