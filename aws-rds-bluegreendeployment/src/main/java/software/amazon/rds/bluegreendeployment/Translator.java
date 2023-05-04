package software.amazon.rds.bluegreendeployment;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.amazonaws.arn.Arn;
import software.amazon.awssdk.services.rds.model.CreateBlueGreenDeploymentRequest;
import software.amazon.awssdk.services.rds.model.DeleteBlueGreenDeploymentRequest;
import software.amazon.awssdk.services.rds.model.DeleteDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.DescribeBlueGreenDeploymentsRequest;
import software.amazon.awssdk.services.rds.model.SwitchoverBlueGreenDeploymentRequest;
import software.amazon.rds.common.handler.Tagging;

public class Translator {
    public static CreateBlueGreenDeploymentRequest createBlueGreenDeploymentRequest(
            final ResourceModel model,
            final Tagging.TagSet tagSet
    ) {
        return CreateBlueGreenDeploymentRequest.builder()
                .blueGreenDeploymentName(model.getBlueGreenDeploymentName())
                .source(model.getSource())
                .tags(Tagging.translateTagsToSdk(tagSet))
                .targetDBClusterParameterGroupName(model.getTargetDBClusterParameterGroupName())
                .targetDBParameterGroupName(model.getTargetDBParameterGroupName())
                .targetEngineVersion(model.getTargetEngineVersion())
                .build();
    }

    public static DescribeBlueGreenDeploymentsRequest describeBlueGreenDeploymentsRequest(final ResourceModel model) {
        return DescribeBlueGreenDeploymentsRequest.builder()
                .blueGreenDeploymentIdentifier(model.getBlueGreenDeploymentIdentifier())
                .build();
    }

    public static DescribeBlueGreenDeploymentsRequest describeBlueGreenDeploymentsRequest(final String nextToken) {
        return DescribeBlueGreenDeploymentsRequest.builder()
                .marker(nextToken)
                .build();
    }

    public static SwitchoverBlueGreenDeploymentRequest switchoverBlueGreenDeploymentRequest(final ResourceModel model) {
        return SwitchoverBlueGreenDeploymentRequest.builder()
                .blueGreenDeploymentIdentifier(model.getBlueGreenDeploymentIdentifier())
                .switchoverTimeout(model.getSwitchoverTimeout())
                .build();
    }

    public static DeleteBlueGreenDeploymentRequest deleteBlueGreenDeploymentRequest(
            final ResourceModel model,
            final boolean deleteTarget
    ) {
        return DeleteBlueGreenDeploymentRequest.builder()
                .blueGreenDeploymentIdentifier(model.getBlueGreenDeploymentIdentifier())
                .deleteTarget(deleteTarget)
                .build();
    }

    public static DeleteDbInstanceRequest deleteDbInstanceRequest(String dbInstanceIdentifier) {
        return DeleteDbInstanceRequest.builder()
                .dbInstanceIdentifier(dbInstanceIdentifier)
                .skipFinalSnapshot(true)
                .build();
    }

    public static String getDBInstanceIdentifier(final String identifierOrArn) {
        if (looksLikeArn(identifierOrArn)) {
            return parseIdentifierFromArn(identifierOrArn);
        }
        return identifierOrArn;
    }

    public static boolean looksLikeArn(final String arnIdentifier) {
        if (StringUtils.isEmpty(arnIdentifier)) {
            return false;
        }
        try {
            Arn.fromString(arnIdentifier);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    public static String parseIdentifierFromArn(final String arnIdentifier) {
        return Arn.fromString(arnIdentifier).getResource().getResource();
    }

    public static ResourceModel translateBlueGreenDeploymentFromSdk(
            final software.amazon.awssdk.services.rds.model.BlueGreenDeployment blueGreenDeployment
    ) {
        return ResourceModel.builder()
                .blueGreenDeploymentIdentifier(blueGreenDeployment.blueGreenDeploymentIdentifier())
                .blueGreenDeploymentName(blueGreenDeployment.blueGreenDeploymentName())
                .source(blueGreenDeployment.source())
                .target(blueGreenDeployment.target())
                .status(blueGreenDeployment.status())
                .tasks(translateTasksFromSdk(blueGreenDeployment.tasks()))
                .switchoverDetails(translateSwitchoverDetailsFromSdk(blueGreenDeployment.switchoverDetails()))
                .tags(translateTagsFromSdk(blueGreenDeployment.tagList()))
                .build();
    }

    public static List<SwitchoverDetail> translateSwitchoverDetailsFromSdk(
            final Collection<software.amazon.awssdk.services.rds.model.SwitchoverDetail> switchoverDetails
    ) {
        return Optional.ofNullable(switchoverDetails).orElse(Collections.emptyList())
                .stream()
                .map(switchoverDetail -> SwitchoverDetail.builder()
                            .sourceMember(switchoverDetail.sourceMember())
                            .targetMember(switchoverDetail.targetMember())
                            .status(switchoverDetail.status())
                            .build()
                )
                .collect(Collectors.toList());
    }

    public static List<Task> translateTasksFromSdk(
            final Collection<software.amazon.awssdk.services.rds.model.BlueGreenDeploymentTask> tasks
    ) {
        return Optional.ofNullable(tasks).orElse(Collections.emptyList())
                .stream()
                .map(task -> Task.builder()
                        .name(task.name())
                        .status(task.status())
                        .build())
                .collect(Collectors.toList());
    }

    public static Set<Tag> translateTagsFromSdk(
            final Collection<software.amazon.awssdk.services.rds.model.Tag> tags
    ) {
        return Optional.ofNullable(tags).orElse(Collections.emptySet())
                .stream()
                .map(tag -> Tag.builder()
                        .key(tag.key())
                        .value(tag.value())
                        .build())
                .collect(Collectors.toSet());
    }

    public static Set<software.amazon.awssdk.services.rds.model.Tag> translateTagsToSdk(final Collection<Tag> tags) {
        return Optional.ofNullable(tags).orElse(Collections.emptyList())
                .stream()
                .map(tag -> software.amazon.awssdk.services.rds.model.Tag.builder()
                        .key(tag.getKey())
                        .value(tag.getValue())
                        .build()
                )
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }
}
