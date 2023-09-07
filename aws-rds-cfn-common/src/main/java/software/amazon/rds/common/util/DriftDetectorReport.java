package software.amazon.rds.common.util;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import software.amazon.rds.common.annotations.ExcludeFromJacocoGeneratedReport;

@Data
@ExcludeFromJacocoGeneratedReport
public class DriftDetectorReport {
    @JsonProperty(value = "Mutations")
    private final Map<String, Mutation> mutations;
}
