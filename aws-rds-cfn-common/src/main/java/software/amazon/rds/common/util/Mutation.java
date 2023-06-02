package software.amazon.rds.common.util;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Mutation {
    @JsonProperty(value = "From")
    private final Object from;

    @JsonProperty(value = "To")
    private final Object to;
}
