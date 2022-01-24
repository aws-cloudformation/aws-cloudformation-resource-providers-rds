package software.amazon.rds.common.logging;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@AllArgsConstructor(access = AccessLevel.PUBLIC)
class CustomerRequestData {
    private String stackId;
    private String awsAccountId;
    private String clientRequestToken;
}
