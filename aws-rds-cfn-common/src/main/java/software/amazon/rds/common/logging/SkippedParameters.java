package software.amazon.rds.common.logging;

import software.amazon.awssdk.utils.StringUtils;
import software.amazon.rds.common.error.ErrorCode;

public enum SkippedParameters {

    MasterUsername("masterUsername"),
    MasterUserPassword("masterUserPassword"),
    TdeCredentialPassword("tdeCredentialPassword");
    
    private String value;
    
    SkippedParameters(final String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }

    public boolean equals(final String cmp) {
        return value.equals(cmp);
    }
    
}
