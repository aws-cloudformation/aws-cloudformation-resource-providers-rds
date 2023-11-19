# AWS::RDS::Integration

An example resource schema demonstrating some basic constructs and validation rules.

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "Type" : "AWS::RDS::Integration",
    "Properties" : {
        "<a href="#integrationname" title="IntegrationName">IntegrationName</a>" : <i>String</i>,
        "<a href="#tags" title="Tags">Tags</a>" : <i>[ <a href="tag.md">Tag</a>, ... ]</i>,
        "<a href="#sourcearn" title="SourceArn">SourceArn</a>" : <i>String</i>,
        "<a href="#targetarn" title="TargetArn">TargetArn</a>" : <i>String</i>,
        "<a href="#kmskeyid" title="KMSKeyId">KMSKeyId</a>" : <i>String</i>,
        "<a href="#additionalencryptioncontext" title="AdditionalEncryptionContext">AdditionalEncryptionContext</a>" : <i><a href="additionalencryptioncontext.md">AdditionalEncryptionContext</a></i>,
    }
}
</pre>

### YAML

<pre>
Type: AWS::RDS::Integration
Properties:
    <a href="#integrationname" title="IntegrationName">IntegrationName</a>: <i>String</i>
    <a href="#tags" title="Tags">Tags</a>: <i>
      - <a href="tag.md">Tag</a></i>
    <a href="#sourcearn" title="SourceArn">SourceArn</a>: <i>String</i>
    <a href="#targetarn" title="TargetArn">TargetArn</a>: <i>String</i>
    <a href="#kmskeyid" title="KMSKeyId">KMSKeyId</a>: <i>String</i>
    <a href="#additionalencryptioncontext" title="AdditionalEncryptionContext">AdditionalEncryptionContext</a>: <i><a href="additionalencryptioncontext.md">AdditionalEncryptionContext</a></i>
</pre>

## Properties

#### IntegrationName

The name of the integration.

_Required_: No

_Type_: String

_Minimum Length_: <code>1</code>

_Maximum Length_: <code>64</code>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### Tags

An array of key-value pairs to apply to this resource.

_Required_: No

_Type_: List of <a href="tag.md">Tag</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### SourceArn

The Amazon Resource Name (ARN) of the Aurora DB cluster to use as the source for replication.

_Required_: Yes

_Type_: String

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### TargetArn

The ARN of the Redshift data warehouse to use as the target for replication.

_Required_: Yes

_Type_: String

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### KMSKeyId

An optional AWS Key Management System (AWS KMS) key ARN for the key used to to encrypt the integration. The resource accepts the key ID and the key ARN forms. The key ID form can be used if the KMS key is owned by te same account. If the KMS key belongs to a different account than the calling account, the full key ARN must be specified. Do not use the key alias or the key alias ARN as this will cause a false drift of the resource.

_Required_: No

_Type_: String

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### AdditionalEncryptionContext

An optional set of non-secret key–value pairs that contains additional contextual information about the data.

_Required_: No

_Type_: <a href="additionalencryptioncontext.md">AdditionalEncryptionContext</a>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

## Return Values

### Ref

When you pass the logical ID of this resource to the intrinsic `Ref` function, Ref returns the IntegrationArn.

### Fn::GetAtt

The `Fn::GetAtt` intrinsic function returns a value for a specified attribute of this type. The following are the available attributes and sample return values.

For more information about using the `Fn::GetAtt` intrinsic function, see [Fn::GetAtt](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/intrinsic-function-reference-getatt.html).

#### IntegrationArn

The ARN of the integration.

#### CreateTime

Returns the <code>CreateTime</code> value.
