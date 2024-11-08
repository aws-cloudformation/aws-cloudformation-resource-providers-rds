# AWS::RDS::DBCluster MasterUserSecret

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "<a href="#kmskeyid" title="KmsKeyId">KmsKeyId</a>" : <i>String</i>
}
</pre>

### YAML

<pre>
<a href="#kmskeyid" title="KmsKeyId">KmsKeyId</a>: <i>String</i>
</pre>

## Properties

#### KmsKeyId

The AWS KMS key identifier that is used to encrypt the secret.

_Required_: No

_Type_: String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)
