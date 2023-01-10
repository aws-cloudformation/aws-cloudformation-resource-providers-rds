# AWS::RDS::DBInstance DBInstanceRole

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "<a href="#featurename" title="FeatureName">FeatureName</a>" : <i>String</i>,
    "<a href="#rolearn" title="RoleArn">RoleArn</a>" : <i>String</i>
}
</pre>

### YAML

<pre>
<a href="#featurename" title="FeatureName">FeatureName</a>: <i>String</i>
<a href="#rolearn" title="RoleArn">RoleArn</a>: <i>String</i>
</pre>

## Properties

#### FeatureName

The name of the feature associated with the AWS Identity and Access Management (IAM) role. IAM roles that are associated with a DB instance grant permission for the DB instance to access other AWS services on your behalf.

_Required_: Yes

_Type_: String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### RoleArn

The Amazon Resource Name (ARN) of the IAM role that is associated with the DB instance.

_Required_: Yes

_Type_: String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)
