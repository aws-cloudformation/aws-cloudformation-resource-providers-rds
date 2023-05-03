# AWS::RDS::BlueGreenDeployment

An example resource schema demonstrating some basic constructs and validation rules.

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "Type" : "AWS::RDS::BlueGreenDeployment",
    "Properties" : {
        "<a href="#bluegreendeploymentname" title="BlueGreenDeploymentName">BlueGreenDeploymentName</a>" : <i>String</i>,
        "<a href="#deletetarget" title="DeleteTarget">DeleteTarget</a>" : <i>Boolean</i>,
        "<a href="#stage" title="Stage">Stage</a>" : <i>String</i>,
        "<a href="#switchovertimeout" title="SwitchoverTimeout">SwitchoverTimeout</a>" : <i>Integer</i>,
        "<a href="#source" title="Source">Source</a>" : <i>String</i>,
        "<a href="#targetengineversion" title="TargetEngineVersion">TargetEngineVersion</a>" : <i>String</i>,
        "<a href="#targetdbparametergroupname" title="TargetDBParameterGroupName">TargetDBParameterGroupName</a>" : <i>String</i>,
        "<a href="#targetdbclusterparametergroupname" title="TargetDBClusterParameterGroupName">TargetDBClusterParameterGroupName</a>" : <i>String</i>,
        "<a href="#tags" title="Tags">Tags</a>" : <i>[ <a href="tag.md">Tag</a>, ... ]</i>
    }
}
</pre>

### YAML

<pre>
Type: AWS::RDS::BlueGreenDeployment
Properties:
    <a href="#bluegreendeploymentname" title="BlueGreenDeploymentName">BlueGreenDeploymentName</a>: <i>String</i>
    <a href="#deletetarget" title="DeleteTarget">DeleteTarget</a>: <i>Boolean</i>
    <a href="#stage" title="Stage">Stage</a>: <i>String</i>
    <a href="#switchovertimeout" title="SwitchoverTimeout">SwitchoverTimeout</a>: <i>Integer</i>
    <a href="#source" title="Source">Source</a>: <i>String</i>
    <a href="#targetengineversion" title="TargetEngineVersion">TargetEngineVersion</a>: <i>String</i>
    <a href="#targetdbparametergroupname" title="TargetDBParameterGroupName">TargetDBParameterGroupName</a>: <i>String</i>
    <a href="#targetdbclusterparametergroupname" title="TargetDBClusterParameterGroupName">TargetDBClusterParameterGroupName</a>: <i>String</i>
    <a href="#tags" title="Tags">Tags</a>: <i>
      - <a href="tag.md">Tag</a></i>
</pre>

## Properties

#### BlueGreenDeploymentName

_Required_: No

_Type_: String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### DeleteTarget

_Required_: No

_Type_: Boolean

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### Stage

_Required_: Yes

_Type_: String

_Allowed Values_: <code>blue</code> | <code>green</code>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### SwitchoverTimeout

_Required_: No

_Type_: Integer

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### Source

_Required_: Yes

_Type_: String

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### TargetEngineVersion

_Required_: No

_Type_: String

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### TargetDBParameterGroupName

_Required_: No

_Type_: String

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### TargetDBClusterParameterGroupName

_Required_: No

_Type_: String

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### Tags

An array of key-value pairs to apply to this resource.

_Required_: No

_Type_: List of <a href="tag.md">Tag</a>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

## Return Values

### Ref

When you pass the logical ID of this resource to the intrinsic `Ref` function, Ref returns the BlueGreenDeploymentIdentifier.

### Fn::GetAtt

The `Fn::GetAtt` intrinsic function returns a value for a specified attribute of this type. The following are the available attributes and sample return values.

For more information about using the `Fn::GetAtt` intrinsic function, see [Fn::GetAtt](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/intrinsic-function-reference-getatt.html).

#### BlueGreenDeploymentIdentifier

Returns the <code>BlueGreenDeploymentIdentifier</code> value.

#### Status

Returns the <code>Status</code> value.

#### SwitchoverDetails

Returns the <code>SwitchoverDetails</code> value.

#### Target

Returns the <code>Target</code> value.

#### Tasks

Returns the <code>Tasks</code> value.
