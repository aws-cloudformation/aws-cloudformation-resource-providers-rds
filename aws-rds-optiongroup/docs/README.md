# AWS::RDS::OptionGroup

The AWS::RDS::OptionGroup resource creates an option group, to enable and configure features that are specific to a particular DB engine.

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "Type" : "AWS::RDS::OptionGroup",
    "Properties" : {
        "<a href="#optiongroupdescription" title="OptionGroupDescription">OptionGroupDescription</a>" : <i>String</i>,
        "<a href="#enginename" title="EngineName">EngineName</a>" : <i>String</i>,
        "<a href="#majorengineversion" title="MajorEngineVersion">MajorEngineVersion</a>" : <i>String</i>,
        "<a href="#optionconfigurations" title="OptionConfigurations">OptionConfigurations</a>" : <i>[ <a href="optionconfiguration.md">OptionConfiguration</a>, ... ]</i>,
        "<a href="#tags" title="Tags">Tags</a>" : <i>[ <a href="tag.md">Tag</a>, ... ]</i>
    }
}
</pre>

### YAML

<pre>
Type: AWS::RDS::OptionGroup
Properties:
    <a href="#optiongroupdescription" title="OptionGroupDescription">OptionGroupDescription</a>: <i>String</i>
    <a href="#enginename" title="EngineName">EngineName</a>: <i>String</i>
    <a href="#majorengineversion" title="MajorEngineVersion">MajorEngineVersion</a>: <i>String</i>
    <a href="#optionconfigurations" title="OptionConfigurations">OptionConfigurations</a>: <i>
      - <a href="optionconfiguration.md">OptionConfiguration</a></i>
    <a href="#tags" title="Tags">Tags</a>: <i>
      - <a href="tag.md">Tag</a></i>
</pre>

## Properties

#### OptionGroupDescription

Provides a description of the option group.

_Required_: Yes

_Type_: String

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### EngineName

Indicates the name of the engine that this option group can be applied to.

_Required_: Yes

_Type_: String

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### MajorEngineVersion

Indicates the major engine version associated with this option group.

_Required_: Yes

_Type_: String

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### OptionConfigurations

Indicates what options are available in the option group.

_Required_: No

_Type_: List of <a href="optionconfiguration.md">OptionConfiguration</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### Tags

An array of key-value pairs to apply to this resource.

_Required_: No

_Type_: List of <a href="tag.md">Tag</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

## Return Values

### Ref

When you pass the logical ID of this resource to the intrinsic `Ref` function, Ref returns the OptionGroupName.

### Fn::GetAtt

The `Fn::GetAtt` intrinsic function returns a value for a specified attribute of this type. The following are the available attributes and sample return values.

For more information about using the `Fn::GetAtt` intrinsic function, see [Fn::GetAtt](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/intrinsic-function-reference-getatt.html).

#### OptionGroupName

Specifies the name of the option group.
