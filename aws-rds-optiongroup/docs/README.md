# AWS::RDS::OptionGroup

An example resource schema demonstrating some basic constructs and validation rules.

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "Type" : "AWS::RDS::OptionGroup",
    "Properties" : {
        "<a href="#id" title="Id">Id</a>" : <i>String</i>,
        "<a href="#enginename" title="EngineName">EngineName</a>" : <i>String</i>,
        "<a href="#majorengineversion" title="MajorEngineVersion">MajorEngineVersion</a>" : <i>String</i>,
        "<a href="#optionconfigurations" title="OptionConfigurations">OptionConfigurations</a>" : <i>[ &lt;a href=&#34;optionconfigurations.md&#34;&gt;OptionConfigurations&lt;/a&gt;, ... ]</i>,
        "<a href="#optiongroupdescription" title="OptionGroupDescription">OptionGroupDescription</a>" : <i>String</i>,
        "<a href="#tags" title="Tags">Tags</a>" : <i>[ &lt;a href=&#34;tags.md&#34;&gt;Tags&lt;/a&gt;, ... ]</i>
    }
}
</pre>

### YAML

<pre>
Type: AWS::RDS::OptionGroup
Properties:
    <a href="#id" title="Id">Id</a>: <i>String</i>
    <a href="#enginename" title="EngineName">EngineName</a>: <i>String</i>
    <a href="#majorengineversion" title="MajorEngineVersion">MajorEngineVersion</a>: <i>String</i>
    <a href="#optionconfigurations" title="OptionConfigurations">OptionConfigurations</a>: <i>
      - &lt;a href=&#34;optionconfigurations.md&#34;&gt;OptionConfigurations&lt;/a&gt;</i>
    <a href="#optiongroupdescription" title="OptionGroupDescription">OptionGroupDescription</a>: <i>String</i>
    <a href="#tags" title="Tags">Tags</a>: <i>
      - &lt;a href=&#34;tags.md&#34;&gt;Tags&lt;/a&gt;</i>
</pre>

## Properties

#### Id

_Required_: No

_Type_: String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### EngineName

Specifies the name of the engine that this option group should be associated with.

_Required_: Yes

_Type_: String

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### MajorEngineVersion

Specifies the major version of the engine that this option group should be associated with.

_Required_: Yes

_Type_: String

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### OptionConfigurations

A list of all available options

_Required_: Yes

_Type_: List of &lt;a href=&#34;optionconfigurations.md&#34;&gt;OptionConfigurations&lt;/a&gt;

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### OptionGroupDescription

The description of the option group.

_Required_: Yes

_Type_: String

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### Tags

An array of key-value pairs to apply to this resource.

_Required_: No

_Type_: List of &lt;a href=&#34;tags.md&#34;&gt;Tags&lt;/a&gt;

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

## Return Values

### Ref

When you pass the logical ID of this resource to the intrinsic `Ref` function, Ref returns the Id.
