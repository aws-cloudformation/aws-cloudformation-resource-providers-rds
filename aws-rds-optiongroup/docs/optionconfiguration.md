# AWS::RDS::OptionGroup OptionConfiguration

The OptionConfiguration property type specifies an individual option, and its settings, within an AWS::RDS::OptionGroup resource.

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "<a href="#dbsecuritygroupmemberships" title="DBSecurityGroupMemberships">DBSecurityGroupMemberships</a>" : <i>[ String, ... ]</i>,
    "<a href="#optionname" title="OptionName">OptionName</a>" : <i>String</i>,
    "<a href="#optionsettings" title="OptionSettings">OptionSettings</a>" : <i>[ <a href="optionsetting.md">OptionSetting</a>, ... ]</i>,
    "<a href="#optionversion" title="OptionVersion">OptionVersion</a>" : <i>String</i>,
    "<a href="#port" title="Port">Port</a>" : <i>Integer</i>,
    "<a href="#vpcsecuritygroupmemberships" title="VpcSecurityGroupMemberships">VpcSecurityGroupMemberships</a>" : <i>[ String, ... ]</i>
}
</pre>

### YAML

<pre>
<a href="#dbsecuritygroupmemberships" title="DBSecurityGroupMemberships">DBSecurityGroupMemberships</a>: <i>
      - String</i>
<a href="#optionname" title="OptionName">OptionName</a>: <i>String</i>
<a href="#optionsettings" title="OptionSettings">OptionSettings</a>: <i>
      - <a href="optionsetting.md">OptionSetting</a></i>
<a href="#optionversion" title="OptionVersion">OptionVersion</a>: <i>String</i>
<a href="#port" title="Port">Port</a>: <i>Integer</i>
<a href="#vpcsecuritygroupmemberships" title="VpcSecurityGroupMemberships">VpcSecurityGroupMemberships</a>: <i>
      - String</i>
</pre>

## Properties

#### DBSecurityGroupMemberships

A list of DBSecurityGroupMembership name strings used for this option.

_Required_: No

_Type_: List of String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### OptionName

The configuration of options to include in a group.

_Required_: Yes

_Type_: String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### OptionSettings

The option settings to include in an option group.

_Required_: No

_Type_: List of <a href="optionsetting.md">OptionSetting</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### OptionVersion

The version for the option.

_Required_: No

_Type_: String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### Port

The optional port for the option.

_Required_: No

_Type_: Integer

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### VpcSecurityGroupMemberships

A list of VpcSecurityGroupMembership name strings used for this option.

_Required_: No

_Type_: List of String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)
