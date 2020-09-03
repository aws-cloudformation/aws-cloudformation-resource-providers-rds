# AWS::RDS::OptionGroup OptionConfiguration

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "<a href="#dbsecuritygroupmemberships" title="DBSecurityGroupMemberships">DBSecurityGroupMemberships</a>" : <i>[ <a href="dbsecuritygroupmembership.md">DBSecurityGroupMembership</a>, ... ]</i>,
    "<a href="#optionname" title="OptionName">OptionName</a>" : <i>String</i>,
    "<a href="#optionsettings" title="OptionSettings">OptionSettings</a>" : <i>[ <a href="optionsetting.md">OptionSetting</a>, ... ]</i>,
    "<a href="#optionversion" title="OptionVersion">OptionVersion</a>" : <i>String</i>,
    "<a href="#port" title="Port">Port</a>" : <i>Integer</i>,
    "<a href="#vpcsecuritygroupmemberships" title="VpcSecurityGroupMemberships">VpcSecurityGroupMemberships</a>" : <i>[ <a href="vpcsecuritygroupmembership.md">VpcSecurityGroupMembership</a>, ... ]</i>
}
</pre>

### YAML

<pre>
<a href="#dbsecuritygroupmemberships" title="DBSecurityGroupMemberships">DBSecurityGroupMemberships</a>: <i>
      - <a href="dbsecuritygroupmembership.md">DBSecurityGroupMembership</a></i>
<a href="#optionname" title="OptionName">OptionName</a>: <i>String</i>
<a href="#optionsettings" title="OptionSettings">OptionSettings</a>: <i>
      - <a href="optionsetting.md">OptionSetting</a></i>
<a href="#optionversion" title="OptionVersion">OptionVersion</a>: <i>String</i>
<a href="#port" title="Port">Port</a>: <i>Integer</i>
<a href="#vpcsecuritygroupmemberships" title="VpcSecurityGroupMemberships">VpcSecurityGroupMemberships</a>: <i>
      - <a href="vpcsecuritygroupmembership.md">VpcSecurityGroupMembership</a></i>
</pre>

## Properties

#### DBSecurityGroupMemberships

_Required_: No

_Type_: List of <a href="dbsecuritygroupmembership.md">DBSecurityGroupMembership</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### OptionName

_Required_: Yes

_Type_: String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### OptionSettings

_Required_: No

_Type_: List of <a href="optionsetting.md">OptionSetting</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### OptionVersion

_Required_: No

_Type_: String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### Port

_Required_: No

_Type_: Integer

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### VpcSecurityGroupMemberships

_Required_: No

_Type_: List of <a href="vpcsecuritygroupmembership.md">VpcSecurityGroupMembership</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

