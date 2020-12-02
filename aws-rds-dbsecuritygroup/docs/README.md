# AWS::RDS::DBSecurityGroup

The AWS::RDS::DBSecurityGroup resource creates or updates an Amazon RDS DB security group.

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "Type" : "AWS::RDS::DBSecurityGroup",
    "Properties" : {
        "<a href="#groupname" title="GroupName">GroupName</a>" : <i>String</i>,
        "<a href="#groupdescription" title="GroupDescription">GroupDescription</a>" : <i>String</i>,
        "<a href="#ec2vpcid" title="EC2VpcId">EC2VpcId</a>" : <i>String</i>,
        "<a href="#dbsecuritygroupingress" title="DBSecurityGroupIngress">DBSecurityGroupIngress</a>" : <i>[ <a href="ingress.md">Ingress</a>, ... ]</i>,
        "<a href="#tags" title="Tags">Tags</a>" : <i>[ <a href="tag.md">Tag</a>, ... ]</i>
    }
}
</pre>

### YAML

<pre>
Type: AWS::RDS::DBSecurityGroup
Properties:
    <a href="#groupname" title="GroupName">GroupName</a>: <i>String</i>
    <a href="#groupdescription" title="GroupDescription">GroupDescription</a>: <i>String</i>
    <a href="#ec2vpcid" title="EC2VpcId">EC2VpcId</a>: <i>String</i>
    <a href="#dbsecuritygroupingress" title="DBSecurityGroupIngress">DBSecurityGroupIngress</a>: <i>
      - <a href="ingress.md">Ingress</a></i>
    <a href="#tags" title="Tags">Tags</a>: <i>
      - <a href="tag.md">Tag</a></i>
</pre>

## Properties

#### GroupName

Specifies the name of the DB security group.

_Required_: No

_Type_: String

_Pattern_: <code>^[a-zA-Z]{1}(?:-?[a-zA-Z0-9]){0,254}$</code>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### GroupDescription

Provides the description of the DB Security Group.

_Required_: Yes

_Type_: String

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### EC2VpcId

The identifier of an Amazon VPC. This property indicates the VPC that this DB security group belongs to.

_Required_: No

_Type_: String

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### DBSecurityGroupIngress

Ingress rules to be applied to the DB security group.

_Required_: Yes

_Type_: List of <a href="ingress.md">Ingress</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### Tags

Tags to assign to the DB security group.

_Required_: No

_Type_: List of <a href="tag.md">Tag</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

## Return Values

### Ref

When you pass the logical ID of this resource to the intrinsic `Ref` function, Ref returns the GroupName.
