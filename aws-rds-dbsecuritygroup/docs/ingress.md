# AWS::RDS::DBSecurityGroup Ingress

The Ingress property type specifies an individual ingress rule within an AWS::RDS::DBSecurityGroup resource.

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "<a href="#cidrip" title="CIDRIP">CIDRIP</a>" : <i>String</i>,
    "<a href="#ec2securitygroupid" title="EC2SecurityGroupId">EC2SecurityGroupId</a>" : <i>String</i>,
    "<a href="#ec2securitygroupname" title="EC2SecurityGroupName">EC2SecurityGroupName</a>" : <i>String</i>,
    "<a href="#ec2securitygroupownerid" title="EC2SecurityGroupOwnerId">EC2SecurityGroupOwnerId</a>" : <i>String</i>
}
</pre>

### YAML

<pre>
<a href="#cidrip" title="CIDRIP">CIDRIP</a>: <i>String</i>
<a href="#ec2securitygroupid" title="EC2SecurityGroupId">EC2SecurityGroupId</a>: <i>String</i>
<a href="#ec2securitygroupname" title="EC2SecurityGroupName">EC2SecurityGroupName</a>: <i>String</i>
<a href="#ec2securitygroupownerid" title="EC2SecurityGroupOwnerId">EC2SecurityGroupOwnerId</a>: <i>String</i>
</pre>

## Properties

#### CIDRIP

The IP range to authorize.

_Required_: No

_Type_: String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### EC2SecurityGroupId

Id of the EC2 Security Group to authorize. For VPC DB Security Groups, EC2SecurityGroupId must be provided. Otherwise, EC2SecurityGroupOwnerId and either EC2SecurityGroupName or EC2SecurityGroupId must be provided.

_Required_: Yes

_Type_: String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### EC2SecurityGroupName

Name of the EC2 Security Group to authorize. For VPC DB Security Groups, EC2SecurityGroupId must be provided. Otherwise, EC2SecurityGroupOwnerId and either EC2SecurityGroupName or EC2SecurityGroupId must be provided.

_Required_: Yes

_Type_: String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### EC2SecurityGroupOwnerId

AWS Account Number of the owner of the EC2 Security Group specified in the EC2SecurityGroupName parameter. The AWS Access Key ID is not an acceptable value. For VPC DB Security Groups, EC2SecurityGroupId must be provided. Otherwise, EC2SecurityGroupOwnerId and either EC2SecurityGroupName or EC2SecurityGroupId must be provided.

_Required_: Yes

_Type_: String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)
