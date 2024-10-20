# AWS::RDS::DBShardGroup

The AWS::RDS::DBShardGroup resource creates an Amazon Aurora Limitless DB Shard Group.

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "Type" : "AWS::RDS::DBShardGroup",
    "Properties" : {
        "<a href="#dbshardgroupidentifier" title="DBShardGroupIdentifier">DBShardGroupIdentifier</a>" : <i>String</i>,
        "<a href="#dbclusteridentifier" title="DBClusterIdentifier">DBClusterIdentifier</a>" : <i>String</i>,
        "<a href="#computeredundancy" title="ComputeRedundancy">ComputeRedundancy</a>" : <i>Integer</i>,
        "<a href="#maxacu" title="MaxACU">MaxACU</a>" : <i>Double</i>,
        "<a href="#minacu" title="MinACU">MinACU</a>" : <i>Double</i>,
        "<a href="#publiclyaccessible" title="PubliclyAccessible">PubliclyAccessible</a>" : <i>Boolean</i>,
        "<a href="#tags" title="Tags">Tags</a>" : <i>[ <a href="tag.md">Tag</a>, ... ]</i>
    }
}
</pre>

### YAML

<pre>
Type: AWS::RDS::DBShardGroup
Properties:
    <a href="#dbshardgroupidentifier" title="DBShardGroupIdentifier">DBShardGroupIdentifier</a>: <i>String</i>
    <a href="#dbclusteridentifier" title="DBClusterIdentifier">DBClusterIdentifier</a>: <i>String</i>
    <a href="#computeredundancy" title="ComputeRedundancy">ComputeRedundancy</a>: <i>Integer</i>
    <a href="#maxacu" title="MaxACU">MaxACU</a>: <i>Double</i>
    <a href="#minacu" title="MinACU">MinACU</a>: <i>Double</i>
    <a href="#publiclyaccessible" title="PubliclyAccessible">PubliclyAccessible</a>: <i>Boolean</i>
    <a href="#tags" title="Tags">Tags</a>: <i>
      - <a href="tag.md">Tag</a></i>
</pre>

## Properties

#### DBShardGroupIdentifier

The name of the DB shard group.

_Required_: No

_Type_: String

_Minimum Length_: <code>1</code>

_Maximum Length_: <code>63</code>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### DBClusterIdentifier

The name of the primary DB cluster for the DB shard group.

_Required_: Yes

_Type_: String

_Minimum Length_: <code>1</code>

_Maximum Length_: <code>63</code>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### ComputeRedundancy

Specifies whether to create standby instances for the DB shard group.

_Required_: No

_Type_: Integer

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### MaxACU

The maximum capacity of the DB shard group in Aurora capacity units (ACUs).

_Required_: Yes

_Type_: Double

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### MinACU

The minimum capacity of the DB shard group in Aurora capacity units (ACUs).

_Required_: No

_Type_: Double

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### PubliclyAccessible

Indicates whether the DB shard group is publicly accessible.

_Required_: No

_Type_: Boolean

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### Tags

An array of key-value pairs to apply to this resource.

_Required_: No

_Type_: List of <a href="tag.md">Tag</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

## Return Values

### Ref

When you pass the logical ID of this resource to the intrinsic `Ref` function, Ref returns the DBShardGroupIdentifier.

### Fn::GetAtt

The `Fn::GetAtt` intrinsic function returns a value for a specified attribute of this type. The following are the available attributes and sample return values.

For more information about using the `Fn::GetAtt` intrinsic function, see [Fn::GetAtt](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/intrinsic-function-reference-getatt.html).

#### DBShardGroupResourceId

The Amazon Web Services Region-unique, immutable identifier for the DB shard group.

#### Endpoint

The connection endpoint for the DB shard group.
