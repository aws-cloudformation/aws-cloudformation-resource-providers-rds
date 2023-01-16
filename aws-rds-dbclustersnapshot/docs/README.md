# AWS::RDS::DBClusterSnapshot

The AWS::RDS::DBClusterSnapshot resource creates a new Amazon RDS DB cluster snapshot. For more information, see Managing an Amazon Aurora DB Cluster Snapshot in the Amazon Aurora User Guide.

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "Type" : "AWS::RDS::DBClusterSnapshot",
    "Properties" : {
        "<a href="#dbclustersnapshotidentifier" title="DBClusterSnapshotIdentifier">DBClusterSnapshotIdentifier</a>" : <i>String</i>,
        "<a href="#dbclusteridentifier" title="DBClusterIdentifier">DBClusterIdentifier</a>" : <i>String</i>,
        "<a href="#tags" title="Tags">Tags</a>" : <i>[ <a href="tag.md">Tag</a>, ... ]</i>
    }
}
</pre>

### YAML

<pre>
Type: AWS::RDS::DBClusterSnapshot
Properties:
    <a href="#dbclustersnapshotidentifier" title="DBClusterSnapshotIdentifier">DBClusterSnapshotIdentifier</a>: <i>String</i>
    <a href="#dbclusteridentifier" title="DBClusterIdentifier">DBClusterIdentifier</a>: <i>String</i>
    <a href="#tags" title="Tags">Tags</a>: <i>
      - <a href="tag.md">Tag</a></i>
</pre>

## Properties

#### DBClusterSnapshotIdentifier

The identifier for the DB snapshot. This parameter is stored as a lowercase string.; FIXME: Look at pattern!

_Required_: No

_Type_: String

_Pattern_: <code>^$|^[a-z]{1}(?:[:-]?[a-z0-9]){0,254}$</code>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### DBClusterIdentifier

A name for the DB instance.; FIXME: Look at pattern!

_Required_: Yes

_Type_: String

_Pattern_: <code>^$|^[a-zA-Z]{1}(?:-?[a-zA-Z0-9]){0,62}$</code>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### Tags

An array of key-value pairs to apply to this resource.; FIXME: Probably look here

_Required_: No

_Type_: List of <a href="tag.md">Tag</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

## Return Values

### Ref

When you pass the logical ID of this resource to the intrinsic `Ref` function, Ref returns the DBClusterSnapshotIdentifier.

### Fn::GetAtt

The `Fn::GetAtt` intrinsic function returns a value for a specified attribute of this type. The following are the available attributes and sample return values.

For more information about using the `Fn::GetAtt` intrinsic function, see [Fn::GetAtt](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/intrinsic-function-reference-getatt.html).

#### DBClusterSnapshotArn

The Amazon Resource Name (ARN) for the snapshot.
