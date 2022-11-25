# AWS::RDS::GlobalCluster

Resource Type definition for AWS::RDS::GlobalCluster

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "Type" : "AWS::RDS::GlobalCluster",
    "Properties" : {
        "<a href="#engine" title="Engine">Engine</a>" : <i>String</i>,
        "<a href="#engineversion" title="EngineVersion">EngineVersion</a>" : <i>String</i>,
        "<a href="#deletionprotection" title="DeletionProtection">DeletionProtection</a>" : <i>Boolean</i>,
        "<a href="#globalclusteridentifier" title="GlobalClusterIdentifier">GlobalClusterIdentifier</a>" : <i>String</i>,
        "<a href="#sourcedbclusteridentifier" title="SourceDBClusterIdentifier">SourceDBClusterIdentifier</a>" : <i>String</i>,
        "<a href="#storageencrypted" title="StorageEncrypted">StorageEncrypted</a>" : <i>Boolean</i>
    }
}
</pre>

### YAML

<pre>
Type: AWS::RDS::GlobalCluster
Properties:
    <a href="#engine" title="Engine">Engine</a>: <i>String</i>
    <a href="#engineversion" title="EngineVersion">EngineVersion</a>: <i>String</i>
    <a href="#deletionprotection" title="DeletionProtection">DeletionProtection</a>: <i>Boolean</i>
    <a href="#globalclusteridentifier" title="GlobalClusterIdentifier">GlobalClusterIdentifier</a>: <i>String</i>
    <a href="#sourcedbclusteridentifier" title="SourceDBClusterIdentifier">SourceDBClusterIdentifier</a>: <i>String</i>
    <a href="#storageencrypted" title="StorageEncrypted">StorageEncrypted</a>: <i>Boolean</i>
</pre>

## Properties

#### Engine

The name of the database engine to be used for this DB cluster. Valid Values: aurora (for MySQL 5.6-compatible Aurora), aurora-mysql (for MySQL 5.7-compatible Aurora).
If you specify the SourceDBClusterIdentifier property, don't specify this property. The value is inherited from the cluster.

_Required_: No

_Type_: String

_Allowed Values_: <code>aurora</code> | <code>aurora-mysql</code> | <code>aurora-postgresql</code>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### EngineVersion

The version number of the database engine to use. If you specify the SourceDBClusterIdentifier property, don't specify this property. The value is inherited from the cluster.

_Required_: No

_Type_: String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### DeletionProtection

The deletion protection setting for the new global database. The global database can't be deleted when deletion protection is enabled.

_Required_: No

_Type_: Boolean

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### GlobalClusterIdentifier

The cluster identifier of the new global database cluster. This parameter is stored as a lowercase string.

_Required_: No

_Type_: String

_Pattern_: <code>^[a-zA-Z]{1}(?:-?[a-zA-Z0-9]){0,62}$</code>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### SourceDBClusterIdentifier

The Amazon Resource Name (ARN) to use as the primary cluster of the global database. This parameter is optional. This parameter is stored as a lowercase string.

_Required_: No

_Type_: String

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### StorageEncrypted

 The storage encryption setting for the new global database cluster.
If you specify the SourceDBClusterIdentifier property, don't specify this property. The value is inherited from the cluster.

_Required_: No

_Type_: Boolean

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

## Return Values

### Ref

When you pass the logical ID of this resource to the intrinsic `Ref` function, Ref returns the GlobalClusterIdentifier.
