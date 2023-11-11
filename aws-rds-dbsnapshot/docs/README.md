# AWS::RDS::DBSnapshot

The AWS::RDS::DBSnapshot resource creates an Amazon RDS DB instance snapshot.

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "Type" : "AWS::RDS::DBSnapshot",
    "Properties" : {
        "<a href="#copyoptiongroup" title="CopyOptionGroup">CopyOptionGroup</a>" : <i>Boolean</i>,
        "<a href="#copytags" title="CopyTags">CopyTags</a>" : <i>Boolean</i>,
        "<a href="#dbinstanceidentifier" title="DBInstanceIdentifier">DBInstanceIdentifier</a>" : <i>String</i>,
        "<a href="#dbsnapshotidentifier" title="DBSnapshotIdentifier">DBSnapshotIdentifier</a>" : <i>String</i>,
        "<a href="#engineversion" title="EngineVersion">EngineVersion</a>" : <i>String</i>,
        "<a href="#kmskeyid" title="KmsKeyId">KmsKeyId</a>" : <i>String</i>,
        "<a href="#optiongroupname" title="OptionGroupName">OptionGroupName</a>" : <i>String</i>,
        "<a href="#sourcedbsnapshotidentifier" title="SourceDBSnapshotIdentifier">SourceDBSnapshotIdentifier</a>" : <i>String</i>,
        "<a href="#sourceregion" title="SourceRegion">SourceRegion</a>" : <i>String</i>,
        "<a href="#tags" title="Tags">Tags</a>" : <i>[ <a href="tag.md">Tag</a>, ... ]</i>,
    }
}
</pre>

### YAML

<pre>
Type: AWS::RDS::DBSnapshot
Properties:
    <a href="#copyoptiongroup" title="CopyOptionGroup">CopyOptionGroup</a>: <i>Boolean</i>
    <a href="#copytags" title="CopyTags">CopyTags</a>: <i>Boolean</i>
    <a href="#dbinstanceidentifier" title="DBInstanceIdentifier">DBInstanceIdentifier</a>: <i>String</i>
    <a href="#dbsnapshotidentifier" title="DBSnapshotIdentifier">DBSnapshotIdentifier</a>: <i>String</i>
    <a href="#engineversion" title="EngineVersion">EngineVersion</a>: <i>String</i>
    <a href="#kmskeyid" title="KmsKeyId">KmsKeyId</a>: <i>String</i>
    <a href="#optiongroupname" title="OptionGroupName">OptionGroupName</a>: <i>String</i>
    <a href="#sourcedbsnapshotidentifier" title="SourceDBSnapshotIdentifier">SourceDBSnapshotIdentifier</a>: <i>String</i>
    <a href="#sourceregion" title="SourceRegion">SourceRegion</a>: <i>String</i>
    <a href="#tags" title="Tags">Tags</a>: <i>
      - <a href="tag.md">Tag</a></i>
</pre>

## Properties

#### CopyOptionGroup

A value that indicates whether to copy the DB option group associated with the source DB snapshot to the target Amazon Web Services account and associate with the target DB snapshot. The associated option group can be copied only with cross-account snapshot copy calls.

_Required_: No

_Type_: Boolean

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### CopyTags

A value that indicates whether to copy all tags from the source DB snapshot to the target DB snapshot

_Required_: No

_Type_: Boolean

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### DBInstanceIdentifier

Specifies the DB instance identifier of the DB instance this DB snapshot was created from.

_Required_: No

_Type_: String

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### DBSnapshotIdentifier

Specifies the identifier for the DB snapshot.

_Required_: No

_Type_: String

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### EngineVersion

Specifies the version of the database engine.

_Required_: No

_Type_: String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### KmsKeyId

If Encrypted is true, the AWS KMS key identifier for the encrypted DB snapshot.

_Required_: No

_Type_: String

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### OptionGroupName

Provides the option group name for the DB snapshot.

_Required_: No

_Type_: String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### SourceDBSnapshotIdentifier

The DB snapshot Amazon Resource Name (ARN) that the DB snapshot was copied from. It only has a value in the case of a cross-account or cross-Region copy.

_Required_: No

_Type_: String

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### SourceRegion

The AWS Region that the DB snapshot was created in or copied from.

_Required_: No

_Type_: String

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### Tags

A list of tags.

_Required_: No

_Type_: List of <a href="tag.md">Tag</a>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

## Return Values

### Ref

When you pass the logical ID of this resource to the intrinsic `Ref` function, Ref returns the DBSnapshotIdentifier.

### Fn::GetAtt

The `Fn::GetAtt` intrinsic function returns a value for a specified attribute of this type. The following are the available attributes and sample return values.

For more information about using the `Fn::GetAtt` intrinsic function, see [Fn::GetAtt](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/intrinsic-function-reference-getatt.html).

#### AllocatedStorage

Specifies the allocated storage size in gibibytes (GiB).

#### AvailabilityZone

Specifies the name of the Availability Zone the DB instance was located in at the time of the DB snapshot.

#### DBSnapshotArn

The Amazon Resource Name (ARN) for the DB snapshot.

#### DbiResourceId

The identifier for the source DB instance, which can't be changed and which is unique to an AWS Region.

#### Encrypted

Specifies whether the DB snapshot is encrypted.

#### Engine

Specifies the name of the database engine.

#### IAMDatabaseAuthenticationEnabled

True if mapping of AWS Identity and Access Management (IAM) accounts to database accounts is enabled, and otherwise false.

#### InstanceCreateTime

Specifies the time in Coordinated Universal Time (UTC) when the DB instance, from which the snapshot was taken, was created.

#### Iops

Specifies the Provisioned IOPS (I/O operations per second) value of the DB instance at the time of the snapshot.

#### LicenseModel

License model information for the restored DB instance.

#### MasterUsername

Provides the master username for the DB snapshot.

#### OriginalSnapshotCreateTime

Specifies the time of the CreateDBSnapshot operation in Coordinated Universal Time (UTC). Doesn't change when the snapshot is copied.

#### PercentProgress

The percentage of the estimated data that has been transferred.

#### Port

Specifies the port that the database engine was listening on at the time of the snapshot.

#### ProcessorFeatures

The number of CPU cores and the number of threads per core for the DB instance class of the DB instance when the DB snapshot was created.

#### SnapshotCreateTime

Specifies when the snapshot was taken in Coordinated Universal Time (UTC). Changes for the copy when the snapshot is copied.

#### SnapshotDatabaseTime

The timestamp of the most recent transaction applied to the database that you're backing up. Thus, if you restore a snapshot, SnapshotDatabaseTime is the most recent transaction in the restored DB instance. In contrast, originalSnapshotCreateTime specifies the system time that the snapshot completed.

#### SnapshotTarget

Specifies where manual snapshots are stored: AWS Outposts or the AWS Region.

#### SnapshotType

Provides the type of the DB snapshot.

#### Status

Specifies the status of this DB snapshot.

#### StorageThroughput

Specifies the storage throughput for the DB snapshot.

#### StorageType

Specifies the storage type associated with DB snapshot.

#### TargetCustomAvailabilityZone

The external custom Availability Zone (CAZ) identifier for the target CAZ.

#### TdeCredentialArn

The ARN from the key store with which to associate the instance for TDE encryption.

#### Timezone

The time zone of the DB snapshot. In most cases, the Timezone element is empty. Timezone content appears only for snapshots taken from Microsoft SQL Server DB instances that were created with a time zone specified.

#### VpcId

Provides the VPC ID associated with the DB snapshot.
