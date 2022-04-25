# AWS::RDS::DBCluster

The AWS::RDS::DBCluster resource creates an Amazon Aurora DB cluster.

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "Type" : "AWS::RDS::DBCluster",
    "Properties" : {
        "<a href="#readendpoint" title="ReadEndpoint">ReadEndpoint</a>" : <i><a href="readendpoint.md">ReadEndpoint</a></i>,
        "<a href="#associatedroles" title="AssociatedRoles">AssociatedRoles</a>" : <i>[ <a href="dbclusterrole.md">DBClusterRole</a>, ... ]</i>,
        "<a href="#availabilityzones" title="AvailabilityZones">AvailabilityZones</a>" : <i>[ String, ... ]</i>,
        "<a href="#backtrackwindow" title="BacktrackWindow">BacktrackWindow</a>" : <i>Integer</i>,
        "<a href="#backupretentionperiod" title="BackupRetentionPeriod">BackupRetentionPeriod</a>" : <i>Integer</i>,
        "<a href="#copytagstosnapshot" title="CopyTagsToSnapshot">CopyTagsToSnapshot</a>" : <i>Boolean</i>,
        "<a href="#databasename" title="DatabaseName">DatabaseName</a>" : <i>String</i>,
        "<a href="#globalclusteridentifier" title="GlobalClusterIdentifier">GlobalClusterIdentifier</a>" : <i>String</i>,
        "<a href="#dbclusteridentifier" title="DBClusterIdentifier">DBClusterIdentifier</a>" : <i>String</i>,
        "<a href="#dbclusterparametergroupname" title="DBClusterParameterGroupName">DBClusterParameterGroupName</a>" : <i>String</i>,
        "<a href="#dbsubnetgroupname" title="DBSubnetGroupName">DBSubnetGroupName</a>" : <i>String</i>,
        "<a href="#deletionprotection" title="DeletionProtection">DeletionProtection</a>" : <i>Boolean</i>,
        "<a href="#enablecloudwatchlogsexports" title="EnableCloudwatchLogsExports">EnableCloudwatchLogsExports</a>" : <i>[ String, ... ]</i>,
        "<a href="#enablehttpendpoint" title="EnableHttpEndpoint">EnableHttpEndpoint</a>" : <i>Boolean</i>,
        "<a href="#enableiamdatabaseauthentication" title="EnableIAMDatabaseAuthentication">EnableIAMDatabaseAuthentication</a>" : <i>Boolean</i>,
        "<a href="#engine" title="Engine">Engine</a>" : <i>String</i>,
        "<a href="#enginemode" title="EngineMode">EngineMode</a>" : <i>String</i>,
        "<a href="#engineversion" title="EngineVersion">EngineVersion</a>" : <i>String</i>,
        "<a href="#kmskeyid" title="KmsKeyId">KmsKeyId</a>" : <i>String</i>,
        "<a href="#masterusername" title="MasterUsername">MasterUsername</a>" : <i>String</i>,
        "<a href="#masteruserpassword" title="MasterUserPassword">MasterUserPassword</a>" : <i>String</i>,
        "<a href="#port" title="Port">Port</a>" : <i>Integer</i>,
        "<a href="#preferredbackupwindow" title="PreferredBackupWindow">PreferredBackupWindow</a>" : <i>String</i>,
        "<a href="#preferredmaintenancewindow" title="PreferredMaintenanceWindow">PreferredMaintenanceWindow</a>" : <i>String</i>,
        "<a href="#replicationsourceidentifier" title="ReplicationSourceIdentifier">ReplicationSourceIdentifier</a>" : <i>String</i>,
        "<a href="#restoretype" title="RestoreType">RestoreType</a>" : <i>String</i>,
        "<a href="#scalingconfiguration" title="ScalingConfiguration">ScalingConfiguration</a>" : <i><a href="scalingconfiguration.md">ScalingConfiguration</a></i>,
        "<a href="#snapshotidentifier" title="SnapshotIdentifier">SnapshotIdentifier</a>" : <i>String</i>,
        "<a href="#sourcedbclusteridentifier" title="SourceDBClusterIdentifier">SourceDBClusterIdentifier</a>" : <i>String</i>,
        "<a href="#sourceregion" title="SourceRegion">SourceRegion</a>" : <i>String</i>,
        "<a href="#storageencrypted" title="StorageEncrypted">StorageEncrypted</a>" : <i>Boolean</i>,
        "<a href="#tags" title="Tags">Tags</a>" : <i>[ <a href="tag.md">Tag</a>, ... ]</i>,
        "<a href="#uselatestrestorabletime" title="UseLatestRestorableTime">UseLatestRestorableTime</a>" : <i>Boolean</i>,
        "<a href="#vpcsecuritygroupids" title="VpcSecurityGroupIds">VpcSecurityGroupIds</a>" : <i>[ String, ... ]</i>
    }
}
</pre>

### YAML

<pre>
Type: AWS::RDS::DBCluster
Properties:
    <a href="#readendpoint" title="ReadEndpoint">ReadEndpoint</a>: <i><a href="readendpoint.md">ReadEndpoint</a></i>
    <a href="#associatedroles" title="AssociatedRoles">AssociatedRoles</a>: <i>
      - <a href="dbclusterrole.md">DBClusterRole</a></i>
    <a href="#availabilityzones" title="AvailabilityZones">AvailabilityZones</a>: <i>
      - String</i>
    <a href="#backtrackwindow" title="BacktrackWindow">BacktrackWindow</a>: <i>Integer</i>
    <a href="#backupretentionperiod" title="BackupRetentionPeriod">BackupRetentionPeriod</a>: <i>Integer</i>
    <a href="#copytagstosnapshot" title="CopyTagsToSnapshot">CopyTagsToSnapshot</a>: <i>Boolean</i>
    <a href="#databasename" title="DatabaseName">DatabaseName</a>: <i>String</i>
    <a href="#globalclusteridentifier" title="GlobalClusterIdentifier">GlobalClusterIdentifier</a>: <i>String</i>
    <a href="#dbclusteridentifier" title="DBClusterIdentifier">DBClusterIdentifier</a>: <i>String</i>
    <a href="#dbclusterparametergroupname" title="DBClusterParameterGroupName">DBClusterParameterGroupName</a>: <i>String</i>
    <a href="#dbsubnetgroupname" title="DBSubnetGroupName">DBSubnetGroupName</a>: <i>String</i>
    <a href="#deletionprotection" title="DeletionProtection">DeletionProtection</a>: <i>Boolean</i>
    <a href="#enablecloudwatchlogsexports" title="EnableCloudwatchLogsExports">EnableCloudwatchLogsExports</a>: <i>
      - String</i>
    <a href="#enablehttpendpoint" title="EnableHttpEndpoint">EnableHttpEndpoint</a>: <i>Boolean</i>
    <a href="#enableiamdatabaseauthentication" title="EnableIAMDatabaseAuthentication">EnableIAMDatabaseAuthentication</a>: <i>Boolean</i>
    <a href="#engine" title="Engine">Engine</a>: <i>String</i>
    <a href="#enginemode" title="EngineMode">EngineMode</a>: <i>String</i>
    <a href="#engineversion" title="EngineVersion">EngineVersion</a>: <i>String</i>
    <a href="#kmskeyid" title="KmsKeyId">KmsKeyId</a>: <i>String</i>
    <a href="#masterusername" title="MasterUsername">MasterUsername</a>: <i>String</i>
    <a href="#masteruserpassword" title="MasterUserPassword">MasterUserPassword</a>: <i>String</i>
    <a href="#port" title="Port">Port</a>: <i>Integer</i>
    <a href="#preferredbackupwindow" title="PreferredBackupWindow">PreferredBackupWindow</a>: <i>String</i>
    <a href="#preferredmaintenancewindow" title="PreferredMaintenanceWindow">PreferredMaintenanceWindow</a>: <i>String</i>
    <a href="#replicationsourceidentifier" title="ReplicationSourceIdentifier">ReplicationSourceIdentifier</a>: <i>String</i>
    <a href="#restoretype" title="RestoreType">RestoreType</a>: <i>String</i>
    <a href="#scalingconfiguration" title="ScalingConfiguration">ScalingConfiguration</a>: <i><a href="scalingconfiguration.md">ScalingConfiguration</a></i>
    <a href="#snapshotidentifier" title="SnapshotIdentifier">SnapshotIdentifier</a>: <i>String</i>
    <a href="#sourcedbclusteridentifier" title="SourceDBClusterIdentifier">SourceDBClusterIdentifier</a>: <i>String</i>
    <a href="#sourceregion" title="SourceRegion">SourceRegion</a>: <i>String</i>
    <a href="#storageencrypted" title="StorageEncrypted">StorageEncrypted</a>: <i>Boolean</i>
    <a href="#tags" title="Tags">Tags</a>: <i>
      - <a href="tag.md">Tag</a></i>
    <a href="#uselatestrestorabletime" title="UseLatestRestorableTime">UseLatestRestorableTime</a>: <i>Boolean</i>
    <a href="#vpcsecuritygroupids" title="VpcSecurityGroupIds">VpcSecurityGroupIds</a>: <i>
      - String</i>
</pre>

## Properties

#### ReadEndpoint

_Required_: No

_Type_: <a href="readendpoint.md">ReadEndpoint</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### AssociatedRoles

Provides a list of the AWS Identity and Access Management (IAM) roles that are associated with the DB cluster. IAM roles that are associated with a DB cluster grant permission for the DB cluster to access other AWS services on your behalf.

_Required_: No

_Type_: List of <a href="dbclusterrole.md">DBClusterRole</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### AvailabilityZones

A list of Availability Zones (AZs) where instances in the DB cluster can be created. For information on AWS Regions and Availability Zones, see Choosing the Regions and Availability Zones in the Amazon Aurora User Guide.

_Required_: No

_Type_: List of String

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### BacktrackWindow

The target backtrack window, in seconds. To disable backtracking, set this value to 0.

_Required_: No

_Type_: Integer

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### BackupRetentionPeriod

The number of days for which automated backups are retained.

_Required_: No

_Type_: Integer

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### CopyTagsToSnapshot

A value that indicates whether to copy all tags from the DB cluster to snapshots of the DB cluster. The default is not to copy them.

_Required_: No

_Type_: Boolean

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### DatabaseName

The name of your database. If you don't provide a name, then Amazon RDS won't create a database in this DB cluster. For naming constraints, see Naming Constraints in the Amazon RDS User Guide.

_Required_: No

_Type_: String

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### GlobalClusterIdentifier

If you are configuring an Aurora global database cluster and want your Aurora DB cluster to be a secondary member in the global database cluster, specify the global cluster ID of the global database cluster. To define the primary database cluster of the global cluster, use the AWS::RDS::GlobalCluster resource.

If you aren't configuring a global database cluster, don't specify this property.

_Required_: No

_Type_: String

_Maximum_: <code>63</code>

_Pattern_: <code>^$|^[a-zA-Z]{1}(?:-?[a-zA-Z0-9]){0,62}$</code>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### DBClusterIdentifier

The DB cluster identifier. This parameter is stored as a lowercase string.

_Required_: No

_Type_: String

_Minimum_: <code>1</code>

_Maximum_: <code>63</code>

_Pattern_: <code>^[a-zA-Z]{1}(?:-?[a-zA-Z0-9]){0,62}$</code>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### DBClusterParameterGroupName

The name of the DB cluster parameter group to associate with this DB cluster.

_Required_: No

_Type_: String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### DBSubnetGroupName

A DB subnet group that you want to associate with this DB cluster.

_Required_: No

_Type_: String

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### DeletionProtection

A value that indicates whether the DB cluster has deletion protection enabled. The database can't be deleted when deletion protection is enabled. By default, deletion protection is disabled.

_Required_: No

_Type_: Boolean

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### EnableCloudwatchLogsExports

The list of log types that need to be enabled for exporting to CloudWatch Logs. The values in the list depend on the DB engine being used. For more information, see Publishing Database Logs to Amazon CloudWatch Logs in the Amazon Aurora User Guide.

_Required_: No

_Type_: List of String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### EnableHttpEndpoint

A value that indicates whether to enable the HTTP endpoint for an Aurora Serverless DB cluster. By default, the HTTP endpoint is disabled.

_Required_: No

_Type_: Boolean

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### EnableIAMDatabaseAuthentication

A value that indicates whether to enable mapping of AWS Identity and Access Management (IAM) accounts to database accounts. By default, mapping is disabled.

_Required_: No

_Type_: Boolean

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### Engine

The name of the database engine to be used for this DB cluster. Valid Values: aurora (for MySQL 5.6-compatible Aurora), aurora-mysql (for MySQL 5.7-compatible Aurora), and aurora-postgresql

_Required_: No

_Type_: String

_Allowed Values_: <code>aurora</code> | <code>aurora-mysql</code> | <code>aurora-postgresql</code> | <code>mysql</code> | <code>postgres</code>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### EngineMode

The DB engine mode of the DB cluster, either provisioned, serverless, parallelquery, global, or multimaster.

_Required_: No

_Type_: String

_Allowed Values_: <code>provisioned</code> | <code>serverless</code> | <code>parallelquery</code> | <code>global</code> | <code>multimaster</code>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### EngineVersion

The version number of the database engine to use.

_Required_: No

_Type_: String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### KmsKeyId

The Amazon Resource Name (ARN) of the AWS Key Management Service master key that is used to encrypt the database instances in the DB cluster, such as arn:aws:kms:us-east-1:012345678910:key/abcd1234-a123-456a-a12b-a123b4cd56ef. If you enable the StorageEncrypted property but don't specify this property, the default master key is used. If you specify this property, you must set the StorageEncrypted property to true.

_Required_: No

_Type_: String

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### MasterUsername

The name of the master user for the DB cluster. You must specify MasterUsername, unless you specify SnapshotIdentifier. In that case, don't specify MasterUsername.

_Required_: No

_Type_: String

_Minimum_: <code>1</code>

_Pattern_: <code>^[a-zA-Z]{1}[a-zA-Z0-9_]*$</code>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### MasterUserPassword

The master password for the DB instance.

_Required_: No

_Type_: String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### Port

The port number on which the instances in the DB cluster accept connections. Default: 3306 if engine is set as aurora or 5432 if set to aurora-postgresql.

_Required_: No

_Type_: Integer

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### PreferredBackupWindow

The daily time range during which automated backups are created if automated backups are enabled using the BackupRetentionPeriod parameter. The default is a 30-minute window selected at random from an 8-hour block of time for each AWS Region. To see the time blocks available, see Adjusting the Preferred DB Cluster Maintenance Window in the Amazon Aurora User Guide.

_Required_: No

_Type_: String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### PreferredMaintenanceWindow

The weekly time range during which system maintenance can occur, in Universal Coordinated Time (UTC). The default is a 30-minute window selected at random from an 8-hour block of time for each AWS Region, occurring on a random day of the week. To see the time blocks available, see Adjusting the Preferred DB Cluster Maintenance Window in the Amazon Aurora User Guide.

_Required_: No

_Type_: String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### ReplicationSourceIdentifier

The Amazon Resource Name (ARN) of the source DB instance or DB cluster if this DB cluster is created as a Read Replica.

_Required_: No

_Type_: String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### RestoreType

The type of restore to be performed. You can specify one of the following values:
full-copy - The new DB cluster is restored as a full copy of the source DB cluster.
copy-on-write - The new DB cluster is restored as a clone of the source DB cluster.

_Required_: No

_Type_: String

_Allowed Values_: <code>full-copy</code> | <code>copy-on-write</code>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### ScalingConfiguration

The ScalingConfiguration property type specifies the scaling configuration of an Aurora Serverless DB cluster.

_Required_: No

_Type_: <a href="scalingconfiguration.md">ScalingConfiguration</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### SnapshotIdentifier

The identifier for the DB snapshot or DB cluster snapshot to restore from.
You can use either the name or the Amazon Resource Name (ARN) to specify a DB cluster snapshot. However, you can use only the ARN to specify a DB snapshot.
After you restore a DB cluster with a SnapshotIdentifier property, you must specify the same SnapshotIdentifier property for any future updates to the DB cluster. When you specify this property for an update, the DB cluster is not restored from the snapshot again, and the data in the database is not changed. However, if you don't specify the SnapshotIdentifier property, an empty DB cluster is created, and the original DB cluster is deleted. If you specify a property that is different from the previous snapshot restore property, the DB cluster is restored from the specified SnapshotIdentifier property, and the original DB cluster is deleted.

_Required_: No

_Type_: String

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### SourceDBClusterIdentifier

The identifier of the source DB cluster from which to restore.

_Required_: No

_Type_: String

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### SourceRegion

The AWS Region which contains the source DB cluster when replicating a DB cluster. For example, us-east-1.

_Required_: No

_Type_: String

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### StorageEncrypted

Indicates whether the DB instance is encrypted.
If you specify the DBClusterIdentifier, SnapshotIdentifier, or SourceDBInstanceIdentifier property, don't specify this property. The value is inherited from the cluster, snapshot, or source DB instance.

_Required_: No

_Type_: Boolean

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### Tags

An array of key-value pairs to apply to this resource.

_Required_: No

_Type_: List of <a href="tag.md">Tag</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### UseLatestRestorableTime

A value that indicates whether to restore the DB cluster to the latest restorable backup time. By default, the DB cluster is not restored to the latest restorable backup time.

_Required_: No

_Type_: Boolean

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### VpcSecurityGroupIds

A list of EC2 VPC security groups to associate with this DB cluster.

_Required_: No

_Type_: List of String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

## Return Values

### Ref

When you pass the logical ID of this resource to the intrinsic `Ref` function, Ref returns the DBClusterIdentifier.

### Fn::GetAtt

The `Fn::GetAtt` intrinsic function returns a value for a specified attribute of this type. The following are the available attributes and sample return values.

For more information about using the `Fn::GetAtt` intrinsic function, see [Fn::GetAtt](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/intrinsic-function-reference-getatt.html).

#### Endpoint

Returns the <code>Endpoint</code> value.

#### Address

Returns the <code>Address</code> value.

#### Port

Returns the <code>Port</code> value.

#### Port

Returns the <code>Port</code> value.

#### Address

Returns the <code>Address</code> value.
