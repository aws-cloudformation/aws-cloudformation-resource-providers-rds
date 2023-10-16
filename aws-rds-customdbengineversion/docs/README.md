# AWS::RDS::CustomDBEngineVersion

The AWS::RDS::CustomDBEngineVersion resource creates an Amazon RDS custom DB engine version.

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "Type" : "AWS::RDS::CustomDBEngineVersion",
    "Properties" : {
        "<a href="#databaseinstallationfiless3bucketname" title="DatabaseInstallationFilesS3BucketName">DatabaseInstallationFilesS3BucketName</a>" : <i>String</i>,
        "<a href="#databaseinstallationfiless3prefix" title="DatabaseInstallationFilesS3Prefix">DatabaseInstallationFilesS3Prefix</a>" : <i>String</i>,
        "<a href="#description" title="Description">Description</a>" : <i>String</i>,
        "<a href="#engine" title="Engine">Engine</a>" : <i>String</i>,
        "<a href="#engineversion" title="EngineVersion">EngineVersion</a>" : <i>String</i>,
        "<a href="#kmskeyid" title="KMSKeyId">KMSKeyId</a>" : <i>String</i>,
        "<a href="#manifest" title="Manifest">Manifest</a>" : <i>String</i>,
        "<a href="#sourcecustomdbengineversionarn" title="SourceCustomDBEngineVersionArn">SourceCustomDBEngineVersionArn</a>" : <i>String</i>,
        "<a href="#useawsprovidedlatestimage" title="UseAwsProvidedLatestImage">UseAwsProvidedLatestImage</a>" : <i>Boolean</i>,
        "<a href="#status" title="Status">Status</a>" : <i>String</i>,
        "<a href="#tags" title="Tags">Tags</a>" : <i>[ <a href="tag.md">Tag</a>, ... ]</i>
    }
}
</pre>

### YAML

<pre>
Type: AWS::RDS::CustomDBEngineVersion
Properties:
    <a href="#databaseinstallationfiless3bucketname" title="DatabaseInstallationFilesS3BucketName">DatabaseInstallationFilesS3BucketName</a>: <i>String</i>
    <a href="#databaseinstallationfiless3prefix" title="DatabaseInstallationFilesS3Prefix">DatabaseInstallationFilesS3Prefix</a>: <i>String</i>
    <a href="#description" title="Description">Description</a>: <i>String</i>
    <a href="#engine" title="Engine">Engine</a>: <i>String</i>
    <a href="#engineversion" title="EngineVersion">EngineVersion</a>: <i>String</i>
    <a href="#kmskeyid" title="KMSKeyId">KMSKeyId</a>: <i>String</i>
    <a href="#manifest" title="Manifest">Manifest</a>: <i>String</i>
    <a href="#sourcecustomdbengineversionarn" title="SourceCustomDBEngineVersionArn">SourceCustomDBEngineVersionArn</a>: <i>String</i>
    <a href="#useawsprovidedlatestimage" title="UseAwsProvidedLatestImage">UseAwsProvidedLatestImage</a>: <i>Boolean</i>
    <a href="#status" title="Status">Status</a>: <i>String</i>
    <a href="#tags" title="Tags">Tags</a>: <i>
      - <a href="tag.md">Tag</a></i>
</pre>

## Properties

#### DatabaseInstallationFilesS3BucketName

The name of an Amazon S3 bucket that contains database installation files for your CEV. For example, a valid bucket name is `my-custom-installation-files`.

_Required_: Yes

_Type_: String

_Minimum Length_: <code>3</code>

_Maximum Length_: <code>63</code>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### DatabaseInstallationFilesS3Prefix

The Amazon S3 directory that contains the database installation files for your CEV. For example, a valid bucket name is `123456789012/cev1`. If this setting isn't specified, no prefix is assumed.

_Required_: No

_Type_: String

_Minimum Length_: <code>1</code>

_Maximum Length_: <code>255</code>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### Description

An optional description of your CEV.

_Required_: No

_Type_: String

_Minimum Length_: <code>1</code>

_Maximum Length_: <code>1000</code>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### Engine

The database engine to use for your custom engine version (CEV). The only supported value is `custom-oracle-ee`.

_Required_: Yes

_Type_: String

_Minimum Length_: <code>1</code>

_Maximum Length_: <code>35</code>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### EngineVersion

The name of your CEV. The name format is 19.customized_string . For example, a valid name is 19.my_cev1. This setting is required for RDS Custom for Oracle, but optional for Amazon RDS. The combination of Engine and EngineVersion is unique per customer per Region.

_Required_: Yes

_Type_: String

_Minimum Length_: <code>1</code>

_Maximum Length_: <code>60</code>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### KMSKeyId

The AWS KMS key identifier for an encrypted CEV. A symmetric KMS key is required for RDS Custom, but optional for Amazon RDS.

_Required_: No

_Type_: String

_Minimum Length_: <code>1</code>

_Maximum Length_: <code>2048</code>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### Manifest

The CEV manifest, which is a JSON document that describes the installation .zip files stored in Amazon S3. Specify the name/value pairs in a file or a quoted string. RDS Custom applies the patches in the order in which they are listed.

_Required_: No

_Type_: String

_Minimum Length_: <code>1</code>

_Maximum Length_: <code>51000</code>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### SourceCustomDBEngineVersionArn

The ARN of the source custom engine version.

_Required_: No

_Type_: String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### UseAwsProvidedLatestImage

A value that indicates whether AWS provided latest image is applied automatically to the Custom Engine Version. By default, AWS provided latest image is applied automatically.

_Required_: No

_Type_: Boolean

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### Status

The availability status to be assigned to the CEV.

_Required_: No

_Type_: String

_Allowed Values_: <code>available</code> | <code>inactive</code> | <code>inactive-except-restore</code>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### Tags

An array of key-value pairs to apply to this resource.

_Required_: No

_Type_: List of <a href="tag.md">Tag</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

## Return Values

### Fn::GetAtt

The `Fn::GetAtt` intrinsic function returns a value for a specified attribute of this type. The following are the available attributes and sample return values.

For more information about using the `Fn::GetAtt` intrinsic function, see [Fn::GetAtt](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/intrinsic-function-reference-getatt.html).

#### DBEngineVersionArn

The ARN of the custom engine version.

