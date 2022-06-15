# AWS::RDS::DBClusterEndpoint

The AWS::RDS::DBClusterEndpoint resource allows you to create custom Aurora Cluster endpoint. For more information, see Using custom endpoints in the Amazon RDS Aurora Guide.

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "Type" : "AWS::RDS::DBClusterEndpoint",
    "Properties" : {
        "<a href="#dbclusteridentifier" title="DBClusterIdentifier">DBClusterIdentifier</a>" : <i>String</i>,
        "<a href="#dbclusterendpointidentifier" title="DBClusterEndpointIdentifier">DBClusterEndpointIdentifier</a>" : <i>String</i>,
        "<a href="#endpointtype" title="EndpointType">EndpointType</a>" : <i>String</i>,
        "<a href="#staticmembers" title="StaticMembers">StaticMembers</a>" : <i>[ String, ... ]</i>,
        "<a href="#excludedmembers" title="ExcludedMembers">ExcludedMembers</a>" : <i>[ String, ... ]</i>,
        "<a href="#tags" title="Tags">Tags</a>" : <i>[ <a href="tag.md">Tag</a>, ... ]</i>
    }
}
</pre>

### YAML

<pre>
Type: AWS::RDS::DBClusterEndpoint
Properties:
    <a href="#dbclusteridentifier" title="DBClusterIdentifier">DBClusterIdentifier</a>: <i>String</i>
    <a href="#dbclusterendpointidentifier" title="DBClusterEndpointIdentifier">DBClusterEndpointIdentifier</a>: <i>String</i>
    <a href="#endpointtype" title="EndpointType">EndpointType</a>: <i>String</i>
    <a href="#staticmembers" title="StaticMembers">StaticMembers</a>: <i>
      - String</i>
    <a href="#excludedmembers" title="ExcludedMembers">ExcludedMembers</a>: <i>
      - String</i>
    <a href="#tags" title="Tags">Tags</a>: <i>
      - <a href="tag.md">Tag</a></i>
</pre>

## Properties

#### DBClusterIdentifier

The DB cluster identifier of the DB cluster associated with the endpoint. This parameter is stored as a lowercase string.

_Required_: Yes

_Type_: String

_Minimum_: <code>1</code>

_Maximum_: <code>63</code>

_Pattern_: <code>^[a-zA-Z]{1}(?:-?[a-zA-Z0-9]){0,62}$</code>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### DBClusterEndpointIdentifier

The identifier to use for the new endpoint. This parameter is stored as a lowercase string.

_Required_: Yes

_Type_: String

_Minimum_: <code>1</code>

_Maximum_: <code>63</code>

_Pattern_: <code>^[a-z]{1}(?:-?[a-z0-9]){0,62}$</code>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### EndpointType

The type of the endpoint, one of: READER , WRITER , ANY

_Required_: Yes

_Type_: String

_Allowed Values_: <code>READER</code> | <code>WRITER</code> | <code>ANY</code>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### StaticMembers

List of DB instance identifiers that are part of the custom endpoint group.

_Required_: No

_Type_: List of String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### ExcludedMembers

List of DB instance identifiers that aren't part of the custom endpoint group. All other eligible instances are reachable through the custom endpoint. This parameter is relevant only if the list of static members is empty.

_Required_: No

_Type_: List of String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### Tags

An array of key-value pairs to apply to this resource.

_Required_: No

_Type_: List of <a href="tag.md">Tag</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

## Return Values

### Ref

When you pass the logical ID of this resource to the intrinsic `Ref` function, Ref returns the DBClusterEndpointIdentifier.

### Fn::GetAtt

The `Fn::GetAtt` intrinsic function returns a value for a specified attribute of this type. The following are the available attributes and sample return values.

For more information about using the `Fn::GetAtt` intrinsic function, see [Fn::GetAtt](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/intrinsic-function-reference-getatt.html).

#### Endpoint

The DNS address of the endpoint.

#### DBClusterEndpointArn

The Amazon Resource Name (ARN) for the endpoint.
