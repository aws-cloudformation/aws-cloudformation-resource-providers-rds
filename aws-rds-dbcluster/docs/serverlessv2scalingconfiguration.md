# AWS::RDS::DBCluster ServerlessV2ScalingConfiguration

Contains the scaling configuration of an Aurora Serverless v2 DB cluster.

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "<a href="#mincapacity" title="MinCapacity">MinCapacity</a>" : <i>Double</i>,
    "<a href="#maxcapacity" title="MaxCapacity">MaxCapacity</a>" : <i>Double</i>
}
</pre>

### YAML

<pre>
<a href="#mincapacity" title="MinCapacity">MinCapacity</a>: <i>Double</i>
<a href="#maxcapacity" title="MaxCapacity">MaxCapacity</a>: <i>Double</i>
</pre>

## Properties

#### MinCapacity

The minimum number of Aurora capacity units (ACUs) for a DB instance in an Aurora Serverless v2 cluster. You can specify ACU values in half-step increments, such as 8, 8.5, 9, and so on. The smallest value that you can use is 0.5.

_Required_: No

_Type_: Double

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### MaxCapacity

The maximum number of Aurora capacity units (ACUs) for a DB instance in an Aurora Serverless v2 cluster. You can specify ACU values in half-step increments, such as 40, 40.5, 41, and so on. The largest value that you can use is 128.

_Required_: No

_Type_: Double

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

