# AWS::RDS::DBCluster ScalingConfiguration

The ScalingConfiguration property type specifies the scaling configuration of an Aurora Serverless DB cluster.

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "<a href="#autopause" title="AutoPause">AutoPause</a>" : <i>Boolean</i>,
    "<a href="#maxcapacity" title="MaxCapacity">MaxCapacity</a>" : <i>Integer</i>,
    "<a href="#mincapacity" title="MinCapacity">MinCapacity</a>" : <i>Integer</i>,
    "<a href="#secondsuntilautopause" title="SecondsUntilAutoPause">SecondsUntilAutoPause</a>" : <i>Integer</i>
}
</pre>

### YAML

<pre>
<a href="#autopause" title="AutoPause">AutoPause</a>: <i>Boolean</i>
<a href="#maxcapacity" title="MaxCapacity">MaxCapacity</a>: <i>Integer</i>
<a href="#mincapacity" title="MinCapacity">MinCapacity</a>: <i>Integer</i>
<a href="#secondsuntilautopause" title="SecondsUntilAutoPause">SecondsUntilAutoPause</a>: <i>Integer</i>
</pre>

## Properties

#### AutoPause

A value that indicates whether to allow or disallow automatic pause for an Aurora DB cluster in serverless DB engine mode. A DB cluster can be paused only when it's idle (it has no connections).

_Required_: No

_Type_: Boolean

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### MaxCapacity

The maximum capacity for an Aurora DB cluster in serverless DB engine mode.
For Aurora MySQL, valid capacity values are 1, 2, 4, 8, 16, 32, 64, 128, and 256.
For Aurora PostgreSQL, valid capacity values are 2, 4, 8, 16, 32, 64, 192, and 384.
The maximum capacity must be greater than or equal to the minimum capacity.

_Required_: No

_Type_: Integer

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### MinCapacity

The minimum capacity for an Aurora DB cluster in serverless DB engine mode.
For Aurora MySQL, valid capacity values are 1, 2, 4, 8, 16, 32, 64, 128, and 256.
For Aurora PostgreSQL, valid capacity values are 2, 4, 8, 16, 32, 64, 192, and 384.
The minimum capacity must be less than or equal to the maximum capacity.

_Required_: No

_Type_: Integer

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### SecondsUntilAutoPause

The time, in seconds, before an Aurora DB cluster in serverless mode is paused.

_Required_: No

_Type_: Integer

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)
