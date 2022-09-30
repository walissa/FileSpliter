[![Build status](https://waal.visualstudio.com/BizTalk%20Components/_apis/build/status/MessageSplitter)](https://waal.visualstudio.com/BizTalk%20Components/_build/latest?definitionId=15)


## Description
MessageSplitter is a BizTalk pipeline component that split the incoming text messages to smaller messages, the splitting can be controlled either by the size of the output messages, or the number of the rows in the output messages.

| Property| Description | Type | Validation |
|-|-|-|-|
| MaxSize | The maximum size of the splitted messages in MB, for smaller sizes decimal values are accepted.<br/> Set this property to zero to omit the size check. | Decimal | Optional |
| MaxRows | The maximum rows in the splitted messages.<br/> Set this property to zero to omit the row count check. | Integer | Optional |
| CopyHeader | Set to true to copy the first row of the original message to all output messages. | Bool | Required |
| Encoding | The encoding of the incoming message, if the encoding is not spicified, UTF-8 is used. | String | Optional |
| Disabled | Set to true to deactivate the component. | String | Optional |


## Remarks ##
- If the incoming message contains BOM, the encoding will be detected, otherwise, the specified encoding will be used.
- If both properties MaxRows and MaxSize are specified, the splitter will split the message based on which condition comes first.
- If both properties MaxRows and MaxSize are zeros, then the original message returns as is (No splitting will happen).
