using BizTalkComponents.Utils;
using Microsoft.BizTalk.Component.Interop;
using Microsoft.BizTalk.Message.Interop;
using System.ComponentModel;
using BizTalkComponents.PipelineComponents.MessageSplitter.Splitters;

namespace BizTalkComponents.PipelineComponents.MessageSplitter
{
    [ComponentCategory(CategoryTypes.CATID_PipelineComponent)]
    [ComponentCategory(CategoryTypes.CATID_DisassemblingParser)]
    [System.Runtime.InteropServices.Guid("be52afe7-a631-43a3-8956-0309c841ee61")]
    public partial class FlatMessageSplitter : IBaseComponent, IDisassemblerComponent, IComponentUI, IPersistPropertyBag
    {
        [RequiredRuntime]
        [DisplayName("Maximum Size(MB)")]
        [Description("Specifies the maximum size based on which the file will be splited.")]
        public double MaxSize { get; set; }

        [DisplayName("Copy Header")]
        [Description("Copies header row to all splitted messages")]
        public bool CopyHeader { get; set; }

        [DisplayName("Encoding")]
        [Description("Specifies the encoding for the incoming message, if it is empty, UTF-8 is used.")]
        public string Encoding { get; set; }

        [DisplayName("Max Rows")]
        [Description("Specifies the maximum number of rows per message, set to zero to split by size only.")]
        public int MaxRows { get; set; }

        private const int MB = 1024 * 1024;

        private FlatFileSplitProcessor ffSplitter;
        private IBaseMessage msg;
        public void Disassemble(IPipelineContext pContext, IBaseMessage pInMsg)
        {
            if (MaxSize == 0 & MaxRows == 0)
            {
                msg = pInMsg;
            }
            else
            {
                ffSplitter=new FlatFileSplitProcessor(pInMsg, (long)(MaxSize * MB), MaxRows, Encoding, CopyHeader);
            }
        }

        public IBaseMessage GetNext(IPipelineContext pContext)
        {
            IBaseMessage result = null;
            if (MaxRows > 0 | MaxSize > 0)
            {
                result = ffSplitter.GetNext(pContext);
            }
            else
            {
                result = msg;
                msg = null;
            }
            return result;
        }
    }
}