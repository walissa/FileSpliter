using Microsoft.BizTalk.Component.Interop;
using Microsoft.BizTalk.Message.Interop;
using Microsoft.BizTalk.Streaming;
using System.IO;
using System.Text;

namespace BizTalkComponents.PipelineComponents.MessageSplitter.Splitters
{
    public class FlatFileSplitProcessor
    {
        private Stream inputStream;
        private StreamReader reader;
        private Encoding _encoding;
        private bool copyHeader, IsFirstRow, addLastLine;
        private long maxLength;
        private int maxRows;
        private string lastLine = null, header = null;
        private int headerSize = 0;
        private IBaseMessageContext msgCtx;

        public FlatFileSplitProcessor(IBaseMessage pInMsg, long maxLength, int maxRows, string encodingName, bool copyHeader)
        {
            msgCtx = PipelineUtil.CloneMessageContext(pInMsg.Context);
            var stream = pInMsg.BodyPart.GetOriginalDataStream();
            VirtualStream virtualStream = new VirtualStream(VirtualStream.MemoryFlag.AutoOverFlowToDisk);
            inputStream = new ReadOnlySeekableStream(stream, virtualStream);
            var bom = new byte[4];
            var i = inputStream.Read(bom, 0, 4);
            inputStream.Seek(0, SeekOrigin.Begin);
            _encoding = CreateEncoding(bom, encodingName, false);
            this.copyHeader = copyHeader;
            this.maxLength = maxLength;
            this.maxRows = maxRows;
            reader = new StreamReader(inputStream, _encoding);
            IsFirstRow = true;
        }

        private Encoding CreateEncoding(byte[] bom, string encodingName, bool preserveBOM = true)
        {
            // Analyze the BOM
            if (bom[0] == 0x2b && bom[1] == 0x2f && bom[2] == 0x76) return Encoding.UTF7;
            if (bom[0] == 0xef && bom[1] == 0xbb && bom[2] == 0xbf) return Encoding.UTF8;
            if (bom[0] == 0xff && bom[1] == 0xfe && bom[2] == 0 && bom[3] == 0) return Encoding.UTF32; //UTF-32LE
            if (bom[0] == 0xff && bom[1] == 0xfe) return Encoding.Unicode; //UTF-16LE
            if (bom[0] == 0xfe && bom[1] == 0xff) return Encoding.BigEndianUnicode; //UTF-16BE
            if (bom[0] == 0 && bom[1] == 0 && bom[2] == 0xfe && bom[3] == 0xff) return new UTF32Encoding(true, true);  //UTF-32BE
            if (string.IsNullOrEmpty(encodingName)) encodingName = "utf-8";
            var encoding = Encoding.GetEncoding(encodingName);
            if (!preserveBOM)
            {
                if (encoding == Encoding.UTF8)
                {
                    encoding = new UTF8Encoding(false);
                }
                else if (encoding == Encoding.Unicode)
                {
                    encoding = new UnicodeEncoding(false, false);
                }
                else if (encoding==Encoding.BigEndianUnicode)
                {
                    encoding = new UnicodeEncoding(true, false);
                }
                else if(encoding==Encoding.UTF32)
                {
                    encoding = new UTF32Encoding(false, false);
                }
                else if (encoding is UTF32Encoding & encoding!=Encoding.UTF32)
                {
                    encoding = new UTF32Encoding(true, false);
                }
            }
            return encoding;
        }

        private Stream GetNextPart()
        {
            var ms = new MemoryStream();
            var writer = new StreamWriter(ms, _encoding);
            int i = 0;
            long fileSize = 0;
            if (reader.Peek() < 0 & !(addLastLine && lastLine.Length > 0)) return null;
            if (copyHeader)
            {
                if (IsFirstRow)
                {
                    header = reader.ReadLine();
                    headerSize = _encoding.GetByteCount(header);
                    headerSize = _encoding.GetByteCount(header) + writer.NewLine.Length;
                    IsFirstRow = false;
                }
                writer.WriteLine(header);
                fileSize += headerSize;
                i++;
            }
            while (reader.Peek() >= 0 | addLastLine)
            {
                i++;
                if (!addLastLine)
                    lastLine = reader.ReadLine();
                else
                    addLastLine = false;
                var length = _encoding.GetByteCount(lastLine) + writer.NewLine.Length;
                if ((maxLength>0 & (fileSize + length) >= maxLength) | (i > maxRows & maxRows > 0))
                {
                    addLastLine = true;
                    break;
                }
                writer.WriteLine(lastLine);
                fileSize += length;
            }
            if (i == 0) return null;
            writer.Flush();
            ms.Seek(0, SeekOrigin.Begin);
            return ms;
        }

        public IBaseMessage GetNext(IPipelineContext pContext)
        {
            var stream = GetNextPart();
            IBaseMessage msg = null;
            if (stream != null)
            {
                msg = pContext.GetMessageFactory().CreateMessage();
                msg.AddPart("Body", pContext.GetMessageFactory().CreateMessagePart(), true);
                msg.BodyPart.Data = stream;
                msg.Context = PipelineUtil.CloneMessageContext(msgCtx);
                pContext.ResourceTracker.AddResource(stream);
            }
            return msg;
        }
    }
}
