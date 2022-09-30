using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BizTalkComponents.PipelineComponents.MessageSplitter.Tests.UnitTests
{
    public class TestStream : Stream
    {
        private int lineLength, extendedLineLength;
        private long rowCount;
        private long length;
        private Encoding encoding;
        private long position;
        private string header;
        private bool headerAdded;
        public TestStream(int lineLength, long rowCount, string header = null)
        {
            this.lineLength = lineLength;
            extendedLineLength = lineLength + 2;
            this.rowCount = rowCount + (string.IsNullOrEmpty(header) ? 0 : 1);
            length = this.rowCount * extendedLineLength;
            encoding = new UTF8Encoding(false);
            this.header = header;
        }
        public override bool CanRead
        {
            get
            {
                return true;
            }
        }

        public override bool CanSeek
        {
            get
            {
                return false;
            }
        }

        public override bool CanWrite
        {
            get
            {
                return false;
            }
        }

        public override long Length
        {
            get
            {
                return length;
            }
        }

        public override long Position
        {
            get
            {
                return position;
            }

            set
            {
                throw new NotImplementedException();
            }
        }

        public override void Flush()
        {
            throw new NotImplementedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (position == length) return 0;
            long linePos = position % extendedLineLength;
            long lineNum = position / extendedLineLength;
            if (headerAdded) lineNum--;
            var sb = new StringBuilder();
            int linesToGenerate = count / extendedLineLength;
            int remainingSpace = count % extendedLineLength;
            if (remainingSpace > 0) linesToGenerate++;
            if ((linesToGenerate * extendedLineLength) - linePos < count) linesToGenerate++;
            for (int i = 0; i < linesToGenerate; i++)
            {
                if (position == 0 & !headerAdded & !string.IsNullOrEmpty(header))
                {
                    headerAdded = true;
                    sb.AppendLine(header.PadRight(lineLength));
                    continue;
                }
                lineNum++;
                sb.AppendLine($"Line#{lineNum}".PadRight(lineLength));
            }
            var s = sb.ToString();
            if (count + position > length)
            {
                count = (int)(length - position);
            }

            int bytesCount = encoding.GetBytes(sb.ToString(), (int)linePos, count, buffer, offset);
            position += bytesCount;
            return bytesCount;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }
    }
}
