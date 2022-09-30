using Microsoft.VisualStudio.TestTools.UnitTesting;
using Winterdom.BizTalk.PipelineTesting;
using System.IO;
using System;
using System.Text;

namespace BizTalkComponents.PipelineComponents.MessageSplitter.Tests.UnitTests
{
    [TestClass]
    public class MessageSplitterTests
    {
        private const int RowCount = 2000000,
            RowLengh = 80,
            CRLFLength = 2,
            MB = 1024 * 1024;
        private const string HeaderRow = "MyHeaderRow";
        [TestMethod]
        public void SplitMessageByMaxSize()
        {
            var pipeline = PipelineFactory.CreateEmptyReceivePipeline();
            var component = new FlatMessageSplitter
            {
                MaxRows = 0, // Do not consider the row count in the output files.
                MaxSize = 20, //No larger than 20 MBs for the splitted files.
                CopyHeader = false,
                Encoding = "UTF-8",
            };
            pipeline.AddComponent(component, PipelineStage.Disassemble);
            var message = MessageHelper.CreateFromStream(new TestStream(RowLengh, RowCount, HeaderRow));
            int fileSize = RowCount * (RowLengh + CRLFLength);
            int maxSize = 20 * MB;
            int fileCount = (fileSize / maxSize) + (fileSize % MB > 0 ? 1 : 0);
            var output = pipeline.Execute(message);
            Assert.AreEqual(output.Count, fileCount);
            for (int i = 0; i < output.Count; i++)
            {
                Assert.IsTrue(output[i].BodyPart.Data.Length <= maxSize);
            }
        }

        [TestMethod]
        public void SplitMessageByMaxRows()
        {
            var pipeline = PipelineFactory.CreateEmptyReceivePipeline();
            int maxRows = 400000;
            var component = new FlatMessageSplitter
            {
                MaxRows = maxRows, // No more than 400,000 rows(excluiding the header) in the splitted files.
                MaxSize = 0, // Do not consider the output files' size.
                CopyHeader = false,
                Encoding = "UTF-8",
            };
            pipeline.AddComponent(component, PipelineStage.Disassemble);
            var message = MessageHelper.CreateFromStream(new TestStream(RowLengh, RowCount));
            var output = pipeline.Execute(message);
            for (int i = 0; i < output.Count; i++)
            {
                var stream = output[i].BodyPart.GetOriginalDataStream();
                stream.Seek(-(RowLengh + CRLFLength), SeekOrigin.End);
                var reader = new StreamReader(stream);
                var lastLine = reader.ReadLine();
                int lineNum = Convert.ToInt32(lastLine.Substring(lastLine.IndexOf('#') + 1));
                Assert.IsTrue((i < output.Count & lineNum == (i + 1) * maxRows)
                    | (i == (output.Count - 1) & lineNum <= (i + 1) * maxRows));
            }
        }

        [TestMethod]
        public void SplitMessageByMaxSizeWithCopyHeader()
        {
            var pipeline = PipelineFactory.CreateEmptyReceivePipeline();
            int maxRows = 400000;
            int fileSize = RowCount * (RowLengh + CRLFLength);
            int maxSize = 20 * MB;
            int fileCount = (fileSize / maxSize) + (fileSize % MB > 0 ? 1 : 0);
            var component = new FlatMessageSplitter
            {
                MaxRows = 0, // Do not consider the row count in the output files.  // No more than 20,000 rows in the splitted files.
                MaxSize = 20, //No larger than 20 MBs for the splitted files.
                CopyHeader = true,
                Encoding = "UTF-8",
            };
            pipeline.AddComponent(component, PipelineStage.Disassemble);
            var message = MessageHelper.CreateFromStream(new TestStream(RowLengh, RowCount, HeaderRow));
            var output = pipeline.Execute(message);
            Assert.IsTrue(output.Count <= fileCount + 1);
            for (int i = 0; i < output.Count; i++)
            {
                Assert.IsTrue(output[i].BodyPart.Data.Length <= maxSize);
                //var reader = new StreamReader(output[i].BodyPart.Data);
                //var firstLine = reader.ReadLine();
                //Assert.AreNotEqual(firstLine, HeaderRow);
                //reader.BaseStream.Seek(-(RowLengh + CRLFLength), SeekOrigin.End);
                //reader = new StreamReader(output[i].BodyPart.Data);
                //var lastLine = reader.ReadLine();
                //lastLine = lastLine.Substring(lastLine.IndexOf('#') + 1);
            }
        }
        [TestMethod]
        public void SplitMessageByMaxRowsWithCopyHeader()
        {
            var pipeline = PipelineFactory.CreateEmptyReceivePipeline();
            int maxRows = 400000;
            var component = new FlatMessageSplitter
            {
                MaxRows = maxRows, // No more than 400,000 rows(excluiding the header) in the splitted files.
                MaxSize = 0, // Do not consider the output files' size.
                CopyHeader = true,
                Encoding = "UTF-8",
            };
            pipeline.AddComponent(component, PipelineStage.Disassemble);
            var message = MessageHelper.CreateFromStream(new TestStream(RowLengh, RowCount, HeaderRow));
            var output = pipeline.Execute(message);
            for (int i = 0; i < output.Count; i++)
            {
                var stream = output[i].BodyPart.GetOriginalDataStream();
                stream.Seek(-(RowLengh + CRLFLength), SeekOrigin.End);
                var reader = new StreamReader(stream);
                var lastLine = reader.ReadLine();
                int lineNum = (i + 1) + Convert.ToInt32(lastLine.Substring(lastLine.IndexOf('#') + 1));
                Assert.IsTrue((i < output.Count & lineNum == (i + 1) * maxRows)
                    | (i == (output.Count - 1) & lineNum <= (i + 1) * maxRows));
            }
        }

        [TestMethod]
        public void SplitMessageByMaxSizeOrMaxRows_MaxSize()
        {
            var pipeline = PipelineFactory.CreateEmptyReceivePipeline();
            int maxRows = 1000000, maxSizeMB = 20, maxSize = maxSizeMB * MB;
            int fileSize = RowCount * (RowLengh + CRLFLength);
            int fileCount = (fileSize / maxSize) + (fileSize % MB > 0 ? 1 : 0);
            var component = new FlatMessageSplitter
            {
                MaxRows = maxRows, // No more than 1M rows in the splitted files (it should be splitted before).
                MaxSize = maxSizeMB, //No larger than 20 MBs for the splitted files.
                CopyHeader = true,
                Encoding = "UTF-8",
            };
            pipeline.AddComponent(component, PipelineStage.Disassemble);
            var message = MessageHelper.CreateFromStream(new TestStream(RowLengh, RowCount, HeaderRow));
            var output = pipeline.Execute(message);
            Assert.IsTrue(output.Count <= fileCount + 1);
            for (int i = 0; i < output.Count; i++)
            {
                Assert.IsTrue(output[i].BodyPart.Data.Length <= maxSize);
                var stream = output[i].BodyPart.GetOriginalDataStream();
                stream.Seek(-(RowLengh + CRLFLength), SeekOrigin.End);
                var reader = new StreamReader(stream);
                var lastLine = reader.ReadLine();
                int lineNum = (i + 1) + Convert.ToInt32(lastLine.Substring(lastLine.IndexOf('#') + 1));
                Assert.IsTrue(lineNum < maxRows * (i + 1));
            }
        }

        [TestMethod]
        public void SplitMessageByMaxSizeOrMaxRows_MaxRows()
        {
            var pipeline = PipelineFactory.CreateEmptyReceivePipeline();
            int maxRows = 400000, maxSizeMB = 50, maxSize = maxSizeMB * MB;
            int fileSize = RowCount * (RowLengh + CRLFLength);
            int fileCount = (fileSize / maxSize) + (fileSize % MB > 0 ? 1 : 0);
            var component = new FlatMessageSplitter
            {
                MaxRows = maxRows, // No more than 400K rows in the splitted files.
                MaxSize = maxSizeMB, //No larger than 50 MBs for the splitted files (it should be splitted before).
                CopyHeader = true,
                Encoding = "UTF-8",
            };
            pipeline.AddComponent(component, PipelineStage.Disassemble);
            var message = MessageHelper.CreateFromStream(new TestStream(RowLengh, RowCount, HeaderRow));
            var output = pipeline.Execute(message);
            for (int i = 0; i < output.Count; i++)
            {
                Assert.IsTrue(output[i].BodyPart.Data.Length < maxSize, "Size");
                var stream = output[i].BodyPart.GetOriginalDataStream();
                stream.Seek(-(RowLengh + CRLFLength), SeekOrigin.End);
                var reader = new StreamReader(stream);
                var lastLine = reader.ReadLine();
                int lineNum = (i + 1) + Convert.ToInt32(lastLine.Substring(lastLine.IndexOf('#') + 1));
                Assert.IsTrue((i < output.Count & lineNum == (i + 1) * maxRows)
                    | (i == (output.Count - 1) & lineNum <= (i + 1) * maxRows), "RowCount");
            }
        }

    }
}
