using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ConsoleApplication
{
    class Copier : IDisposable
    {
        private string fromFile;
        private string toFile;
        private string offsetFile;
        private int chunkSize;
        private int maxChunksBuffer;

        private FileStream offsetFileStream;
        private FileStream fromFileStream;
        private FileStream toFileStream;
        private volatile int spinner;

        public Copier(
            string fromFile,
            string toFile,
            string offsetFile,
            int chunkSize,
            int maxChunksBuffer)
        {
            if (chunkSize < 1024)
            {
                throw new CopyException("The chunk size should be at least a 1KB"); 
            }

            this.fromFile = fromFile;
            this.toFile = toFile;
            this.offsetFile = offsetFile;
            this.chunkSize = chunkSize;
            this.maxChunksBuffer = maxChunksBuffer;
        }

        public async Task Start(CancellationToken ct)
        {
            long offset;
            long size = new FileInfo(fromFile).Length;

            if (File.Exists(offsetFile))
            {
                offsetFileStream = File.Open(offsetFile, FileMode.Open, FileAccess.ReadWrite);
                offset = await this.ReadOffsetFile();

                if (size == offset)
                {
                    return;
                }

                if (size < offset)
                {
                    throw new CopyException("The bytes copied in the offset file are more than the size of the file we are supposed to copy.");
                }
            }
            else
            {
                offsetFileStream = File.Open(offsetFile, FileMode.CreateNew, FileAccess.ReadWrite);
                await this.WriteOffsetFile(0);
                offset = 0;
            }

            fromFileStream = File.Open(fromFile, FileMode.Open, FileAccess.Read);
            toFileStream = File.Open(toFile, FileMode.OpenOrCreate, FileAccess.Write);

            fromFileStream.Seek(offset, SeekOrigin.Begin);
            toFileStream.Seek(offset, SeekOrigin.Begin);
            
            var readChunkBlock = new TransformBlock<long, Tuple<long, byte[]>>(
                    async (chunk) =>
                    {
                        if (fromFileStream.Position != offset + (chunk * (long)chunkSize))
                        {
                            throw new Exception("Something went wrong! The file position is not where it is supposed to be.");
                        }

                        int count = chunkSize;
                        byte[] chunkBuffer = new byte[chunkSize];
                        while (count > 0)
                        {
                            int bytesRead = await fromFileStream.ReadAsync(chunkBuffer, chunkSize - count, count);
                            if (bytesRead == 0)
                            {
                                break;
                            }

                            count -= bytesRead;

                            this.Spin();
                        }

                        // if this is the last chunk resize the chunk buffer
                        if (count > 0)
                        {
                            byte[] lastChunkBuffer = new byte[chunkSize - count];
                            Buffer.BlockCopy(chunkBuffer, 0, lastChunkBuffer, 0, chunkSize - count);

                            chunkBuffer = lastChunkBuffer;
                        }

                        return Tuple.Create(chunk, chunkBuffer);
                    },
                    new ExecutionDataflowBlockOptions
                    {
                        MaxDegreeOfParallelism = 1,
                        BoundedCapacity = maxChunksBuffer,
                        EnsureOrdered = true,
                        CancellationToken = ct,
                    });

            var writeChunkBlock = new ActionBlock<Tuple<long, byte[]>>(
                    async (chunkInfo) =>
                    {
                        long chunk = chunkInfo.Item1;
                        byte[] chunkBuffer = chunkInfo.Item2;

                        await toFileStream.WriteAsync(chunkBuffer, 0, chunkBuffer.Length);
                        await toFileStream.FlushAsync();

                        long copied =
                            offset + // where we started
                            (chunk * (long)chunkSize) + // the amount copied so far (except this chunk)
                            chunkBuffer.Length; // the size of the current chunk (if its the last one, it may be less than 'chunkSize')

                        await this.WriteOffsetFile(copied);

                        this.UpdatePercentage(
                            Convert.ToInt32(
                                Math.Floor((double)100 * copied / size)));
                    },
                    new ExecutionDataflowBlockOptions
                    {
                        MaxDegreeOfParallelism = 1,
                        BoundedCapacity = maxChunksBuffer,
                        EnsureOrdered = true,
                        CancellationToken = ct,
                    });

            var bufferBlock = new BufferBlock<long>(
                new DataflowBlockOptions
                {
                    BoundedCapacity = DataflowBlockOptions.Unbounded,
                    EnsureOrdered = true,
                    CancellationToken = ct,
                });

            bufferBlock.LinkTo(
                readChunkBlock,
                new DataflowLinkOptions { PropagateCompletion = true });

            readChunkBlock.LinkTo(
                writeChunkBlock,
                new DataflowLinkOptions { PropagateCompletion = true });

            long chunksLeft =
                Convert.ToInt64(
                    Math.Ceiling((double)(size - offset) / chunkSize));
            
            if (chunksLeft < 1)
            {
                throw new CopyException("Something went wrong! There should be at least one chunk left.");
            }

            for (long i = 0; i < chunksLeft; i++)
            {
                await bufferBlock.SendAsync(i);
            }
            bufferBlock.Complete();

            await writeChunkBlock.Completion;

            fromFileStream.Dispose();
            toFileStream.Dispose();
            offsetFileStream.Dispose();
        }

        private async Task WriteOffsetFile(long copied)
        {
            //truncate the file
            offsetFileStream.SetLength(0);

            using (var textWriter = new StreamWriter(this.offsetFileStream, Encoding.ASCII, 1024, true))
            {
                await textWriter.WriteAsync(copied.ToString());
            }

            await offsetFileStream.FlushAsync();
        }

        private async Task<long> ReadOffsetFile()
        {
            offsetFileStream.Seek(0, SeekOrigin.Begin);

            using (var textReader = new StreamReader(this.offsetFileStream, Encoding.ASCII, false, 1024, true))
            {
                var offsetStr = await textReader.ReadLineAsync();
                long res;
                if (!long.TryParse(offsetStr, out res))
                {
                    throw new CopyException("Unrecognizable content in offset file");
                }

                return res;
            }
        }

        private void Spin()
        {
            Console.Write($"\r{GetNextSpinner()}");
        }

        private void UpdatePercentage(int percent)
        {
            Console.Write($"\r{GetNextSpinner()}   {percent}%");
        }

        private string GetNextSpinner()
        {
            Interlocked.Increment(ref spinner);
            switch (spinner % 4)
            {
                default:
                case 0: return "/";
                case 1: return "-";
                case 2: return "\\";
                case 3: return "|";
            }
        }

        #region IDisposable Support
        private bool disposedValue = false;

        protected virtual void Dispose(bool disposing)
        {
            if (!this.disposedValue)
            {
                if (disposing)
                {
                    if (fromFileStream != null)
                    {
                        this.fromFileStream.Dispose();
                        this.fromFileStream = null;
                    }
                    if (this.toFileStream != null)
                    {
                        this.toFileStream.Dispose();
                        this.toFileStream = null;
                    }
                    if (this.offsetFile != null)
                    {
                        this.offsetFileStream.Dispose();
                        this.offsetFileStream = null;
                    }
                }

                this.disposedValue = true;
            }
        }

        void IDisposable.Dispose()
        {
            this.Dispose(true);
        }
        #endregion
    }
}
