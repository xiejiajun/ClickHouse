#include <Storages/MergeTree/MergeTreeBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Storages/StorageMergeTree.h>
#include <Interpreters/PartLog.h>


namespace DB
{

Block MergeTreeBlockOutputStream::getHeader() const
{
    return metadata_snapshot->getSampleBlock();
}


void MergeTreeBlockOutputStream::writePrefix()
{
    /// Only check "too many parts" before write,
    /// because interrupting long-running INSERT query in the middle is not convenient for users.
    storage.delayInsertOrThrowIfNeeded();
}


/**
 * TODO 追踪到最底层的 MergeTreeBlockOutputStream 我们会发现最终数据由MergeTreeDataWriter(dbms/src/Storages/MergeTree/MergeTreeDataWriter.h)写入，
 *  而MergeTreeDataWriter是MergeTreeData(dbms/src/Storages/MergeTree/MergeTreeData.h)的封装，MergeTree的数据都由MergeTreeData对象管理。
 *  存储的格式可以看看[这篇文章](http://jackpgao.github.io/2017/12/06/ClickHouse-Primary-key/)，后面可能会另写文再说说。
 *
 *  TODO MergeTreeBlockOutputStream一次写入一个Block,然后会唤醒后台任务将一个个小的Block合并。这应该就是MergeTree命名的由来了。
 *   由此我们可知，Clickhouse应尽可能的批量写入数据而不是一条一条的写。
 */
void MergeTreeBlockOutputStream::write(const Block & block)
{
    auto part_blocks = storage.writer.splitBlockIntoParts(block, max_parts_per_block, metadata_snapshot);
    for (auto & current_block : part_blocks)
    {
        Stopwatch watch;

        MergeTreeData::MutableDataPartPtr part = storage.writer.writeTempPart(current_block, metadata_snapshot, optimize_on_insert);

        /// If optimize_on_insert setting is true, current_block could become empty after merge
        /// and we didn't create part.
        if (!part)
            continue;

        /// Part can be deduplicated, so increment counters and add to part log only if it's really added
        if (storage.renameTempPartAndAdd(part, &storage.increment, nullptr, storage.getDeduplicationLog()))
        {
            PartLog::addNewPart(storage.getContext(), part, watch.elapsed());

            /// Initiate async merge - it will be done if it's good time for merge and if there are space in 'background_pool'.
            storage.background_executor.triggerTask();
        }
    }
}

}
