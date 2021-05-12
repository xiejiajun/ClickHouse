#pragma once

#include <Core/Block.h>
#include <DataStreams/IBlockStream_fwd.h>
#include <Storages/TableLockHolder.h>

#include <boost/noncopyable.hpp>

#include <memory>
#include <string>
#include <vector>


namespace DB
{

struct Progress;

/** Interface of stream for writing data (into table, filesystem, network, terminal, etc.)
 * TODO 不同的Stream可以组合起来完成数据的转化。比如最初的 IBlockInputStream外层套一个 FilterBlockInputStream过滤掉不符合条件的数据，
 *  再接一个AggregatingBlockInputStream将原始数据聚合给下一个 IBlockInputStream。其实Block Stream类似TiDB里面的算子,
 *  或者类比Python的迭代器，最外层不断调用 read/write方法驱动整个计算的进行。
 *  也类似于Spark的RDD，子RDD不断调用compute方法拉取父RDD计算的结果，然后执行自己特定的过滤/聚合等逻辑后再丢给自己的子RDD
 */
class IBlockOutputStream : private boost::noncopyable
{
public:
    IBlockOutputStream() {}

    /** Get data structure of the stream in a form of "header" block (it is also called "sample block").
      * Header block contains column names, data types, columns of size 0. Constant columns must have corresponding values.
      * You must pass blocks of exactly this structure to the 'write' method.
      */
    virtual Block getHeader() const = 0;

    /** Write block.
      */
    virtual void write(const Block & block) = 0;

    /** Write or do something before all data or after all data.
      */
    virtual void writePrefix() {}
    virtual void writeSuffix() {}

    /** Flush output buffers if any.
      */
    virtual void flush() {}

    /** Methods to set additional information for output in formats, that support it.
      */
    virtual void setRowsBeforeLimit(size_t /*rows_before_limit*/) {}
    virtual void setTotals(const Block & /*totals*/) {}
    virtual void setExtremes(const Block & /*extremes*/) {}

    /** Notify about progress. Method could be called from different threads.
      * Passed value are delta, that must be summarized.
      */
    virtual void onProgress(const Progress & /*progress*/) {}

    /** Content-Type to set when sending HTTP response.
      */
    virtual std::string getContentType() const { return "text/plain; charset=UTF-8"; }

    virtual ~IBlockOutputStream() = default;

    /** Don't let to alter table while instance of stream is alive.
      */
    void addTableLock(const TableLockHolder & lock) { table_locks.push_back(lock); }

private:
    std::vector<TableLockHolder> table_locks;
};

}
