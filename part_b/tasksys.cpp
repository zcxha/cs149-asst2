#include "tasksys.h"

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char *TaskSystemSerial::name()
{
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads) : ITaskSystem(num_threads)
{
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable *runnable, int num_total_tasks)
{
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                          const std::vector<TaskID> &deps)
{
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync()
{
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelSpawn::name()
{
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads) : ITaskSystem(num_threads)
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable *runnable, int num_total_tasks)
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                 const std::vector<TaskID> &deps)
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync()
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSpinning::name()
{
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads) : ITaskSystem(num_threads)
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable *runnable, int num_total_tasks)
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync()
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSleeping::name()
{
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads) : ITaskSystem(num_threads)
{
    threads = new std::thread[num_threads];

    for (int i = 0; i < num_threads; i++)
    {
        threads[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::run_per_thread, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping()
{
    shutdown = true;
    WQ.condition_variable.notify_all();

    for (int i = 0; i < num_threads; i++)
    {
        threads[i].join();
    }

    delete[] threads;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable, int num_total_tasks)
{

    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }
}

void TaskSystemParallelThreadPoolSleeping::run_per_thread()
{
    while (!shutdown)
    {
        Work cur_work;
        {
            std::unique_lock<std::mutex> lk(WQ.mutex);
            WQ.condition_variable.wait(lk, [&]
                                       { return !WQ.works.empty() || shutdown; });
            if (shutdown)
            {
                break;
            }
            std::cerr << __LINE__ << std::endl;
            cur_work = WQ.works.front();
            WQ.works.pop();
            WQ.condition_variable.notify_all();
        }
        
        std::cerr << __LINE__ << std::endl;
        cur_work.runnable->runTask(cur_work.idx, cur_work.num_total_tasks);
        std::cerr << __LINE__ << std::endl;
        update_deps(cur_work);
        std::cerr << __LINE__ << std::endl;
        enqueue();
        std::cerr << __LINE__ << std::endl;
    }
}

// used by thread to update dependencies
// 根据当前完成的Work，判断并决定是否更新它的出度结点如果是就执行
void TaskSystemParallelThreadPoolSleeping::update_deps(Work work)
{
    Launchlet *parent;
    launch.mutex.lock();
    parent = &launch.llet[work.parent];

    parent->work_remain--;
    assert(parent->work_remain >= 0);
    if (parent->work_remain > 0)
    {
        return;
    }

    assert(parent->num_deps == 0);
    assert(parent->status == Queued);

    parent->status = Finished;
    pending_tasks--;
    if (pending_tasks == 0)
    {
        sync_cv.notify_all();
    }
    assert(pending_tasks >= 0);
    for (auto tid : parent->outd)
    {
        Launchlet *llet = &launch.llet[tid];
        llet->num_deps--;
        assert(llet->status == Unqueued);
        assert(llet->num_deps >= 0);
    }

    launch.mutex.unlock();
}

// Used by thread and launch to update work queue
// 进行一次Launch的检查，把
// 没有运行过并且依赖关系满足的LLEt加入工作队列
void TaskSystemParallelThreadPoolSleeping::enqueue()
{
    std::vector<Work> works;
    launch.mutex.lock();
    for (auto &task : launch.llet)
    {
        if (task.status == Unqueued && task.num_deps == 0)
        {
            works.push_back(Work(task.runnable, task.id, task.num_total_tasks, task.id));
            task.status = Queued;
        }
    }
    launch.mutex.unlock();

    WQ.mutex.lock();
    for (auto w : works)
    {
        for(int idx = 0; idx < w.num_total_tasks; idx++)
        {
            WQ.works.push(Work(w.runnable, idx, w.num_total_tasks, w.parent));
        }
    }
    WQ.mutex.unlock();
    WQ.condition_variable.notify_all();
}

// 因为只会传入出现过的TaskID，所以加入一个新的Task时，这个Task还不会被其他人依赖
// 换句话说
// 只需要根据传入的deps，更新已有Launch的出度即可
void TaskSystemParallelThreadPoolSleeping::add_launchlet(IRunnable *runnable, int num_total_tasks,
                                                         const std::vector<TaskID> &deps)
{
    pending_tasks++;
    launch.mutex.lock();
    TaskID tid = launch.llet.size();
    int dep_count = deps.size();
    for (TaskID dep_task : deps)
    {
        Launchlet Llet = launch.llet[dep_task];
        Llet.outd.push_back(tid);

        if (Llet.status == Finished)
        {
            dep_count--;
        }
    }
    assert(dep_count >= 0);
    launch.llet.push_back(Launchlet(tid, runnable, num_total_tasks, dep_count, {}));
    launch.mutex.unlock();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{
    add_launchlet(runnable, num_total_tasks, deps);
    std::cerr << __LINE__ << std::endl;
    enqueue();
    std::cerr << __LINE__ << std::endl;
    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync()
{
    std::cerr << __LINE__ << std::endl;
    std::unique_lock<std::mutex> lk(sync_mutex);
    if (pending_tasks != 0)
    {
        std::cerr << __LINE__ << std::endl;
        sync_cv.wait(lk, [&]
                     { return pending_tasks == 0; });
    }
    std::cerr << __LINE__ << std::endl;
    assert(WQ.works.empty());
    launch.llet.clear();
    return;
}
