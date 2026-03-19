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
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync()
{
    // You do not need to implement this method.
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

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads) : ITaskSystem(num_threads), num_threads(num_threads)
{
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run_per_thread(IRunnable *runnable, int tid, int num_total_tasks, int tasks_per_thread)
{
    int start_task_id = tid * tasks_per_thread;
    int end_task_id = std::min(num_total_tasks, start_task_id + tasks_per_thread);

    for (int i = start_task_id; i < end_task_id; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }
    // runnable->runTask(tid, num_total_tasks);
}

void TaskSystemParallelSpawn::run(IRunnable *runnable, int num_total_tasks)
{

    int cur_thread_num = std::min(num_threads, num_total_tasks);
    int tasks_per_thread = (num_total_tasks + cur_thread_num - 1) / cur_thread_num;

    std::thread *threads = new std::thread[cur_thread_num];

    for (int i = 0; i < cur_thread_num; i++)
    {
        threads[i] = std::thread(&TaskSystemParallelSpawn::run_per_thread, this, runnable, i, num_total_tasks, tasks_per_thread);
    }

    for (int i = 0; i < cur_thread_num; i++)
    {
        threads[i].join();
    }

    delete[] threads;
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                 const std::vector<TaskID> &deps)
{
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync()
{
    // You do not need to implement this method.
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

void TaskSystemParallelThreadPoolSpinning::run_per_thread()
{
    while (1)
    {
        if (stop)
        {
            break;
        }
        work_queue->mutex->lock();
        if (work_queue->queue.empty())
        {
            work_queue->mutex->unlock();
            continue;
        }
                                                     
        IRunnable *runnable = cur_runnable;
        int task_id = work_queue->queue.front();
        int total_tasks = num_total_tasks;
        work_queue->queue.pop();

        work_queue->mutex->unlock();

        // 在此处runnable、num_total_tasks理论上已经被赋值
        runnable->runTask(task_id, total_tasks);

        finish_queue->mutex->lock();
        finish_queue->queue.push(task_id);
        finish_queue->mutex->unlock();
    }
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads) : ITaskSystem(num_threads), num_threads(num_threads)
{
    work_queue = new Queue();
    finish_queue = new Queue();
    threads = new std::thread[num_threads];

    for (int i = 0; i < num_threads; i++)
    {
        threads[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::run_per_thread, this);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning()
{
    stop = true;
    for (int i = 0; i < num_threads; i++)
    {
        threads[i].join();
    }
    delete[] threads;
    delete work_queue;
    delete finish_queue;
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable *runnable, int num_total_tasks)
{
    work_queue->mutex->lock();
    cur_runnable = runnable;
    this->num_total_tasks = num_total_tasks;

    for (int i = 0; i < num_total_tasks; i++)
    {
        work_queue->queue.push(i);
    }
    work_queue->mutex->unlock();

    while (1)
    {
        finish_queue->mutex->lock();
        if (finish_queue->queue.size() == num_total_tasks)
        {
            finish_queue->queue = std::queue<int>();
            finish_queue->mutex->unlock();
            break;
        }
        finish_queue->mutex->unlock();
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync()
{
    // You do not need to implement this method.
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
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping()
{
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable, int num_total_tasks)
{

    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{

    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync()
{

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
