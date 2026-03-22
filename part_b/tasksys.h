#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <mutex>
#include <condition_variable>
#include <thread>
#include <queue>
#include <vector>
#include <atomic>
#include <cassert>
#include <iostream>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial : public ITaskSystem
{
public:
    TaskSystemSerial(int num_threads);
    ~TaskSystemSerial();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn : public ITaskSystem
{
public:
    TaskSystemParallelSpawn(int num_threads);
    ~TaskSystemParallelSpawn();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning : public ITaskSystem
{
public:
    TaskSystemParallelThreadPoolSpinning(int num_threads);
    ~TaskSystemParallelThreadPoolSpinning();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();
};

enum Status {
    Unqueued,
    Queued,
    Finished
};

class Launchlet
{
public:
    TaskID id;
    IRunnable *runnable;
    int num_total_tasks;

    int work_remain;
    int num_deps;
    std::vector<TaskID> outd;
    int status{Unqueued};
    Launchlet() {}
    Launchlet(TaskID id, IRunnable *runnable, int num_total_tasks, int num_deps, std::vector<TaskID> outd):
    id(id), runnable(runnable), num_total_tasks(num_total_tasks), num_deps(num_deps), outd(outd), work_remain(num_total_tasks) {}
};

class Launch
{
public:
    std::mutex mutex;
    std::condition_variable condition_variable;
    std::vector<Launchlet> llet;
};

class Work
{
public:
    IRunnable *runnable;
    int idx;
    int num_total_tasks;
    TaskID parent;
    Work() {};
    Work(IRunnable *runnable, int idx, int num_total_tasks, TaskID parent):
    runnable(runnable), idx(idx), num_total_tasks(num_total_tasks), parent(parent) {}
};

class WorkQueue
{
public:
    std::mutex mutex;
    std::condition_variable condition_variable;
    std::queue<Work> works;
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping : public ITaskSystem
{
public:
    int num_threads;
    std::thread *threads;
    void run_per_thread();

    // manage working queue
    void enqueue();
    WorkQueue WQ;

    // manage launches
    void update_deps(Work work);
    void add_launchlet(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps);
    Launch launch;

    // for RAII purpose
    std::atomic<bool> shutdown{false};

    // responsible for syncing
    std::mutex sync_mutex;
    std::condition_variable sync_cv;
    std::atomic<int> pending_tasks{0};
    TaskSystemParallelThreadPoolSleeping(int num_threads);
    ~TaskSystemParallelThreadPoolSleeping();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();
};

#endif
