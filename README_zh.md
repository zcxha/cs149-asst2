# 作业 2：从零开始构建任务执行库 #

**截止时间：10 月 16 日（周四）23:59**

**总分 100 分**

## 概述 ##

每个人都喜欢尽快完成任务，而在这次作业中，我们正是要求你做到这一点！你将实现一个 C++ 库，使其能够在多核 CPU 上尽可能高效地执行应用程序提供的任务。

在作业的第一部分中，你将实现一个支持对同一任务的大量实例进行批量（数据并行）启动的任务执行库版本。这一功能与你在作业 1 中用来在多核上并行化代码的 [ISPC task launch 行为](http://ispc.github.io/ispc.html#task-parallelism-launch-and-sync-statements) 类似。

在第二部分中，你将扩展你的任务运行时系统，使其能够执行更复杂的_任务图（task graphs）_，其中某些任务的执行可能依赖于其他任务产生的结果。这些依赖关系会限制你的任务调度系统中哪些任务可以安全地并行运行。在并行机器上调度数据并行任务图的执行，是许多流行并行运行时系统都具备的功能，从著名的 [Thread Building Blocks](https://github.com/intel/tbb) 库，到 [Apache Spark](https://spark.apache.org/)，再到现代深度学习框架如 [PyTorch](https://pytorch.org/) 和 [TensorFlow](https://www.tensorflow.org/) 都是如此。

本次作业将要求你：

* 使用线程池管理任务执行
* 使用互斥锁、条件变量等同步原语协调工作线程执行
* 实现能够反映任务图依赖关系的任务调度器
* 理解工作负载特性，以便做出高效的任务调度决策

我们建议你复习一下我们的 [C++ 同步教程](tutorial/README.md)，以获取更多关于 C++ 标准库中同步原语的信息。另外，查看一下 [测试用例说明](tests/) 也会很有帮助，这能帮助你理解你的库需要支持哪些类型的工作负载。

### 等等，我是不是以前做过这个？ ###

你可能已经在 CS107 或 CS111 之类的课程中实现过线程池和任务执行库。
不过，这次作业提供了一个独特的机会，让你更深入地理解这些系统。
你将实现多个任务执行库，有些不使用线程池，有些则使用不同类型的线程池。
通过实现多种任务调度策略，并比较它们在不同工作负载下的性能，你将更好地理解在构建并行系统时一些关键设计选择的影响。

## 环境配置 ##

**我们将在 Amazon AWS 的 `c7g.4xlarge` 实例上对本作业进行评分。我们在[这里](https://github.com/stanford-cs149/asst2/blob/master/cloud_readme.md)提供了配置 VM 的说明。请确保你的代码能在该 VM 上运行，因为我们会用它进行性能测试和评分。**

作业起始代码可在 [Github](https://github.com/stanford-cs149/asst2) 获取。请从以下地址下载作业 2 的 starter code：

    https://github.com/stanford-cs149/asst2/archive/refs/heads/master.zip

**重要：** 不要修改提供的 `Makefile`。否则可能会破坏我们的评分脚本。

## Part A：同步批量任务启动

在作业 1 中，你使用了 ISPC 的任务启动原语来启动 N 个 ISPC 任务实例（`launch[N] myISPCFunction()`）。在本作业的第一部分中，你将在自己的任务执行库中实现类似功能。

开始之前，请先熟悉 `itasksys.h` 中对 `ITaskSystem` 的定义。这个[抽象类](https://www.tutorialspoint.com/cplusplus/cpp_interfaces.htm)定义了你的任务执行系统接口。该接口包含一个 `run()` 方法，签名如下：

    virtual void run(IRunnable* runnable, int num_total_tasks) = 0;

`run()` 会执行指定任务的 `num_total_tasks` 个实例。由于一次函数调用会导致许多任务被执行，我们把每次调用 `run()` 称为一次_批量任务启动（bulk task launch）_。

起始代码中的 `tasksys.cpp` 提供了 `TaskSystemSerial::run()` 的一个正确但串行的实现，作为任务系统如何使用 `IRunnable` 接口执行批量任务启动的示例。（`IRunnable` 的定义在 `itasksys.h` 中。）请注意，在每次调用 `IRunnable::runTask()` 时，任务系统都会向任务提供当前任务标识符（一个介于 0 和 `num_total_tasks` 之间的整数）以及本次批量任务启动中的任务总数。任务实现会使用这些参数来决定自己应该执行哪一部分工作。

`run()` 的一个重要细节是：相对于调用线程，它必须同步地执行任务。换句话说，当 `run()` 返回时，应用程序应当被保证本次批量任务启动中的****所有任务****都已经执行完毕。起始代码提供的串行版 `run()` 在调用线程上执行所有任务，因此满足这一要求。

### 运行测试 ###

起始代码包含一套使用你的任务系统的测试程序。关于测试框架中各测试的说明，请参见 `tests/README.md`；关于测试定义本身，请参见 `tests/tests.h`。要运行某个测试，请使用 `runtasks` 脚本。例如，要运行名为 `mandelbrot_chunked` 的测试，它会通过一次批量启动来计算 Mandelbrot 分形图像，其中每个任务处理图像的一段连续区域，可以输入：

```bash
./runtasks -n 16 mandelbrot_chunked
```

不同测试具有不同的性能特征。有些测试每个任务工作量很小，有些则需要大量计算。有些测试每次启动会创建大量任务，有些则很少。有时一次启动中的所有任务计算成本都相近；而有时同一批量启动中各任务的开销差异很大。我们已经在 `tests/README.md` 中描述了大部分测试，但仍然鼓励你阅读 `tests/tests.h` 中的代码，以更详细地理解所有测试的行为。

> [!TIP]
> 在你实现解决方案时，有一个可能对调试正确性很有帮助的测试是 `simple_test_sync`。这是一个非常小的测试，不适合用于衡量性能，但足够小，适合用打印语句或调试器进行调试。参见 `tests/tests.h` 中的 `simpleTest` 函数。


我们鼓励你编写自己的测试。可以参考 `tests/tests.h` 中已有的测试获取灵感。我们也为你提供了一个由 `class YourTask` 和函数 `yourTest()` 组成的骨架测试，供你在需要时扩展。对于你自己创建的测试，请务必把它们添加到 `tests/main.cpp` 中的测试列表和测试名称列表里，并相应调整变量 `n_tests`。请注意，虽然你可以用自己的实现来运行你新增的测试，但你无法编译参考实现来运行这些自定义测试。

命令行选项 `-n` 用于指定任务系统实现最多可以使用多少线程。在上面的例子中，我们选择了 `-n 16`，因为 AWS 实例上的 CPU 具有 16 个执行上下文。所有可运行测试的完整列表可通过命令行帮助（`-h` 选项）查看。

命令行选项 `-i` 用于指定在性能测量期间重复运行测试的次数。为了获得更准确的性能测量，`./runtasks` 会多次运行测试，并记录多次运行中的_最小_耗时。通常默认值已经足够；更大的值可能会得到更准确的结果，但代价是测试运行时间更长。

此外，我们还提供了用于性能评分的测试框架：

```bash
>>> python3 ../tests/run_test_harness.py
```

该框架支持如下命令行参数：

```bash
>>> python3 run_test_harness.py -h
usage: run_test_harness.py [-h] [-n NUM_THREADS]
                           [-t TEST_NAMES [TEST_NAMES ...]] [-a]

Run task system performance tests

optional arguments:
  -h, --help            show this help message and exit
  -n NUM_THREADS, --num_threads NUM_THREADS
                        Max number of threads that the task system can use. (16
                        by default)
  -t TEST_NAMES [TEST_NAMES ...], --test_names TEST_NAMES [TEST_NAMES ...]
                        List of tests to run
  -a, --run_async       Run async tests
```

它会生成一份详细的性能报告，类似这样：

```bash
>>> python3 ../tests/run_test_harness.py -t super_light super_super_light
python3 ../tests/run_test_harness.py -t super_light super_super_light
================================================================================
Running task system grading harness... (2 total tests)
  - Detected CPU with 16 execution contexts
  - Task system configured to use at most 16 threads
================================================================================
================================================================================
Executing test: super_super_light...
Reference binary: ./runtasks_ref_linux
Results for: super_super_light
                                        STUDENT   REFERENCE   PERF?
[Serial]                                9.053     9.022       1.00  (OK)
[Parallel + Always Spawn]               8.982     33.953      0.26  (OK)
[Parallel + Thread Pool + Spin]         8.942     12.095      0.74  (OK)
[Parallel + Thread Pool + Sleep]        8.97      8.849       1.01  (OK)
================================================================================
Executing test: super_light...
Reference binary: ./runtasks_ref_linux
Results for: super_light
                                        STUDENT   REFERENCE   PERF?
[Serial]                                68.525    68.03       1.01  (OK)
[Parallel + Always Spawn]               68.178    40.677      1.68  (NOT OK)
[Parallel + Thread Pool + Spin]         67.676    25.244      2.68  (NOT OK)
[Parallel + Thread Pool + Sleep]        68.464    20.588      3.33  (NOT OK)
================================================================================
Overall performance results
[Serial]                                : All passed Perf
[Parallel + Always Spawn]               : Perf did not pass all tests
[Parallel + Thread Pool + Spin]         : Perf did not pass all tests
[Parallel + Thread Pool + Sleep]        : Perf did not pass all tests
```

在上述输出中，`PERF` 是你的实现运行时间与参考实现运行时间之比。因此，小于 1 的值表示你的任务系统实现比参考实现更快。

> [!TIP]
> Mac 用户请注意：虽然我们为 Part A 和 Part B 都提供了参考实现二进制文件，但我们将使用 Linux 版本二进制文件来测试你的代码。因此，我们建议你在提交前先在 AWS 实例上检查你的实现。如果你使用的是较新的 M1 芯片 Mac，本地测试时请使用 `runtasks_ref_osx_arm`。否则请使用 `runtasks_ref_osx_x86`。

> [!IMPORTANT]
> 我们会在 AWS 上使用参考实现的 `runtasks_ref_linux_arm` 版本为你的作业评分。请确保你的解答能在 AWS ARM 实例上正确运行。

### 你需要做什么 ###

你的任务是实现一个能够高效利用多核 CPU 的任务执行引擎。我们会同时根据正确性（必须正确执行所有任务）和性能为你的实现评分。这会是一次很有趣的编程挑战，但它并不是一项简单的工作。为了帮助你循序渐进地完成 Part A，我们会让你实现多个版本的任务系统，逐步提升复杂度和性能。你的三个实现将位于 `tasksys.cpp/.h` 中定义的以下类中：

* `TaskSystemParallelSpawn`
* `TaskSystemParallelThreadPoolSpinning`
* `TaskSystemParallelThreadPoolSleeping`

__请在 `part_a/` 子目录中实现你的 Part A 版本，以便与正确的参考实现（`part_a/runtasks_ref_*`）进行比较。__

_小提示：请注意，下面的说明采用的是“先尝试最简单的改进”的思路。每一步都会提高任务执行系统实现的复杂度，但在每个阶段，你都应该拥有一个可以正常工作的（完全正确的）任务运行时系统。_

我们还要求你至少创建一个测试，它可以用于验证正确性或性能。更多信息请参见上面的“运行测试”部分。

#### 第一步：迁移到并行任务系统 ####

__在这一步中，请实现类 `TaskSystemParallelSpawn`。__

起始代码提供了一个可工作的串行任务系统实现 `TaskSystemSerial`。在这一步中，你将扩展起始代码，使其能够并行执行一次批量任务启动。

* 你需要创建额外的控制线程来完成一次批量任务启动中的工作。请注意，`TaskSystem` 的构造函数提供了参数 `num_threads`，它表示你的实现最多可以使用的****工作线程数量上限****。

* 本着“先做最简单的事情”的精神，我们建议你在 `run()` 开始时创建工作线程，并在 `run()` 返回之前由主线程对这些线程执行 `join`。这是一个正确的实现，但由于频繁创建线程，会带来显著的额外开销。

* 你将如何把任务分配给工作线程？你是否应该考虑静态分配还是动态分配？

* 是否存在需要防止多个线程同时访问的共享变量（也就是任务执行系统的内部状态）？

#### 第二步：使用线程池避免频繁创建线程 ####

__在这一步中，请实现类 `TaskSystemParallelThreadPoolSpinning`。__

你在第一步中的实现会在每次调用 `run()` 时因为创建线程而带来开销。当任务计算代价较低时，这种开销尤其明显。此时，我们建议你转向“线程池”实现，即在一开始就创建所有工作线程（例如在 `TaskSystem` 构造时，或第一次调用 `run()` 时）。

* 作为起始实现，我们建议你让工作线程持续循环，不断检查是否有更多工作需要执行。（线程进入 `while` 循环并持续等待某个条件为真，通常称为“自旋（spinning）”。）工作线程应当如何判断是否有任务可做？

* 现在，要保证 `run()` 具有所要求的同步行为就不再那么简单了。你需要如何修改 `run()` 的实现，才能判断一次批量任务启动中的所有任务都已完成？

#### 第三步：在线程无事可做时让它们休眠 ####

__在这一步中，请实现类 `TaskSystemParallelThreadPoolSleeping`。__

第二步实现的一个缺点是：线程在“自旋”等待工作时会占用 CPU 核心的执行资源。例如，工作线程可能会循环等待新任务到来；主线程也可能会循环等待工作线程完成所有任务，以便从 `run()` 调用中返回。由于这些线程虽然在运行却并未做有用工作，这会影响整体性能。

在这一部分中，我们希望你通过让线程在等待条件满足时休眠，来提升任务系统的效率。

* 你的实现可以选择使用条件变量（condition variables）来实现这种行为。条件变量是一种同步原语，它允许线程在等待某个条件成立时进入休眠状态（从而不占用 CPU 处理资源）。其他线程会“通知”这些等待中的线程唤醒，并检查它们等待的条件是否已经满足。例如，当没有工作可做时，可以让工作线程休眠（这样它们就不会抢占真正干活的线程的 CPU 资源）。又例如，调用 `run()` 的主应用线程也可能希望在等待一次批量任务启动中的所有任务都完成时进入休眠。（否则，自旋等待的主线程会抢占工作线程的 CPU 资源！）

* 你在这一部分的实现中可能会遇到棘手的竞态条件。你需要考虑线程行为的多种可能交错执行情况。

* 你可能需要考虑编写额外的测试用例来验证系统。__起始代码中包含了评分脚本将用于性能评分的工作负载，但我们还会使用更广泛的一组、并未在 starter code 中提供的工作负载来测试你实现的正确性！__

## Part B：支持执行任务图

在 Part B 中，你将扩展 Part A 中的任务系统实现，使其支持异步启动那些可能依赖于之前任务的任务。这些任务间依赖会形成你的任务执行库必须遵守的调度约束。

`ITaskSystem` 接口还包含一个额外的方法：

    virtual TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                    const std::vector<TaskID>& deps) = 0;

`runAsyncWithDeps()` 与 `run()` 类似，也用于执行一次包含 `num_total_tasks` 个任务的批量启动。不过，它与 `run()` 有几个关键不同点……

#### 异步任务启动 ####

首先，通过 `runAsyncWithDeps()` 创建的任务是相对于调用线程_异步_执行的。这意味着 `runAsyncWithDeps()` 应当_立即_向调用者返回，即使这些任务尚未执行完成。该方法会返回一个与本次批量任务启动关联的唯一标识符。

调用线程可以通过调用 `sync()` 来判断该批量任务启动何时真正完成。

    virtual void sync() = 0;

`sync()` 会在__所有此前批量任务启动所关联的任务都完成之后__才向调用者返回。例如，请看下面的代码：

    // assume taskA and taskB are valid instances of IRunnable...

    std::vector<TaskID> noDeps;  // empty vector

    ITaskSystem *t = new TaskSystem(num_threads);

    // bulk launch of 4 tasks
    TaskID launchA = t->runAsyncWithDeps(taskA, 4, noDeps);

    // bulk launch of 8 tasks
    TaskID launchB = t->runAsyncWithDeps(taskB, 8, noDeps);

    // at this point tasks associated with launchA and launchB
    // may still be running

    t->sync();

    // at this point all 12 tasks associated with launchA and launchB
    // are guaranteed to have terminated

如上面注释所述，在调用线程调用 `sync()` 之前，不能保证此前 `runAsyncWithDeps()` 调用所创建的任务已经执行完成。更准确地说，`runAsyncWithDeps()` 是通知你的任务系统“执行一个新的批量任务启动”，但你的实现可以灵活决定在下一次调用 `sync()` 之前的任意时刻执行这些任务。请注意，这意味着你的实现并不保证一定会先完成 `launchA` 的任务再开始执行 `launchB`！

#### 显式依赖支持 ####

`runAsyncWithDeps()` 的第二个关键细节是它的第三个参数：一个 `TaskID` 标识符向量，其中的每个标识符都必须引用此前通过 `runAsyncWithDeps()` 启动的批量任务。这个向量指定了当前批量任务启动所依赖的先前任务。__因此，在依赖向量中列出的所有启动都完成之前，你的任务运行时不能开始执行当前批量任务启动中的任何任务！__ 例如，请看下面这个例子：

    std::vector<TaskID> noDeps;  // empty vector
    std::vector<TaskID> depOnA;
    std::vector<TaskID> depOnBC;

    ITaskSystem *t = new TaskSystem(num_threads);

    TaskID launchA = t->runAsyncWithDeps(taskA, 128, noDeps);
    depOnA.push_back(launchA);

    TaskID launchB = t->runAsyncWithDeps(taskB, 2, depOnA);
    TaskID launchC = t->runAsyncWithDeps(taskC, 6, depOnA);
    depOnBC.push_back(launchB);
    depOnBC.push_back(launchC);

    TaskID launchD = t->runAsyncWithDeps(taskD, 32, depOnBC);
    t->sync();

上面的代码包含四次批量任务启动（taskA：128 个任务，taskB：2 个任务，taskC：6 个任务，taskD：32 个任务）。请注意，taskB 和 taskC 的启动都依赖于 taskA。taskD 的批量启动（`launchD`）则同时依赖于 `launchB` 和 `launchC` 的结果。因此，虽然你的任务运行时可以按任意顺序处理 `launchB` 和 `launchC` 所关联的任务（包括并行处理），但这两次启动中的所有任务都必须在 `launchA` 完成之后才能开始执行，并且必须在你的运行时开始执行 `launchD` 中任何任务之前全部完成。

我们可以将这些依赖关系直观地表示为一个__任务图（task graph）__。任务图是一个有向无环图（DAG），图中的节点对应批量任务启动，从节点 X 指向节点 Y 的边表示 Y 依赖于 X 的输出。上面这段代码对应的任务图如下：

<p align="center">
    <img src="figs/task_graph.png" width=400>
</p>

请注意，如果你在一台具有 8 个执行上下文的 Myth 机器上运行上面的例子，那么能够并行调度 `launchB` 和 `launchC` 中的任务可能会非常有用，因为这两个批量任务启动中的任何一个单独来看都不足以充分利用整台机器的所有执行资源。

### 测试 ###

所有以后缀 `Async` 结尾的测试都应用于测试 Part B。评分框架中包含的测试子集在 `tests/README.md` 中有说明；所有测试都可以在 `tests/tests.h` 中找到，并列在 `tests/main.cpp` 中。为了调试正确性，我们提供了一个小测试 `simple_test_async`。请查看 `tests/tests.h` 中的 `simpleTest` 函数。`simple_test_async` 足够小，适合在 `simpleTest` 内部用打印语句或断点进行调试。

我们鼓励你编写自己的测试。可以参考 `tests/tests.h` 中已有的测试获取灵感。我们也为你提供了一个由 `class YourTask` 和函数 `yourTest()` 组成的骨架测试，供你在需要时扩展。对于你自己创建的测试，请务必把它们添加到 `tests/main.cpp` 中的测试列表和测试名称列表里，并相应调整变量 `n_tests`。请注意，虽然你可以用自己的实现来运行你新增的测试，但你无法编译参考实现来运行这些自定义测试。

### 你需要做什么 ###

你必须扩展你在 Part A 中使用线程池（且支持休眠）的任务系统实现，正确实现 `TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps()` 和 `TaskSystemParallelThreadPoolSleeping::sync()`。我们也要求你至少创建一个测试，它可以用于验证正确性或性能。更多信息请参见上面的“测试”部分。补充说明一下：你*需要*在 writeup 中描述你自己写的测试，但我们的自动评分器*不会*直接测试你的测试本身。
**你不需要在 Part B 中实现其他 `TaskSystem` 类。**

和 Part A 一样，我们给出以下提示帮助你开始：
* 你可以把 `runAsyncWithDeps()` 的行为理解为：向一个“工作队列”中压入一条与该批量任务启动对应的记录，或者压入多条分别对应于该批量启动中每个任务的记录。一旦表示待执行工作的记录进入队列，`runAsyncWithDeps()` 就可以向调用者返回。

* 这一部分的关键在于：你需要做合适的 bookkeeping（状态记录）来追踪依赖关系。当一次批量任务启动中的所有任务完成时，必须做什么？（这正是新的任务可能变为可执行的时候。）

* 在你的实现中，拥有两个数据结构可能会很有帮助：
  (1) 一个表示已经通过 `runAsyncWithDeps()` 添加到系统中、但由于依赖的任务仍在运行而尚未准备好执行的任务结构（这些任务正在“等待”其他任务完成）；
  (2) 一个“就绪队列（ready queue）”，其中的任务不再等待任何先前任务完成，一旦有工作线程可用就可以安全执行。

* 在生成唯一任务启动 ID 时，你无需担心整数回绕问题。我们不会用超过 2^31 次批量任务启动来测试你的系统。

* 你可以假设所有程序要么只调用 `run()`，要么只调用 `runAsyncWithDeps()`；也就是说，你不需要处理一种情况：某个 `run()` 调用需要等待此前所有 `runAsyncWithDeps()` 调用结束。请注意，这一假设意味着你可以通过适当地调用 `runAsyncWithDeps()` 和 `sync()` 来实现 `run()`。

* 你可以假设唯一的多线程行为来自你的实现所创建/使用的多个线程。也就是说，我们不会再额外创建线程并从那些线程中调用你的实现。

__请在 `part_b/` 子目录中实现你的 Part B 版本，以便与正确的参考实现（`part_b/runtasks_ref_*`）进行比较。__

## 评分 ##

本次作业的分值分配如下：

**Part A（50 分）**
- `TaskSystemParallelSpawn::run()` 的正确性 5 分 + 性能 5 分。（共 10 分）
- `TaskSystemParallelThreadPoolSpinning::run()` 和 `TaskSystemParallelThreadPoolSleeping::run()` 的正确性各 10 分，性能各 10 分。（共 40 分）

**Part B（40 分）**
- `TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps()`、`TaskSystemParallelThreadPoolSleeping::run()` 和 `TaskSystemParallelThreadPoolSleeping::sync()` 的正确性共 30 分
- `TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps()`、`TaskSystemParallelThreadPoolSleeping::run()` 和 `TaskSystemParallelThreadPoolSleeping::sync()` 的性能共 10 分。对于 Part B，你可以忽略 `Parallel + Always Spawn` 和 `Parallel + Thread Pool + Spin` 的结果。也就是说，对每个测试用例，你只需要通过 `Parallel + Thread Pool + Sleep`。

**Writeup（10 分）**
- 更多细节请参考下方 “Handin” 部分。

对于每个测试，只要实现的性能在参考实现的 20%（Part A）和 50%（Part B）范围内，就能拿到满额性能分。只有在实现返回正确答案的前提下，才会授予性能分。正如前面提到的，我们还可能使用一组更广泛、未在 starter code 中提供的工作负载来测试你实现的_正确性_。

## 提交 ##

请通过 [Gradescope](https://www.gradescope.com/) 提交你的作业。你的提交应当同时包含任务系统代码，以及一份描述你实现方式的 writeup。我们期望你提交以下五个文件：

 * part_a/tasksys.cpp
 * part_a/tasksys.h
 * part_b/tasksys.cpp
 * part_b/tasksys.h
 * 你的 write-up PDF（提交到 gradescope 的 write-up 作业）

#### 代码提交 ####

我们要求你将源文件 `part_a/tasksys.cpp|.h` 和 `part_b/tasksys.cpp|.h` 打包为一个压缩文件提交。你可以创建一个目录（例如命名为 `asst2_submission`），其中包含子目录 `part_a` 和 `part_b`，把相关文件放进去，然后运行 `tar -czvf asst2.tar.gz asst2_submission` 来压缩该目录并上传。请将**压缩文件** `asst2.tar.gz` 提交到 Gradescope 上的作业 *Assignment 2 (Code)*。

在提交源代码之前，请确保所有代码都可以编译并运行！我们应当能够将这些文件放入一份干净的 starter code 树中，输入 `make`，然后无需手动干预即可执行你的程序。

我们的评分脚本会运行 starter code 中提供给你的检查程序，以确定性能得分。_我们还会在 starter code 未提供的其他应用程序上运行你的代码，以进一步测试其正确性！_ 评分脚本会在作业截止之后运行。

#### Writeup 提交 ####

请将一份简短的 writeup 提交到 Gradescope 上的作业 *Assignment 2 (Write-up)*，内容需回答以下问题：

 1. 描述你的任务系统实现（1 页左右即可）。除了总体说明其工作方式之外，请务必回答以下问题：
  * 你是如何管理线程的？（例如，你是否实现了线程池？）
  * 你的系统如何把任务分配给工作线程？你使用的是静态分配还是动态分配？
  * 在 Part B 中，你是如何追踪依赖关系以确保任务图被正确执行的？

 2. 在 Part A 中，你可能已经注意到，更简单的任务系统实现（例如，完全串行的实现，或者每次启动都新建线程的实现）有时会和更高级的实现一样快，甚至更快。请结合具体测试示例解释为什么会这样。例如，在什么情况下串行任务系统表现最好？为什么？在什么情况下“每次启动都创建线程”的实现会和使用线程池的更高级并行实现一样好？它又在什么时候表现不好？
 3. 描述一个你为本次作业实现的测试。这个测试做了什么、它旨在检查什么、你如何验证自己的解答在该测试上表现良好？你新增测试的结果是否促使你修改了作业实现？
