#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->numThreads = num_threads;
    workers = new std::thread[num_threads];
    this->runThreads = 1;
    // this->numTasks = -1;
    this->mutex_ = new std::mutex();
    this->work_avail_cond_ = new std::condition_variable();
    this->tasks_done_cond_ = new std::condition_variable();
    this->tasksDone = 0;
    // this->totalTasks = 0;
    this->threadsDone = 0;
    // this->taskRunnable = nullptr;
    this->taskId = 0;

    std::vector<TaskID>* deps = nullptr;

    wait_q.push(WorkerQ{-1, nullptr, 0, -1, *deps});
    ready_q.push(WorkerQ{-1, nullptr, 0, -1, *deps});


    // Thread 0: signals other threads when work is ready
    workers[0] = std::thread([&]{
        while (this->runThreads){
            int ind = -1;

            // Check if any work is available
            this->mutex_->lock();
            if (ready_q.front().num_tasks >= 0)
                ind = ready_q.front().num_tasks--;
            this->mutex_->unlock();

            // If work available, run task
            if (ind >= 0) {
                if (ind > 0) this->work_avail_cond_->notify_all();
                ready_q.front().runnable->runTask(ind, ready_q.front().total_num_tasks);
                this->tasksDone.fetch_add(1);
                // Notify caller function if done condition is met
                if (this->tasksDone.load() == ready_q.front().total_num_tasks){
                    this->tasks_done_cond_->notify_all();
                }
            }
        }
    });

    // All other threads wait for signal from Thread 0
    for (int i = 1; i < this->numThreads; i++) {
        workers[i] = std::thread([&, i]{
            std::unique_lock<std::mutex> lk(*this->mutex_);
            lk.unlock();
            while (this->runThreads){
                int ind = -1;

                // Check if any work is available
                lk.lock();
                this->threadsDone.fetch_add(1);
                while (ready_q.front().num_tasks < 0 && this->runThreads) {
                    this->work_avail_cond_->wait(lk);
                }
                this->threadsDone.fetch_sub(1);
                ind = ready_q.front().num_tasks--;
                lk.unlock();

                // If work available, run task
                if (ind >= 0) {
                    ready_q.front().runnable->runTask(ind, ready_q.front().total_num_tasks);
                    this->tasksDone.fetch_add(1);
                    // Notify caller function if done condition is met
                    if (this->tasksDone.load() == ready_q.front().total_num_tasks){
                        this->tasks_done_cond_->notify_all();
                    }

                }
            }
        });
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->mutex_->lock();
    this->runThreads = 0;
    this->mutex_->unlock();
    this->work_avail_cond_->notify_all();
    for (int i = 0; i < this->numThreads; i++) {
        workers[i].join();
    }
    delete this->mutex_;
    delete this->work_avail_cond_;
    delete this->tasks_done_cond_;
    delete[] workers;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    std::vector<TaskID>* deps = nullptr;
    runAsyncWithDeps(runnable, num_total_tasks, *deps);

    sync();

    
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    std::unique_lock<std::mutex> lk(*this->mutex_);
    this->lk_main_thread = &lk;
    this->tasksDone = 0;

    //this all is not needed
    // this->totalTasks = num_total_tasks;
    // this->taskRunnable = runnable;
    // this->numTasks = num_total_tasks-1;
    //this all is not needed anymore

    wait_q.push(WorkerQ{this->taskId++, runnable, num_total_tasks, num_total_tasks-1, deps});
    ready_q.push(WorkerQ{this->taskId++, runnable, num_total_tasks, num_total_tasks-1, deps});

    //this may not work in this case as we want to return to the caller immediately
    // Put run() to sleep until all tasks are done
    // this->tasks_done_cond_->wait(lk);


    // // Once awake, finish run()
    // ready_q.front().runnable = nullptr;
    // lk.unlock();

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    this->tasks_done_cond_->wait(*this->lk_main_thread);


    // Once awake, finish run()
    ready_q.front().runnable = nullptr;
    lk_main_thread->unlock();

    ready_q.pop();
    wait_q.pop();

    return;
}
