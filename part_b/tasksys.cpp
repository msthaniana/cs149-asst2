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
    this->mutex_ = new std::mutex();
    this->work_avail_cond_ = new std::condition_variable();
    this->tasks_done_cond_ = new std::condition_variable();
    this->tasksDone = 0;
    this->threadsDone = 0;
    this->taskId = 0;

    //this is for the initial condition. Must be popped as soon as there is an actual ready to be processed
    ready_q.push_back({-1, nullptr, 0, -1, {}});
    this->myWorker = ready_q.front();


    // Thread 0: signals other threads when work is ready
    workers[0] = std::thread([&]{
        while (this->runThreads){
            int ind = -1;

            // Check if any work is available
            this->mutex_->lock();
            if (this->myWorker.num_tasks >= 0)
                ind = this->myWorker.num_tasks--;
            this->mutex_->unlock();

            // If work available, run task
            if (ind >= 0) {
                if (ind > 0) this->work_avail_cond_->notify_all();
                this->myWorker.runnable->runTask(ind, this->myWorker.total_num_tasks);
                this->tasksDone.fetch_add(1);
                // Notify caller function if done condition is met
                if (this->tasksDone.load() == this->myWorker.total_num_tasks){
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
                while (this->myWorker.num_tasks < 0 && this->runThreads) {
                    this->work_avail_cond_->wait(lk);
                }
                this->threadsDone.fetch_sub(1);
                ind = this->myWorker.num_tasks--;
                lk.unlock();

                // If work available, run task
                if (ind >= 0) {
                    this->myWorker.runnable->runTask(ind, this->myWorker.total_num_tasks);
                    this->tasksDone.fetch_add(1);
                    // Notify caller function if done condition is met
                    if (this->tasksDone.load() == this->myWorker.total_num_tasks){
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

    this->lk_main_thread = std::unique_lock<std::mutex> {*this->mutex_};
    std::vector<TaskID>* deps = nullptr;
    ready_q.pop_front(); //pop the previous task. Since this is async case
    runAsyncWithDeps(runnable, num_total_tasks, *deps);

    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    this->tasksDone = 0;

    ready_q.push_back({this->taskId++, runnable, num_total_tasks, num_total_tasks-1, &deps});
    this->myWorker = ready_q.front();

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    // Put run() to sleep until all tasks are done
    this->tasks_done_cond_->wait(this->lk_main_thread);

    // // Once awake, finish run()
    this->myWorker.runnable = nullptr;
    
    lk_main_thread.unlock();

    return;
}
