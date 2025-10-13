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
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
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
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->numThreads = num_threads;
    this->mutex_ = new std::mutex();
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {
    delete this->mutex_;
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    std::thread workers[this->numThreads];
    this->index_ = 0;
    this->my_counter = 0;


    //Dynamic with mutexes
    // for (int i = 0; i < this->numThreads; i++) {
    //     workers[i] = std::thread([&]{
    //         while (true){
    //             this->mutex_->lock();
    //             int ind = this->index_++;
    //             this->mutex_->unlock();
    //             if (ind >= num_total_tasks){break;}
    //             // printf("threadID = %d, index = %d \n",i, ind);
    //             runnable->runTask(ind, num_total_tasks);
    //         }
    //     });
    // }

    //Dynamic with atomic variable - slightly better but does not matter
    for (int i = 0; i < this->numThreads; i++) {
        workers[i] = std::thread([&]{
            while (true){
                int ind = this->my_counter.fetch_add(1);
                if (ind >= num_total_tasks){break;}
                // printf("threadID = %d, index = %d \n",i, ind);
                runnable->runTask(ind, num_total_tasks);
            }
        });
    }


    for (int i = 0; i < this->numThreads; i++) {
        workers[i].join();
    }


}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
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
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->numThreads = num_threads;
    workers = new std::thread[num_threads];
    this->runThreads = 1;
    this->numTasks = -1;
    this->mutex_ = new std::mutex();
    this->taskRunnable = nullptr;

    for (int i = 0; i < this->numThreads; i++) {
        workers[i] = std::thread([&]{
            while (this->runThreads){
                int ind = -1;
                // Check if any work is available
                this->mutex_->lock();
                if (this->numTasks >= 0)
                    ind = this->numTasks--;
                this->mutex_->unlock();
                // If work available, run task
                if (ind >= 0) {
                    taskRunnable->runTask(ind, this->totalTasks);
                    this->tasksDone.fetch_add(1);
                }
            }
        });
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    this->runThreads = 0;
    for (int i = 0; i < this->numThreads; i++) {
        workers[i].join();
    }
    delete this->mutex_;
    delete[] workers;
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    this->mutex_->lock();
    this->tasksDone = 0;
    this->totalTasks = num_total_tasks;
    this->taskRunnable = runnable;
    this->numTasks = num_total_tasks-1;
    this->mutex_->unlock();

    // Once each thread finishes a task, it increments the number of tasksDone
    while(this->tasksDone < this->totalTasks){};//spinning here
    this->mutex_->lock();
    this->taskRunnable = nullptr;
    this->mutex_->unlock();
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
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
    this->numTasks = -1;
    this->mutex_ = new std::mutex();
    this->condition_variable_ = new std::condition_variable();
    this->tasksDone = 0;
    this->totalTasks = 0;
    this->threadsDone = 0;
    this->thread_mutex_ = new std::mutex();

    // Thread 0: signals other threads when work is ready
    workers[0] = std::thread([&]{
        while (this->runThreads){
            int ind = -2;
            // Check if any work is available
            this->mutex_->lock();
            if (this->numTasks >= 0)
                ind = this->numTasks--;
            this->mutex_->unlock();
            // If this is the first task, wake up other threads
            if (ind == this->totalTasks-1) {
                this->condition_variable_->notify_all();
            }
            // If work available, run task
            if (ind >= 0) {
                taskRunnable->runTask(ind, this->totalTasks);
                this->tasksDone.fetch_add(1);
            }
        }
    });

    // All other threads wait for signal from Thread 0
    for (int i = 1; i < this->numThreads; i++) {
        workers[i] = std::thread([&, i]{
            while (this->runThreads){
                int ind = -1;
                // Check if any work is available
                this->mutex_->lock();
                if (this->numTasks >= 0)
                    ind = this->numTasks--;
                this->mutex_->unlock();
                // If work available, run task; else, put thread to sleep
                if (ind >= 0) {
                    taskRunnable->runTask(ind, this->totalTasks);
                    this->tasksDone.fetch_add(1);
                } else {
                    std::unique_lock<std::mutex> lk(*this->thread_mutex_);
                    this->threadsDone.fetch_add(1);
                    this->condition_variable_->wait(lk);
                    lk.unlock();
                }
            }
        });
    }
    while (this->threadsDone < num_threads-1) {};
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->runThreads = 0;
    this->condition_variable_->notify_all();
    for (int i = 0; i < this->numThreads; i++) {
        workers[i].join();
    }
    delete this->mutex_;
    delete this->condition_variable_;
    delete this->thread_mutex_;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    this->tasksDone = 0;
    this->threadsDone = 0;
    this->totalTasks = num_total_tasks;
    this->taskRunnable = runnable;
    this->numTasks = num_total_tasks-1;

    // Once each thread finishes a task, it increments the number of tasksDone
    while(this->tasksDone < this->totalTasks || this->threadsDone < this->numThreads-1){};
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
