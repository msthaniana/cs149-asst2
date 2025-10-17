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

 bool checkForDependency(std::list<WorkerQ> queue, const std::vector<TaskID>& deps){
    bool dependencyFound = 0;
    if (!queue.empty() && deps.size() != 0){
        for (TaskID dep_task_id_ : deps){
            for (WorkerQ temp_worker : queue){
                if (dep_task_id_ == temp_worker.task_id) dependencyFound = 1;
            }
        }
    }
    return dependencyFound;
}


const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

void TaskSystemParallelThreadPoolSleeping::updateQs(){

    
    std::vector<WorkerQ> task_index_remove;
    for (WorkerQ temp_worker : wait_q){
        if (!checkForDependency(ready_q, *temp_worker.deps)){
            ready_q.push_back(temp_worker);
            task_index_remove.push_back(temp_worker);
        }
    }
    for (auto worker : task_index_remove){ //doing seperately to not bother the for loop
        wait_q.remove(worker);
    }

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
    this->taskId = 0;
    this->myWorker = {-1, nullptr, 0, -1, 0, {}};//this just holds the dummy that we want it to hold to start

    // All threads sleep until work is available
    for (int i = 0; i < this->numThreads; i++) {
        workers[i] = std::thread([&, i]{
            int ind = -1;
            WorkerQ* my_worker_q_ = &this->myWorker;
            while (this->runThreads){
                // Request mutex
                std::unique_lock<std::mutex> lk(*this->mutex_);

                // Increment tasksDone if previous iteration completed task
                if (ind >= 0) {
                    my_worker_q_->num_tasks_finished++;
                    // Check whether this task launch is fully completed
                    if (my_worker_q_->num_tasks_finished == my_worker_q_->total_num_tasks) {
                        ready_q.remove(*my_worker_q_);

                        updateQs();//TODO likely need a Q mutex

                        if (ready_q.empty()) {
                            my_worker_q_ = &this->myWorker;
                            lk.unlock();
                            this->tasks_done_cond_->notify_all();
                            lk.lock();
                        }
                    }
                }

                if (!ready_q.empty()){//select a task to run now.
                    auto it = ready_q.begin();
                    // std::advance(it,(rand()%ready_q.size())); //trying to pick a random value with this - not working //likely failing because of dependancies //TODO i think we would have to do this to get performance
                    my_worker_q_ = &(*it);
                }

                // Poll for new work available
                while (my_worker_q_->num_tasks_in_process < 0 && this->runThreads) {
                    this->work_avail_cond_->wait(lk);
                    if (ready_q.size() == 0 && wait_q.size() == 0) return;
                    break;
                }

                // Assign task index to run
                ind = my_worker_q_->num_tasks_in_process--;


                lk.unlock();

                // If work available, run task
                if (ind >= 0) {
                    my_worker_q_->runnable->runTask(ind, my_worker_q_->total_num_tasks);
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

    // Prepare new entry to work queues
    WorkerQ newTask;
    newTask.task_id = this->taskId++;
    newTask.runnable = runnable;
    newTask.total_num_tasks = num_total_tasks;
    newTask.num_tasks_in_process = num_total_tasks-1;
    newTask.num_tasks_finished = 0;
    newTask.deps = &deps;


    // Assign to queue
    if (!ready_q.empty()){
        // printf("Task id %d pushed to wait \n", newTask.task_id);
        wait_q.push_back(newTask);
    }
    else {//if not this way we would have to check the dependancies in both wait and ready
        ready_q.push_back(newTask);
    }
    this->mutex_->unlock();

    // Wake up threads if they are sleeping
    this->work_avail_cond_->notify_all();

    // Return taskID assigned to this task
    return newTask.task_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    // Put run() to sleep until all tasks are done
    std::unique_lock<std::mutex> lk(*this->mutex_);
    while (ready_q.size() > 0 || wait_q.size() > 0) {
        this->tasks_done_cond_->wait(lk);
    }
    
    lk.unlock();

    return;
}
