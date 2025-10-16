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

    //This logic has no point right now as I am not populating the wait_q TODO convert this into a function
    bool dependant = 0;
    std::vector<int> task_index_remove;
    for (WorkerQ temp_worker : wait_q){
        dependant = checkForDependency(ready_q, *temp_worker.deps);
        if (!dependant){
            ready_q.push_back(temp_worker);
            // printf("Task id %d moved from wait to ready - task_id %d \n", wait_q.front().task_id, ready_q.front().task_id);
            task_index_remove.push_back(temp_worker.task_id);
            //TODO: need to remove these task_ids frpm wait q or just store the index and then remove them
        }
    }
    if (task_index_remove.size() != 0) {
        for (int idx : task_index_remove){
            auto it = wait_q.begin();
            std::advance(it, idx);
            wait_q.erase(it);//TODO this is no tcorrect will give the wrong result
        }
    }
    //No point logic right now end

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
    this->myWorker = {-1, nullptr, 0, -1, 0, {}};

    // All threads sleep until work is available
    for (int i = 0; i < this->numThreads; i++) {
        workers[i] = std::thread([&, i]{
            int ind = -1;
            while (this->runThreads){
                // Request mutex
                std::unique_lock<std::mutex> lk(*this->mutex_);

                // Increment tasksDone if previous iteration completed task
                if (ind >= 0) {
                    this->myWorker.num_tasks_finished++;
                    // Check whether this task launch is fully completed
                    if (this->myWorker.num_tasks_finished == this->myWorker.total_num_tasks) {
                        ready_q.pop_front();

                        updateQs();

                        if (ready_q.empty()) {
                            this->myWorker = {-1, nullptr, 0, -1, 0, {}};
                            lk.unlock();
                            this->tasks_done_cond_->notify_all();
                            lk.lock();
                        } else {
                            this->myWorker = ready_q.front();
                            // printf("myWorker task_id is %d \n",myWorker.task_id);
                        }
                    }
                }

                // Poll for new work available
                while (this->myWorker.num_tasks_in_process < 0 && this->runThreads) {
                    this->work_avail_cond_->wait(lk);
                }

                // Assign task index to run
                ind = this->myWorker.num_tasks_in_process--;
                lk.unlock();

                // If work available, run task
                if (ind >= 0) {
                    this->myWorker.runnable->runTask(ind, this->myWorker.total_num_tasks);
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

    this->mutex_->lock();
    // Check for any matches in dependency list in ready_q or wait_q
    // bool dependencyFound = checkForDependency(ready_q, deps);//TODO see why this is failing
    bool dependencyFound =0;

    // Assign to queue
    if (dependencyFound){
        // printf("Task id %d pushed to wait \n", newTask.task_id);
        wait_q.push_back(newTask);
    }
    else {
        ready_q.push_back(newTask);
        // printf("Task id %d pushed to ready \n", newTask.task_id);
        if (ready_q.size() == 1)
            this->myWorker = ready_q.front();
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

    // Once awake, finish run()
    this->myWorker.runnable = nullptr;
    
    lk.unlock();

    return;
}
