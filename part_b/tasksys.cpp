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

 bool checkForDependency(std::list<WorkerQ> queue, WorkerQ currentWorker){
    if (queue.empty() || currentWorker.deps.empty()) return 0;
    for (auto dep_task_id_ : currentWorker.deps){
        for (WorkerQ temp_worker : queue){
            if (dep_task_id_ == temp_worker.task_id) return 1;
        }
    }
    return 0;
}


const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}


//new
void TaskSystemParallelThreadPoolSleeping::updateQs(){

    if (wait_q.empty()) return;
    std::vector<WorkerQ> task_index_remove;
    std::list<WorkerQ> task_index_not_remove = ready_q;
    
    for (WorkerQ temp_worker : wait_q){
        if (!checkForDependency(task_index_not_remove, temp_worker)){
            ready_q.push_back(temp_worker);
            task_index_remove.push_back(temp_worker);
        }
        task_index_not_remove.push_back(temp_worker);
        if ( task_index_not_remove.size() > this->numThreads/2) break;
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
    this->tasksDone = 0;
    std::vector<TaskID> deps = {};
    this->myWorker = {-1, nullptr, 0, -1, 0, deps};//this just holds the dummy that we want it to hold to start

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
                        my_worker_q_ = &this->myWorker;
                        this->tasksDone++;
                        if (this->tasksDone == this->taskId) {
                            lk.unlock();
                            this->tasks_done_cond_->notify_all();
                            lk.lock();
                        } else {
                            updateQs();
                        }
                    }
                }

                // Poll for new work available
                ind = -1;
                while (ready_q.empty() && this->runThreads) {
                    this->work_avail_cond_->wait(lk);
                }

                // Assign task index to run
                for (WorkerQ& temp_worker : ready_q) {
                    if (temp_worker.num_tasks_in_process >= 0) {
                        my_worker_q_ = &temp_worker;
                        ind = my_worker_q_->num_tasks_in_process--;
                        break;
                    }
                }
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

    std::vector<TaskID> deps;
    runAsyncWithDeps(runnable, num_total_tasks, deps);

    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    // Prepare new entry to work queues
    WorkerQ newTask;
    newTask.runnable = runnable;
    newTask.total_num_tasks = num_total_tasks;
    newTask.num_tasks_in_process = num_total_tasks-1;
    newTask.num_tasks_finished = 0;
    newTask.deps = deps;

    // Assign to queue
    this->mutex_->lock(); 
    newTask.task_id = this->taskId++;
    if (this->tasksDone == this->taskId-1 || deps.size() == 0){
        ready_q.push_back(newTask);
    } else {
        wait_q.push_back(newTask);
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
    while (this->tasksDone < this->taskId) {
        this->tasks_done_cond_->wait(lk);
    }
    
    lk.unlock();

    return;
}
