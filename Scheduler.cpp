//
//  Scheduler.cpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

#include <cassert>

#include "Scheduler.hpp"

// variable constants
#define STATE_CHANGE_THRESHOLD 10000000 // 10 seconds

static unsigned active_machines;
static map<MachineId_t, vector<VMId_t>> vms_per_machine;
// static vector<int> checks_since_last_work;

static map<VMId_t, bool> migration;
static map<MachineId_t, bool> stateChange;

static map<VMId_t, vector<TaskId_t>> pendingTasks;
static map<MachineId_t, vector<VMId_t>> pendingVMs;

static map<MachineId_t, unsigned> last_memory_used;
static map<MachineId_t, Time_t> last_task;

using namespace std;

void Scheduler::Init() {
    active_machines = Machine_GetTotal();
    
    std::cout << "Scheduler::Init(): Total number of machines is " + to_string(Machine_GetTotal()) << std::endl;
    SimOutput("Scheduler::Init(): Initializing scheduler", 1);

    cout << "Number of tasks: " << GetNumTasks() << endl;

    for(unsigned i = 0; i < active_machines; i++) {
        machines.push_back(MachineId_t(i));
        stateChange[machines[i]] = false;

        last_memory_used[machines[i]] = 0;
        last_task[machines[i]] = 0;
    }
}

Priority_t Scheduler::CalculatePriority(TaskId_t task_id) {
    SLAType_t sla = RequiredSLA(task_id);

    Priority_t priority = LOW_PRIORITY;
    if (sla == SLA0) {
        priority = HIGH_PRIORITY;
    } else if (sla == SLA1 || sla == SLA2) {
        priority = MID_PRIORITY;
    }

    return priority;
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    SimOutput("Migration has started for id : " + to_string(vm_id) + " at time " + to_string(time), 3);
    for (unsigned i = 0; i < pendingTasks[vm_id].size(); ++i) {
        TaskId_t task_id = pendingTasks[vm_id][i];
        VM_AddTask(vm_id, task_id, CalculatePriority(task_id));
    }

    migration[vm_id] = false;
    pendingTasks[vm_id].clear();
    SimOutput("Migration has completed for id : " + to_string(vm_id) + " at time " + to_string(time), 3);
}

void Scheduler::HandleStateChange(Time_t time, MachineId_t machine_id) {
    stateChange[machine_id] = false;
    SimOutput("Handling state change for machine id : " + to_string(machine_id) + " with state " + to_string(Machine_GetInfo(machine_id).s_state) + " at time " + to_string(time), 3);

    if (Machine_GetInfo(machine_id).s_state != S0) {
        SimOutput("Skipping clearing pending VMs because s_state is not S0 for machine id " + to_string(machine_id) + " at time " + to_string(time), 3);
        return;
    }

    for (unsigned i = 0; i < pendingVMs[machine_id].size(); ++i) {
        SimOutput("Clearing pending VMs for machine id : " + to_string(machine_id) + " at time " + to_string(time), 3);
        VMId_t vm_id = pendingVMs[machine_id][i];
        vms_per_machine[machine_id].push_back(vm_id);
        VM_Attach(vm_id, machine_id);
        for (unsigned j = 0; j < pendingTasks[vm_id].size(); ++j) {
            SimOutput("Clearing pending tasks for VM id : " + to_string(vm_id) + " at time " + to_string(time), 3);

            TaskId_t task_id = pendingTasks[vm_id][j];
            VM_AddTask(vm_id, task_id, CalculatePriority(task_id));
        }
        pendingTasks[vm_id].clear();
    }

    pendingVMs[machine_id].clear();
    SimOutput("State change has completed for id : " + to_string(machine_id) + " at time " + to_string(time), 3);
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    bool gpu = IsTaskGPUCapable(task_id);
    unsigned int task_mem = GetTaskMemory(task_id) + VM_MEMORY_OVERHEAD;
    VMType_t vm_type = RequiredVMType(task_id);
    CPUType_t cpu = RequiredCPUType(task_id);

    Priority_t priority = CalculatePriority(task_id);

    SimOutput("Attempting to look for machine to place new task in with task id " + to_string(task_id) + " at time " + to_string(now), 0);

    MachineId_t machine_id;
    VMId_t vm_id = vms.size();
    bool found = FindMachine(gpu, task_mem, cpu, vm_type, machine_id);

    if (!found) {
        SimOutput("Unable to find machine for task with id " + to_string(task_id), 0);
        return;
    }

    // assert(Machine_GetInfo(machine_id).s_state == S0);
    SimOutput("Found machine with id " + to_string(machine_id) + " with state " + to_string(Machine_GetInfo(machine_id).s_state) + " at time " + to_string(now), 0);
    if (Machine_GetInfo(machine_id).s_state != S0) {
        Machine_SetState(machine_id, S0);
    }

    // find a suitable VM on said machine

    for (VMId_t vm : vms_per_machine[machine_id]) {
        VMInfo_t vm_info = VM_GetInfo(vm);
        if (vm_info.active_tasks.size() < 100 && vm_info.vm_type == vm_type) {
            vm_id = vm;
            break;
        }
    }

    // check if we need to attach a new VM
    if (vm_id == vms.size()) {
        vm_id = VM_Create(vm_type, cpu);
        migration[vm_id] = false;
        vms.push_back(vm_id);
        SimOutput("Initializing VM with id " + to_string(vm_id), 0);

        if (stateChange[machine_id] || Machine_GetInfo(machine_id).s_state != S0) {
            pendingVMs[machine_id].push_back(vm_id);
            Machine_SetState(machine_id, S0);
        } else {
            VM_Attach(vm_id, machine_id);
            vms_per_machine[machine_id].push_back(vm_id);
            SimOutput("Attached VM " + to_string(vm_id) + " to Machine " + to_string(machine_id), 3);
        }
    }
    SimOutput("Deciding to place Task with task id " + to_string(task_id) + " on " + to_string(machine_id) + ". Migration of vm == " + to_string(migration[vm_id]) + ". State change of machine == " + to_string(stateChange[machine_id]), 3);

    if (migration[vm_id] || stateChange[machine_id]) {
        pendingTasks[vm_id].push_back(task_id);
        Machine_SetState(machine_id, S0);
        SimOutput("Task with task id " + to_string(task_id) + " awaits placement on machine " + to_string(machine_id), 3);
    } else {
        VM_AddTask(vm_id, task_id, priority);
        SimOutput("Task with task id " + to_string(task_id) + " placed successfully on machine " + to_string(machine_id), 3);
    }
}

/**
 * Find a machine id capable of handling the needs of a specific task.
 * @param prefer_gpu true if task prefers machines with gpus
 * @param task_mem the memory a task takes up
 * @param cpu the cpu type of the task
 * @param vm_type the vm type of a task
 */
bool Scheduler::FindMachine(bool prefer_gpu, unsigned int task_mem, CPUType_t cpu, VMType_t vm_type, MachineId_t& machine_id) {
    vector<MachineId_t> sorted = machines;
    sort(sorted.begin(), sorted.end(), compareMachines);

    MachineId_t best_id = active_machines;
    // variables about the best machine so far
    bool best_has_gpu = !prefer_gpu;
    bool best_is_running = false;
    bool best_memory = false;

    for (unsigned int i = 0; i < active_machines; ++i) {
        if (best_has_gpu && best_is_running) break;

        unsigned pending_mem = 0;
        for (VMId_t vm : pendingVMs[sorted[i]]) {
            for (TaskId_t tsk : VM_GetInfo(vm).active_tasks) {
                pending_mem += GetTaskInfo(tsk).required_memory;
            }
        }

        MachineInfo_t info = Machine_GetInfo(sorted[i]);
        unsigned mem_left = info.memory_size - info.memory_used - pending_mem;

        bool is_running = info.s_state == S0;
        bool enough_mem = task_mem <= mem_left;
        if (Machine_GetCPUType(sorted[i]) == cpu) {
            // if best machine does not have a gpu and we prefer one and cur machine does
            if ((!best_has_gpu && info.gpus)) {
                best_id = sorted[i];
                best_has_gpu = true;
                best_is_running = is_running;
                best_memory = enough_mem;
            // if best machine is not running and cur machine does
            } else if (!best_is_running && is_running) {
                best_id = sorted[i];
                best_is_running = is_running;
                best_memory = enough_mem;
            // if best machine does not have enough memory and cur machine does
            } else if (!best_memory && enough_mem) {
                best_id = sorted[i];
                best_memory = enough_mem;
            }
        }
    }

    machine_id = best_id;
    return machine_id != active_machines;
}


void Scheduler::PeriodicCheck(Time_t now) {
    for (unsigned i = 0; i < active_machines; ++i) {
        MachineId_t machine_id = machines[i];
        MachineInfo_t info = Machine_GetInfo(machine_id);

        Time_t last_task_time = last_task[machine_id];

        unsigned current_mem = info.memory_used;
        unsigned previous_mem = last_memory_used[machine_id];

        /*
            S-State change
        */
        MachineState_t s_state = info.s_state;

        SimOutput("PeriodicCheck(): [S-State] current_mem == 0: " + 
                  to_string(current_mem == 0), 3);
        SimOutput("PeriodicCheck(): [S-State] now - last_task_time >= threshold: " + 
                  to_string(now - last_task_time >= STATE_CHANGE_THRESHOLD), 3);

        // Check up on machines with no active processes
        if (current_mem == 0 && now - last_task_time >= STATE_CHANGE_THRESHOLD) {
            if (pendingVMs[machine_id].empty()) {
                s_state = s_state != S5 ? MachineState_t(s_state + 1) : S5;
            } else {
                s_state = S0;
            }
        }

        if (s_state != info.s_state) {
            Machine_SetState(machine_id, s_state);
            stateChange[machine_id] = true;
            SimOutput("PeriodicCheck(): Updated state of machine " + to_string(machine_id) +
                      " to " + to_string(s_state) + " at time " + to_string(now), 3);
        }

        /*
            P-State Change
        */
        CPUPerformance_t p_state = info.p_state;

        if (current_mem > previous_mem) {
            p_state = p_state != P0 ? CPUPerformance_t(p_state - 1) : P0;
        } else if (current_mem < previous_mem) {
            p_state = p_state != P0 ? CPUPerformance_t(p_state - 1) : P0;
        }

        if (p_state != info.p_state) {
            Machine_SetCorePerformance(machine_id, 0, p_state);
            SimOutput("PeriodicCheck(): Updated p_state of machine " + to_string(machine_id) +
                      " to " + to_string(p_state) + " at time " + to_string(now), 3);
        }

        last_memory_used[machine_id] = current_mem;

        // update time once condition has been hit
        if (now - last_task_time >= STATE_CHANGE_THRESHOLD) {
            last_task[machine_id] = now;
        }
    }
}

void Scheduler::Shutdown(Time_t time) {
    // Do your final reporting and bookkeeping here.
    // Report about the total energy consumed
    // Report about the SLA compliance
    // Shutdown everything to be tidy :-)
    SimOutput("SimulationComplete(): Initiating shutdown...", 3);
    for(const auto & vm: vms) {
        VM_Shutdown(vm);
    }
    SimOutput("SimulationComplete(): Finished!", 3);
    SimOutput("SimulationComplete(): Time is " + to_string(time), 3);
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    vector<MachineId_t> sorted = machines;
    sort(sorted.begin(), sorted.end(), compareMachines);
}

void Scheduler::HandleWarning(Time_t now, TaskId_t task_id) {
    SimOutput("handling warning", 3);
    vector<MachineId_t> sorted = machines;
    sort(sorted.begin(), sorted.end(), compareMachines);
    TaskInfo_t tskInf = GetTaskInfo(task_id);
}

// Public interface below

static Scheduler Scheduler;

void InitScheduler() {
    SimOutput("InitScheduler(): Initializing scheduler", 4);
    Scheduler.Init();
}

void HandleNewTask(Time_t time, TaskId_t task_id) {
    SimOutput("HandleNewTask(): Received new task " + to_string(task_id) + " at time " + to_string(time), 4);
    Scheduler.NewTask(time, task_id);
}

void HandleTaskCompletion(Time_t time, TaskId_t task_id) {
    SimOutput("HandleTaskCompletion(): Task " + to_string(task_id) + " completed at time " + to_string(time), 4);
    Scheduler.TaskComplete(time, task_id);
}

void MemoryWarning(Time_t time, MachineId_t machine_id) {
    // The simulator is alerting you that machine identified by machine_id is overcommitted
    SimOutput("MemoryWarning(): Overflow at " + to_string(machine_id) + " was detected at time " + to_string(time), 0);
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    // The function is called on to alert you that migration is complete
    SimOutput("MigrationDone(): Migration of VM " + to_string(vm_id) + " was completed at time " + to_string(time), 4);
    Scheduler.MigrationComplete(time, vm_id);
    migration[vm_id] = false;
}

void SchedulerCheck(Time_t time) {
    // This function is called periodically by the simulator, no specific event
    SimOutput("SchedulerCheck(): SchedulerCheck() called at " + to_string(time), 4);
    Scheduler.PeriodicCheck(time);
}

void SimulationComplete(Time_t time) {
    // This function is called before the simulation terminates Add whatever you feel like.
    cout << "SLA violation report" << endl;
    cout << "SLA0: " << GetSLAReport(SLA0) << "%" << endl;
    cout << "SLA1: " << GetSLAReport(SLA1) << "%" << endl;
    cout << "SLA2: " << GetSLAReport(SLA2) << "%" << endl;     // SLA3 do not have SLA violation issues
    cout << "Total Energy " << Machine_GetClusterEnergy() << "KW-Hour" << endl;
    cout << "Simulation run finished in " << double(time)/1000000 << " seconds" << endl;
    SimOutput("SimulationComplete(): Simulation finished at time " + to_string(time), 4);
    
    Scheduler.Shutdown(time);
}

void SLAWarning(Time_t time, TaskId_t task_id) {
    SimOutput("SLAWarning(): Task " + to_string(task_id) + " experiencing SLA Warning at time " + to_string(time), 3);
    Scheduler.HandleWarning(time, task_id);
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    // Called in response to an earlier request to change the state of a machine
    Scheduler.HandleStateChange(time, machine_id);
}
