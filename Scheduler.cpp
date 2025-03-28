//
//  Scheduler.cpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

#include <cassert>
#include <algorithm>
#include "Scheduler.hpp"

// variable constants
#define STATE_CHANGE_THRESHOLD 1000000 // time between state change checks
#define VM_LIMIT_THRESHOLD 100 // max tasks per VM

static unsigned active_machines;
static map<MachineId_t, vector<VMId_t>> vms_per_machine;
// static vector<int> checks_since_last_work;

static map<VMId_t, bool> migration;
static map<MachineId_t, bool> stateChange;
static map<MachineId_t, bool> experiencingMigration;


static map<VMId_t, vector<TaskId_t>> pendingTasks;

static map<MachineId_t, vector<VMId_t>> pendingVMs;

static map<MachineId_t, bool> migrationQueued;
static map<MachineId_t, VMId_t> pendingMigration; // only one VM can migrate at a time

static map<MachineId_t, unsigned> last_memory_used;
static map<MachineId_t, Time_t> last_task;

static map<TaskId_t, VMId_t> task_to_vm;

using namespace std;

void Scheduler::Init() {
    active_machines = Machine_GetTotal();
    
    std::cout << "Scheduler::Init(): Total number of machines is " + to_string(Machine_GetTotal()) << std::endl;
    SimOutput("Scheduler::Init(): Initializing scheduler", 1);

    cout << "Number of tasks: " << GetNumTasks() << endl;

    for(unsigned i = 0; i < active_machines; i++) {
        machines.push_back(MachineId_t(i));
        stateChange[machines[i]] = false;
        experiencingMigration[machines[i]] = false;

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
    // Clear migration mapping once migrating is done
    migration[vm_id] = false;
    experiencingMigration[VM_GetInfo(vm_id).machine_id] = false;
    cout << "Machine memory limit: " << Machine_GetInfo(VM_GetInfo(vm_id).machine_id).memory_size << endl;
    cout << "Machine that is being migrated to memory usage after migration: " << Machine_GetInfo(VM_GetInfo(vm_id).machine_id).memory_used << endl;
    SimOutput("Migration has completed for VM id: " + to_string(vm_id) + " on machine " + to_string(VM_GetInfo(vm_id).machine_id) + " at time " + to_string(time), 3);
}

void Scheduler::HandleStateChange(Time_t time, MachineId_t machine_id) {
    SimOutput("Handling state change for machine id: " + to_string(machine_id) + " with state " +
              to_string(Machine_GetInfo(machine_id).s_state) + " at time " + to_string(time), 3);

    if (Machine_GetInfo(machine_id).s_state != S0) {
        SimOutput("Skipping clearing pending VMs because s_state is not S0 for machine id " +
                  to_string(machine_id) + " at time " + to_string(time), 3);
        return;
    }

    if (pendingMigration[machine_id] && !experiencingMigration[machine_id]) {
        experiencingMigration[machine_id] = true;
        pendingMigration[machine_id] = false;
        VM_Migrate(pendingMigration[machine_id], machine_id);
        vms_per_machine[machine_id].push_back(pendingMigration[machine_id]);
    }

    for (unsigned i = 0; i < pendingVMs[machine_id].size(); ++i) {
        SimOutput("Clearing pending VMs for machine id: " + to_string(machine_id) + " at time " + to_string(time), 3);
        VMId_t vm_id = pendingVMs[machine_id][i];
        vms_per_machine[machine_id].push_back(vm_id);
        VM_Attach(vm_id, machine_id);
        for (unsigned j = 0; j < pendingTasks[vm_id].size(); ++j) {
            SimOutput("Clearing pending tasks for VM id: " + to_string(vm_id) + " at time " + to_string(time), 3);
            TaskId_t task_id = pendingTasks[vm_id][j];
            VM_AddTask(vm_id, task_id, CalculatePriority(task_id));
        }
        pendingTasks[vm_id].clear();
    }
    pendingVMs[machine_id].clear();
    stateChange[machine_id] = false;
    SimOutput("State change has completed for machine id: " + to_string(machine_id) + " at time " + to_string(time), 3);
}

bool Scheduler::FindMachine(bool prefer_gpu, unsigned int task_mem, CPUType_t cpu, VMType_t vm_type, MachineId_t& machine_id, bool require_memory) {
    vector<MachineId_t> sorted = machines;
    sort(sorted.begin(), sorted.end(), compareMachines);

    MachineId_t best_id = active_machines;
    bool best_has_gpu = !prefer_gpu;
    bool best_is_running = false;
    bool best_memory = false;

    for (unsigned int i = 0; i < active_machines; ++i) {
        // Calculate pending memory usage on this machine.
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

        // if machine does not have enough memory
        if (require_memory && !enough_mem)
            continue;

        if (Machine_GetCPUType(sorted[i]) == cpu) {
            // if best machine does not have gpu and cur machine does
            if (!best_has_gpu && info.gpus) {
                best_id = sorted[i];
                best_has_gpu = true;
                best_is_running = is_running;
                best_memory = enough_mem;
            }
            // if best machine is not running and cur machine is
            else if (!best_is_running && is_running) {
                best_id = sorted[i];
                best_is_running = is_running;
                best_memory = enough_mem;
            }
            // if best machine does not have enouhg mem and cur machine does
            else if (!best_memory && enough_mem) {
                best_id = sorted[i];
                best_memory = enough_mem;
            }
        }
    }

    machine_id = best_id;
    return machine_id != active_machines;
}

bool Scheduler::FindMachineForVM(VMInfo_t vm_info, MachineId_t& machine_id, bool require_running) {
    cout << " In find machine for vm" << endl;
    vector<MachineId_t> sorted = machines;
    sort(sorted.begin(), sorted.end(), compareMachines);

    MachineId_t best_id = active_machines;

    // find if any tasks prefer gpu
    bool best_has_gpu = true;
    unsigned vm_mem = VM_MEMORY_OVERHEAD;
    for (TaskId_t tsk : vm_info.active_tasks) {
        if (GetTaskInfo(tsk).gpu_capable) {
            best_has_gpu = false;
        }
        vm_mem += GetTaskInfo(tsk).required_memory;
    }

    cout << "vm_mem: " << vm_mem << endl;

    bool best_is_running = !require_running;

    for (unsigned int i = 0; i < active_machines; ++i) {

        if (experiencingMigration[sorted[i]]) continue;

        if (best_has_gpu && best_is_running) break;

        // Calculate pending memory usage on this machine.
        unsigned pending_mem = 0;
        for (VMId_t vm : pendingVMs[sorted[i]]) {
            for (TaskId_t tsk : VM_GetInfo(vm).active_tasks) {
                pending_mem += GetTaskInfo(tsk).required_memory;
            }
        }

        MachineInfo_t info = Machine_GetInfo(sorted[i]);
        cout << "Machine id: " << info.machine_id << endl;
        cout << "info.mem size: " << info.memory_size << endl;
        cout << "info.mem used: " << info.memory_used << endl;
        cout << " Pending memory: " << pending_mem << endl;
        unsigned mem_left = info.memory_size - info.memory_used - pending_mem;

        if (mem_left > info.memory_size) {
            cout << "Error: Calculated available memory (" << mem_left 
                 << ") exceeds total memory (" << info.memory_size 
                 << ") for machine " << info.machine_id << ". VM list: ";

            for (unsigned j = 0; j < active_machines; ++j) {
                cout << "Machine " << j << ": ";
                for (auto vm : vms_per_machine[j]) {
                    cout << vm << " ";
                }
                cout << endl;
            }
        }

        cout << " mem_left: " << mem_left << endl;
        bool is_running = info.s_state == S0;
        bool enough_mem = vm_mem <= mem_left;

        // if machine does not have enough memory
        if (!enough_mem || Machine_GetCPUType(sorted[i]) != vm_info.cpu) continue;

        // if best machine does not have gpu and cur machine does
        if (!best_has_gpu && info.gpus) {
            best_id = sorted[i];
            best_has_gpu = true;
            best_is_running = is_running;
        }
        // if best machine is not running and cur machine is
        else if (!best_is_running && is_running) {
            best_id = sorted[i];
            best_is_running = is_running;
        }
    }

    cout << "best machine is: " << best_id << endl;
    machine_id = best_id;
    return machine_id != active_machines;
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    bool gpu = IsTaskGPUCapable(task_id);
    unsigned int task_mem = GetTaskMemory(task_id) + VM_MEMORY_OVERHEAD;
    VMType_t vm_type = RequiredVMType(task_id);
    CPUType_t cpu = RequiredCPUType(task_id);

    Priority_t priority = CalculatePriority(task_id);

    SimOutput("Attempting to find a machine for new task with id " + to_string(task_id) + " at time " + to_string(now), 0);

    MachineId_t machine_id;
    VMId_t vm_id = vms.size();

    bool found = FindMachine(gpu, task_mem, cpu, vm_type, machine_id, true);

    if (!found) {
        SimOutput("Unable to find machine for task with id " + to_string(task_id), 0);
        return;
    }

    // Set machine to S0 if not already
    SimOutput("Found machine with id " + to_string(machine_id) + " with state " + to_string(Machine_GetInfo(machine_id).s_state) + " at time " + to_string(now), 0);
    if (Machine_GetInfo(machine_id).s_state != S0) {
        stateChange[machine_id] = true;
        Machine_SetState(machine_id, S0);
    }

    // See if machine has viable VM
    for (VMId_t vm : vms_per_machine[machine_id]) {
        VMInfo_t vm_info = VM_GetInfo(vm);
        if (vm_info.active_tasks.size() < VM_LIMIT_THRESHOLD && vm_info.vm_type == vm_type) {
            vm_id = vm;
            break;
        }
    }

    // If needed, make a new VM to place on machine for the current task
    if (vm_id == vms.size()) {
        vm_id = VM_Create(vm_type, cpu);
        migration[vm_id] = false;
        vms.push_back(vm_id);
        SimOutput("Initializing VM with id " + to_string(vm_id), 0);

        // If machine is still transitioning to S0, push on a pending queue
        if (stateChange[machine_id]) {
            pendingVMs[machine_id].push_back(vm_id);
            SimOutput("VM " + to_string(vm_id) + " awaits placement on machine " + to_string(machine_id), 3);
        } else {
            VM_Attach(vm_id, machine_id);
            vms_per_machine[machine_id].push_back(vm_id);
            SimOutput("Attached VM " + to_string(vm_id) + " to Machine " + to_string(machine_id), 3);
        }
    }
    SimOutput("Deciding to place Task with id " + to_string(task_id) + " on machine " + to_string(machine_id) +
              ". Migration of VM == " + to_string(migration[vm_id]) + ". State change of machine == " + to_string(stateChange[machine_id]), 3);

    task_to_vm[task_id] = vm_id;

    // If VM is migrating or machine still transitioning, place task on pending queue
    if (migration[vm_id] || stateChange[machine_id]) {
        pendingTasks[vm_id].push_back(task_id);
        stateChange[machine_id] = true;
        Machine_SetState(machine_id, S0);
        SimOutput("Task with id " + to_string(task_id) + " awaits placement on machine " + to_string(machine_id), 3);
    } else {
        VM_AddTask(vm_id, task_id, priority);
        SimOutput("Task with id " + to_string(task_id) + " placed successfully on machine " + to_string(machine_id), 3);
    }
}


void Scheduler::HandleWarning(Time_t now, TaskId_t task_id) {
    SimOutput("Handling SLA warning for task id " + to_string(task_id), 3);
    // VMId_t vm_to_migrate = vms.size();

    // for (VMId_t vm : vms) {
    //     VMInfo_t vm_info = VM_GetInfo(vm);
    //     if (find(vm_info.active_tasks.begin(), vm_info.active_tasks.end(), task_id) != vm_info.active_tasks.end()) {
    //         vm_to_migrate = vm;
    //         break;
    //     }
    // }

    // if (vm_to_migrate == vms.size()) {
    //     for (auto vm : vms) {
    //         cout << "VM " << vm << " active tasks: ";
    //         for (auto t : VM_GetInfo(vm).active_tasks) {
    //             cout << t << " ";
    //         }
    //         cout << "\n";
    //     }
    //     SimOutput("Could not find the VM for SLA warning task id " + to_string(task_id), 3);
    //     return;
    // }

    VMId_t vm_to_migrate = task_to_vm[task_id];

    if (migration[vm_to_migrate]) {
        SimOutput("VM " + to_string(vm_to_migrate) + " is already migrating.", 3);
        return;
    }
    

    MachineId_t dest_machine;
    bool found = FindMachineForVM(VM_GetInfo(vm_to_migrate), dest_machine, true);
    assert(!experiencingMigration[dest_machine]);

    if (!found) {
        SimOutput("No suitable machine found for migrating VM " + to_string(vm_to_migrate), 3);
        return;
    }

    migration[vm_to_migrate] = true;
    experiencingMigration[dest_machine] = true;
    stateChange[dest_machine] = true;

    Machine_SetState(dest_machine, S0);

    // find VM's old machine and remove it from mapping
    auto& vm_list = vms_per_machine[VM_GetInfo(vm_to_migrate).machine_id];

    auto it = find(vm_list.begin(), vm_list.end(), vm_to_migrate);
    assert(find(vm_list.begin(), vm_list.end(), vm_to_migrate) != vm_list.end());
    vm_list.erase(it); 

    if (!stateChange[dest_machine] && Machine_GetInfo(dest_machine).s_state == S0) {
        VM_Migrate(vm_to_migrate, dest_machine);
        vms_per_machine[dest_machine].push_back(vm_to_migrate);
        SimOutput("Initiated migration of VM " + to_string(vm_to_migrate) + " to machine " +
        to_string(dest_machine) + " at time " + to_string(now), 3);
    } else {
        migrationQueued[dest_machine] = true;   
        pendingMigration[dest_machine] = vm_to_migrate;
        SimOutput("Awaiting migration of VM " + to_string(vm_to_migrate) + " to machine " +
        to_string(dest_machine) + " at time " + to_string(now), 3);
    }

}

void Scheduler::PeriodicCheck(Time_t now) {
    for (unsigned i = 0; i < active_machines; ++i) {
        MachineId_t machine_id = machines[i];
        MachineInfo_t info = Machine_GetInfo(machine_id);

        Time_t last_task_time = last_task[machine_id];

        unsigned current_mem = info.memory_used;
        unsigned previous_mem = last_memory_used[machine_id];

        // if this machine has load over 80% (memory used / memory size)
        //  migrate largest VM to lower utilized machine, reuse FindMachine()
        
        if (now - last_task_time >= STATE_CHANGE_THRESHOLD) {

            /*
                S-State Change
            */

            cout << "cur mem: " << current_mem << endl;
            cout << "previous mem: " << previous_mem << endl;
           MachineState_t s_state = info.s_state;
           if (current_mem == 0 && previous_mem == 0) {
                s_state = s_state != S5 ? MachineState_t(s_state + 1) : S5;
           } else {
                s_state = s_state != S0 ? MachineState_t(s_state - 1) : S0;
           }

           if (s_state != info.s_state) {
            stateChange[machine_id] = true;
            Machine_SetState(machine_id, s_state);
            SimOutput("PeriodicCheck(): Updated s_state of machine " + to_string(machine_id) +
                    " to " + to_string(s_state) + " at time " + to_string(now), 3);
            }

            /*
                P-State Change
            */
            CPUPerformance_t p_state = info.p_state;

            if (current_mem > previous_mem) {
                p_state = p_state != P0 ? CPUPerformance_t(p_state - 1) : P0;
            } else if (current_mem < previous_mem) {
                p_state = p_state != P3 ? CPUPerformance_t(p_state + 1) : P3;
            }

            if (p_state != info.p_state) {
                Machine_SetCorePerformance(machine_id, 0, p_state);
                SimOutput("PeriodicCheck(): Updated p_state of machine " + to_string(machine_id) +
                        " to " + to_string(p_state) + " at time " + to_string(now), 3);
            }

            last_task[machine_id] = now;
            last_memory_used[machine_id] = current_mem;
        }
    }
}

void Scheduler::Shutdown(Time_t time) {
    SimOutput("SimulationComplete(): Initiating shutdown...", 3);
    for (const auto & vm : vms) {
        VM_Shutdown(vm);
    }
    SimOutput("SimulationComplete(): Finished!", 3);
    SimOutput("SimulationComplete(): Time is " + to_string(time), 3);
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    vector<MachineId_t> sorted = machines;
    sort(sorted.begin(), sorted.end(), compareMachines);
}

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
    SimOutput("MemoryWarning(): Overflow at machine " + to_string(machine_id) + " detected at time " + to_string(time), 0);
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    SimOutput("MigrationDone(): Migration of VM " + to_string(vm_id) + " completed at time " + to_string(time), 4);
    Scheduler.MigrationComplete(time, vm_id);
}

void SchedulerCheck(Time_t time) {
    SimOutput("SchedulerCheck(): Called at " + to_string(time), 4);
    Scheduler.PeriodicCheck(time);
}

void SimulationComplete(Time_t time) {
    cout << "SLA violation report" << endl;
    cout << "SLA0: " << GetSLAReport(SLA0) << "%" << endl;
    cout << "SLA1: " << GetSLAReport(SLA1) << "%" << endl;
    cout << "SLA2: " << GetSLAReport(SLA2) << "%" << endl;
    cout << "Total Energy " << Machine_GetClusterEnergy() << " KW-Hour" << endl;
    cout << "Simulation run finished in " << double(time) / 1000000 << " seconds" << endl;
    SimOutput("SimulationComplete(): Finished at time " + to_string(time), 4);
    
    Scheduler.Shutdown(time);
}

void SLAWarning(Time_t time, TaskId_t task_id) {
    SimOutput("SLAWarning(): Task " + to_string(task_id) + " experiencing SLA Warning at time " + to_string(time), 3);
    Scheduler.HandleWarning(time, task_id);
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    Scheduler.HandleStateChange(time, machine_id);
}
