//
//  Scheduler.cpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

#include <cassert>

#include "Scheduler.hpp"

// static bool migrating = false;
static unsigned active_machines;
static vector<vector<VMId_t>> vms_per_machine;

static map<VMId_t, bool> migration;
static map<MachineId_t, bool> stateChange;

using namespace std;

void Scheduler::Init() {
    // Find the parameters of the clusters
    // Get the total number of machines
    // For each machine:
    //      Get the type of the machine
    //      Get the memory of the machine
    //      Get the number of CPUs
    //      Get if there is a GPU or not
    // 
    active_machines = Machine_GetTotal();
    vms_per_machine = vector<vector<VMId_t>>(active_machines);
    
    std::cout << "Scheduler::Init(): Total number of machines is " + to_string(Machine_GetTotal()) << std::endl;
    SimOutput("Scheduler::Init(): Initializing scheduler", 1);


    cout << "Number of tasks: " << GetNumTasks() << endl;

    for(unsigned i = 0; i < active_machines; i++) {
        machines.push_back(MachineId_t(i));
        stateChange[machines[i]] = false;
    }
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    // Update your data structure. The VM now can receive new tasks
    SimOutput("Migration has completed for id : " + to_string(vm_id) + " at time " + to_string(time), 3);
    migration[vms[vm_id]] = false;
}

void Scheduler::HandleStateChange(Time_t time, MachineId_t machine_id) {
    SimOutput("State change has completed for id : " + to_string(machine_id) + " at time " + to_string(time), 3);
    stateChange[machines[machine_id]] = false;
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    bool gpu = IsTaskGPUCapable(task_id);
    unsigned int task_mem = GetTaskMemory(task_id) + VM_MEMORY_OVERHEAD;
    VMType_t vm_type = RequiredVMType(task_id);
    CPUType_t cpu = RequiredCPUType(task_id);
    SLAType_t sla = RequiredSLA(task_id);
    
    // SLA0: high priority
    // SLA1, SLA2: mid priority
    // SLA3: low priority
    Priority_t priority = LOW_PRIORITY;
    if (sla == SLA0) {
        priority = HIGH_PRIORITY;
    } else if (sla == SLA1 || sla == SLA2) {
        priority = MID_PRIORITY;
    }

    
    SimOutput("Attempting to look for machine to place new task in with task id " + to_string(task_id) + " at time " + to_string(now), 0);
    auto ret = FindMachine(gpu, task_mem, cpu, vm_type);
    MachineId_t machine_id = ret.first;
    VMId_t vm_id = ret.second;

    if (machine_id == active_machines + 1) {
        SimOutput("Unable to find machine for task with id " + to_string(task_id), 0);
        return;
    }

    if (vm_id == vms.size() + 1) {
        vm_id = VM_Create(vm_type, cpu);
        SimOutput("Initializing VM with id " + to_string(vm_id), 0);

        VM_Attach(vm_id, machine_id);

        vms.push_back(vm_id);

        migration[vm_id] = false;
    }

    SimOutput("Found VM " + to_string(vm_id) + " on Machine " + to_string(machine_id) + " for task " + to_string(task_id), 0);

    vms_per_machine[machine_id].push_back(vm_id);
    VM_AddTask(vm_id, task_id, priority);

    SimOutput("Task with task id " + to_string(task_id) + " placed successfully on machine " + to_string(machine_id), 0);
}

/**
 * Find a machine id capable of handling the needs of a specific task.
 * @param prefer_gpu true if task prefers machines with gpus
 * @param task_mem the memory a task takes up
 * @param cpu the cpu type of the task
 * @param vm_type the vm type of a task
 */
pair<MachineId_t, VMId_t> Scheduler::FindMachine(bool prefer_gpu, unsigned int task_mem, CPUType_t cpu, VMType_t vm_type) {
    MachineId_t best_id = machines.size() + 1;
    // variables about the best machine so far
    bool best_has_gpu = !prefer_gpu;
    bool best_has_vm_type = false;
    bool best_is_running = false;

    for (unsigned int i = 0; i < machines.size(); i++) {
        if (best_has_gpu && best_has_vm_type && best_is_running) break;

        MachineInfo_t info = Machine_GetInfo(machines[i]);
        unsigned mem_left = info.memory_size - info.memory_used;
        auto vms_on_machine = vms_per_machine[i];

        // Determine if machine has VM with Task's vm type
        bool has_vm_type = find_if(vms_on_machine.begin(), vms_on_machine.end(), [&vm_type](VMId_t id) {
            return VM_GetInfo(id).vm_type == vm_type;
        }) != vms_on_machine.end();

        bool is_running = info.s_state != S5;
        // 1.
        if (Machine_GetCPUType(i) == cpu && mem_left >= task_mem) {
            // 2. if best machine does not have a gpu and we prefer one and cur machine does
            if (!best_has_gpu && info.gpus) {
                best_id = i;
                best_has_gpu = true;
                best_has_vm_type = has_vm_type;
                best_is_running = is_running;
            // 3. if best machine does not have a matching vm and cur machine does
            } else if (!best_has_vm_type && has_vm_type) {
                best_id = i;
                best_has_vm_type = has_vm_type;
                best_is_running = is_running;
            // 4. if best machine is not running and cur machine does
            } else if (!best_is_running && is_running) {
                best_id = machines[i];
                best_is_running = is_running;
            }
        }
    }
    VMId_t vm_id = vms.size() + 1;

    // If machine has the matching VM type, let's find that VM
    if (best_has_vm_type) {
        for (VMId_t id : vms_per_machine[best_id]) {
            if (VM_GetInfo(id).vm_type == vm_type) {
                vm_id = id;
                break;
            }
        }
    }

    return {best_id, vm_id};
}


void Scheduler::PeriodicCheck(Time_t now) {
    // This method should be called from SchedulerCheck()
    // SchedulerCheck is called periodically by the simulator to allow you to monitor, make decisions, adjustments, etc.
    // Unlike the other invocations of the scheduler, this one doesn't report any specific event
    // Recommendation: Take advantage of this function to do some monitoring and adjustments as necessary
    for (unsigned i = 0; i < active_machines; ++i) {
        MachineInfo_t info = Machine_GetInfo(machines[i]);
        if (info.memory_used == 0 && info.active_vms == 0 && info.s_state != S5) {
            // Turn off the machine
            Machine_SetState(machines[i], S4);
            stateChange[machines[i]] = true;
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
    // vector<MachineId_t> sorted = machines;
    // sort(sorted.begin(), sorted.end(), compareMachines);
    // for (unsigned i = sorted.size() - 1; i < sorted.size() && Machine_GetInfo(i).memory_used != 0; --i) {
    //     // I'm just purposely overflowing i because I know for a fact we are NOT Having integer_max machines
    //     MachineInfo_t inf = Machine_GetInfo(i);
        
    //     for (const auto& vm : vms) {
    //         VMInfo_t vmInfo = VM_GetInfo(vm);
    //         if (vmInfo.machine_id == i) {
    //             for (unsigned j = 0; j < vmInfo.active_tasks.size(); ++j) {
    //                 TaskInfo_t tsk = GetTaskInfo(vmInfo.active_tasks[i]);
    //                 for (unsigned k = i + 1; k < sorted.size(); ++k) {
    //                     MachineInfo_t mchInf = Machine_GetInfo(sorted[k]);
    //                     unsigned remainingMem = mchInf.memory_size - mchInf.memory_used;
    //                     if (tsk.required_memory + VM_MEMORY_OVERHEAD <= remainingMem && tsk.required_cpu == Machine_GetCPUType(machines[k])) {
    //                         migration[vm] = true;
    //                         VM_Migrate(vm, k);
    //                         break;
    //                     }
    //                 }
    //             }
    //         }
    //     }

    //     if (inf.active_vms == 0) {
    //         Machine_SetState(i, S5);
    //     }
    // }
}

void Scheduler::HandleWarning(Time_t now, TaskId_t task_id) {
    vector<MachineId_t> sorted = machines;
    sort(sorted.begin(), sorted.end(), compareMachines);
    TaskInfo_t tskInf = GetTaskInfo(task_id);
    bool found = false;
    for (unsigned i = 0; i < sorted.size(); ++i) {
        MachineInfo_t mchInf = Machine_GetInfo(sorted[i]);
        unsigned remainingMem = mchInf.memory_size - mchInf.memory_used;
        if (tskInf.required_memory + VM_MEMORY_OVERHEAD <= remainingMem && Machine_GetCPUType(sorted[i]) == tskInf.required_cpu) {
            for (const auto& vm : vms) {
                VMInfo_t vmInf = VM_GetInfo(vm);
                for (TaskId_t tsk : vmInf.active_tasks) {
                    if (tsk == task_id) {
                        migration[vm] = true;
                        VM_Migrate(vm, i);
                        found = true;
                        break;
                    }
                }
                if (found) {
                    break;
                }
            }
        }
        if (found) {
            break;
        }
    }
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
    SimOutput("SLAWarning(): Task " + to_string(task_id) + " experiencing SLA Warning at time " + to_string(time), 4);
    Scheduler.HandleWarning(time, task_id);
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    // Called in response to an earlier request to change the state of a machine
    Scheduler.HandleStateChange(time, machine_id);
}
