//
//  Scheduler.cpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

#include <cassert>

#include "Scheduler.hpp"

#define STATE_CHANGE_THRESHOLD 100000000
#define BASIC_TASK_THRESHOLD 100

// static bool migrating = false;
static unsigned active_machines;
static map<MachineId_t, vector<VMId_t>> vms_per_machine;
// find the vm for a particular task
static map<TaskId_t, VMId_t> VM_per_task;

static map<VMId_t, bool> migration;
static map<MachineId_t, bool> experiencingMigration;
static map<MachineId_t, bool> stateChange;
static map<VMId_t, vector<TaskId_t>> pendingTasks;
static map<MachineId_t, vector<VMId_t>> pendingVMs;

static map<MachineId_t, Time_t> last_task;
static map<MachineId_t, unsigned> last_memory_used;

static map<VMId_t, unsigned> migrationCooldown;

static map<MachineId_t, bool> migrationQueued;
static map<MachineId_t, VMId_t> pendingMigration; // only one VM can migrate at a time

static vector<MachineId_t> gpuMachines;

using namespace std;

unsigned CalcPendingMem(vector<VMId_t> vms) {
    unsigned pending_mem = 0;
    for (auto vm : vms) {
        pending_mem += VM_MEMORY_OVERHEAD;
        VMInfo_t mchVMInf = VM_GetInfo(vm);
        for (auto tsk : pendingTasks[vm]) {
            pending_mem += GetTaskInfo(tsk).required_memory;
        }
        for (auto tsk : mchVMInf.active_tasks) {
            pending_mem += GetTaskInfo(tsk).required_memory;
        }
    }
    return pending_mem;
}

unsigned CalcUtil(MachineId_t machine_id) {
    MachineInfo_t info = Machine_GetInfo(machine_id);
    unsigned mem_size = info.memory_size;

    // Machines with more memory should be able to handle more tasks.
    return mem_size / BASIC_TASK_THRESHOLD;
}

void DecrementState(Time_t now, MachineId_t machine_id) {
    MachineInfo_t info = Machine_GetInfo(machine_id);
    Time_t last_task_time = last_task[machine_id];

    unsigned current_mem = info.memory_used;
    unsigned previous_mem = last_memory_used[machine_id];

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
    // vms_per_machine = vector<vector<VMId_t>>(active_machines);
    
    std::cout << "Scheduler::Init(): Total number of machines is " + to_string(Machine_GetTotal()) << std::endl;
    SimOutput("Scheduler::Init(): Initializing scheduler", 1);


    cout << "Number of tasks: " << GetNumTasks() << endl;

    for(unsigned i = 0; i < active_machines; i++) {
        machines.push_back(MachineId_t(i));
        if (Machine_GetInfo(machines[i]).gpus) {
            gpuMachines.push_back(machines[i]);
        }
        stateChange[machines[i]] = false;
    }
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    // Update your data structure. The VM now can receive new tasks
    SimOutput("Migration has completed for id : " + to_string(vm_id) + " at time " + to_string(time), 3);
    migration[vm_id] = false;
    VMInfo_t vmInf = VM_GetInfo(vm_id);

    for (unsigned i = 0; i < pendingTasks[vm_id].size(); ++i) {
        VM_AddTask(vm_id, pendingTasks[vm_id][i], HIGH_PRIORITY);
    }

    vector<VMId_t>::iterator itr = find(pendingVMs[vmInf.machine_id].begin(), pendingVMs[vmInf.machine_id].end(), vm_id);
    pendingVMs[vmInf.machine_id].erase(itr);
    MachineInfo_t mchInf = Machine_GetInfo(vmInf.machine_id);

    SimOutput("Machine " + to_string(vmInf.machine_id) + " has " + 
    to_string(mchInf.memory_size - mchInf.memory_used) + " left.", 3);

    pendingTasks[vm_id].clear();
    migrationCooldown[vm_id] = 100;
}

void Scheduler::HandleStateChange(Time_t time, MachineId_t machine_id) {
    SimOutput("State change has completed for id : " + to_string(machine_id) + 
    " at time " + to_string(time) + " where s-state is now " + to_string(Machine_GetInfo(machine_id).s_state), 3);
    stateChange[machine_id] = false;

    if (Machine_GetInfo(machine_id).s_state == S0) {
        for (unsigned i = 0; i < pendingVMs[machine_id].size(); ++i) {
            VM_Attach(pendingVMs[machine_id][i], machine_id);
            for (unsigned j = 0; j < pendingTasks[pendingVMs[machine_id][i]].size(); ++j) {
                VM_AddTask(pendingVMs[machine_id][i], pendingTasks[pendingVMs[machine_id][i]][j], HIGH_PRIORITY);
            }
            pendingTasks[pendingVMs[machine_id][i]].clear();
        }

        SimOutput("All desired VMs and tasks added to the machine : " + to_string(machine_id), 3);
        
        pendingVMs[machine_id].clear();
    }  
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    bool gpu = IsTaskGPUCapable(task_id);
    unsigned int task_mem = GetTaskMemory(task_id) + VM_MEMORY_OVERHEAD;
    VMType_t vm_type = RequiredVMType(task_id);
    CPUType_t cpu = RequiredCPUType(task_id);
    SLAType_t sla = RequiredSLA(task_id);

    // SimOutput("Task ID " + to_string(task_id) + " is projected to take : " 
    //     + to_string(GetTaskInfo(task_id).target_completion - GetTaskInfo(task_id).arrival) + " amount of time.", 3);
    
    // SLA0: high priority
    // SLA1, SLA2: mid priority
    // SLA3: low priority
    Priority_t priority = LOW_PRIORITY;
    if (sla == SLA0) {
        priority = HIGH_PRIORITY;
    } else if (sla == SLA1 || sla == SLA2) {
        priority = MID_PRIORITY;
    }

    
    SimOutput("Attempting to look for machine to place new task in with task id " + to_string(task_id), 3);
    auto ret = FindMachine(gpu, task_mem, cpu, vm_type);
    MachineId_t machine_id = ret.first;
    // VMId_t vm_id = ret.second;

    if (machine_id == machines.size()) {
        SimOutput("Unable to find machine for task with id " + to_string(task_id), 3);
        // Call SLAViolation routine
        return;
    }

    bool taskMustWait = false;

    VMId_t vm_id = vms.size();

    for (auto vm : vms_per_machine[machine_id]) {
        if (VM_GetInfo(vm).vm_type == vm_type && VM_GetInfo(vm).active_tasks.size() < 1000) {
            vm_id = vm;
            break;
        }
    }

    // if (vm_id == vms.size()) {
    // cout << "size of vms list is : " << vms.size() << endl;
    if (vm_id == vms.size()) {
        vm_id = VM_Create(vm_type, cpu);
        SimOutput("Initializing VM with id " + to_string(vm_id), 3);

        if (Machine_GetInfo(machine_id).s_state == S3 || stateChange[machine_id]) {
            pendingVMs[machine_id].push_back(vm_id);
            pendingTasks[vm_id].push_back(task_id);
            SimOutput("VM " + to_string(vm_id) + " waits to be added to Machine " + to_string(machine_id), 3);
            taskMustWait = true;
            // stateChange[machine_id] = true;
            // Machine_SetState(machine_id, S0);
        } else {
            VM_Attach(vm_id, machine_id);
            SimOutput("Attached VM " + to_string(vm_id) + " to Machine " + to_string(machine_id), 3);
        }

        vms.push_back(vm_id);

        migration[vm_id] = false;
        vms_per_machine[machine_id].push_back(vm_id);
    } else {
        SimOutput("Using pre-existing VM " + to_string(vm_id) + " on Machine " + to_string(machine_id), 3);
        SimOutput("VM CPU type : " + to_string(VM_GetInfo(vm_id).cpu) + ", and task CPU type" + to_string(GetTaskInfo(task_id).required_cpu) + 
        ", coupled with machine CPU type " + to_string(Machine_GetCPUType(machine_id)), 3);
    }

    if (!taskMustWait) {
        bool pending = false;
        for (unsigned i = 0; i < pendingVMs[machine_id].size(); ++i) {
            if (pendingVMs[machine_id][i] == vm_id) {
                pending = true;
                break;
            }
        }

        VM_per_task[task_id] = vm_id;

        if (migration[vm_id] || pending) {
            pendingTasks[vm_id].push_back(task_id);
            SimOutput("Task with task id " + to_string(task_id) + " awaits placement on VM " + to_string(vm_id), 3);
        } else {
            VM_AddTask(vm_id, task_id, priority);
            SimOutput("Task with task id " + to_string(task_id) + " placed successfully on machine " + to_string(machine_id), 3);
        }
    } else {
        SimOutput("Task with task id " + to_string(task_id) + " awaits placement on machine " + to_string(machine_id), 3);
    }

    if (task_id == 81) {
        cout << "VMS PER MACHINE" << endl;
        for (const auto& pair : vms_per_machine) {
            cout << pair.first << ": ";
            
            for (auto id : pair.second) {
                cout << id << " ";
            }
            cout << endl;
        }

        cout << "PENDING TASKS" << endl;
        for (const auto& pair : pendingTasks) {
            cout << pair.first << ": ";
            
            for (auto id : pair.second) {
                cout << id << " ";
            }
            cout << endl;
        }

        cout << "PENDING VMS" << endl;
        for (const auto& pair : pendingVMs) {
            cout << pair.first << ": ";
            
            for (auto id : pair.second) {
                cout << id << " ";
            }
            cout << endl;
        }

        cout << "MIGRATION" << endl;
        for (const auto& pair : migration) {
            cout << pair.first << ": " << to_string(pair.second) << endl;
        }

        cout << "STATE CHANGE" << endl;
        for (const auto& pair : stateChange) {
            cout << pair.first << ": " << to_string(pair.second) << endl;
        }

    }
}

/**
 * Find a machine id capable of handling the needs of a specific task.
 * @param prefer_gpu true if task prefers machines with gpus
 * @param task_mem the memory a task takes up
 * @param cpu the cpu type of the task
 * @param vm_type the vm type of a task
 */
pair<MachineId_t, VMId_t> Scheduler::FindMachine(bool prefer_gpu, unsigned int task_mem, CPUType_t cpu, VMType_t vm_type) {
            vector<MachineId_t> sorted;
            if (prefer_gpu) {
                sorted = gpuMachines;
            } else {
                sorted = machines;
            }

            sort(sorted.begin(), sorted.end(), cmpMachinesEnergy);

            pair<MachineId_t, VMId_t> ret = {sorted.size(), vms.size()};

            for (unsigned int i = 0; i < sorted.size(); i++) {
                MachineInfo_t info = Machine_GetInfo(sorted[i]);

                unsigned pending_mem = CalcPendingMem(pendingVMs[sorted[i]]);
                
                unsigned mem_left = info.memory_size - info.memory_used - pending_mem;
                SimOutput("Machine with id " + to_string(sorted[i]) + " has " + 
                    to_string(mem_left) + " memory remaining and cpu of type " + to_string(info.cpu), 3);
                if (Machine_GetCPUType(sorted[i]) == cpu && mem_left >= task_mem) {
                    ret = {sorted[i], vms.size()};
                    SimOutput("Found a machine with id " + to_string(sorted[i]) + " and cpu type : " + to_string(Machine_GetCPUType(sorted[i])), 3);
                    break;
                }
            }

            // for (unsigned i = 0; i < active_machines; ++i) {
            //     MachineInfo_t mchInf = Machine_GetInfo(sorted[i]);
            //     if (mchInf.memory_used == 0 && mchInf.active_vms == 0 && mchInf.s_state != S3 && !stateChange[mchInf.machine_id]) {
            //         SimOutput("Turning off machine : " + to_string(mchInf.machine_id) + " in FindMachines, based on P_Mapper", 3);
            //         // stateChange[sorted[i]] = true;
            //         // Machine_SetState(sorted[i], S3);
            //     }
            // }
            
            return ret;
}


void Scheduler::PeriodicCheck(Time_t now) {
    // This method should be called from SchedulerCheck()
    // SchedulerCheck is called periodically by the simulator to allow you to monitor, make decisions, adjustments, etc.
    // Unlike the other invocations of the scheduler, this one doesn't report any specific event
    // Recommendation: Take advantage of this function to do some monitoring and adjustments as necessary

    // for (unsigned i = 0; i < active_machines; ++i) {
    //     MachineInfo_t info = Machine_GetInfo(machines[i]);
    //     if (info.memory_used == 0 && info.active_vms == 0 && info.s_state != S3 && !stateChange[machines[i]]) {
    //         // Turn off the machine
    //         SimOutput("Turning off machine : " + to_string(machines[i]) + " at time : " + to_string(now), 3);
    //         // stateChange[machines[i]] = true;
    //         // Machine_SetState(machines[i], S3);
    //     }
    // }

    // To prevent machines from migrating too much because that's annoying and time-consuming
    // Could later repurpose for state changes? Or the general idea of it anyway
    for (auto a : migrationCooldown) {
        migrationCooldown[a.first] -= 20;
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
    // Do any bookkeeping necessary for the data structures
    // Decide if a machine is to be turned off, slowed down, or VMs to be migrated according to your policy
    // This is an opportunity to make any adjustments to optimize performance/energy
    SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) + " is complete at " + to_string(now), 4);
        vector<MachineId_t> sorted = machines;
        sort(sorted.begin(), sorted.end(), compareMachines);
        unsigned mid = sorted.size() / 2;
        TaskId_t smallest = 0;

        // I have arbitrarily set this value to be UINT_MAX - 1
        unsigned smallestMemReq = 4294967294;

        VMId_t smallestVM = 0;

        vector<VMId_t> listVMs = vms_per_machine[sorted[0]];

        for (const auto& vm : listVMs) {
            VMInfo_t vmInf = VM_GetInfo(vm);
            const vector<TaskId_t>& tsks = vmInf.active_tasks;
            for (unsigned i = 0; i < tsks.size(); ++i) {
                unsigned old = smallestMemReq;
                smallestMemReq = min(smallestMemReq, GetTaskInfo(tsks[i]).required_memory + VM_MEMORY_OVERHEAD);
                if (smallestMemReq != old) {
                    smallest = tsks[i];
                    smallestVM = vm;
                }
            }
        }
        
        TaskInfo_t tskInfo = GetTaskInfo(smallest);
        // bool found = false;

        for (unsigned i = mid + 1; i < sorted.size(); ++i) {
            MachineInfo_t mchInf = Machine_GetInfo(sorted[i]);
            unsigned remainingMem = mchInf.memory_size - mchInf.memory_used;
            unsigned pending_mem = CalcPendingMem(pendingVMs[sorted[i]]);

            remainingMem -= pending_mem;

            if (smallestMemReq <= remainingMem && mchInf.cpu == tskInfo.required_cpu) {
                SimOutput("Migrating VM in TaskComplete " + to_string(smallestVM) + " to machine " + to_string(sorted[i]), 3);
                migration[smallestVM] = true;
                pendingVMs[sorted[i]].push_back(smallestVM);
                VM_Migrate(smallestVM, sorted[i]);
                SetTaskPriority(task_id, HIGH_PRIORITY);

                vector<VMId_t>::iterator itr = find(vms_per_machine[VM_GetInfo(smallestVM).machine_id].begin(), 
                vms_per_machine[VM_GetInfo(smallestVM).machine_id].end(), smallestVM);

                if (itr != vms_per_machine[VM_GetInfo(smallestVM).machine_id].end()) {
                    vms_per_machine[VM_GetInfo(smallestVM).machine_id].erase(itr);
                }
                
                break;
            }
        }
}

void Scheduler::HandleWarning(Time_t now, TaskId_t task_id) {
    vector<MachineId_t> sorted = machines;
    sort(sorted.begin(), sorted.end(), compareMachines);
    TaskInfo_t tskInf = GetTaskInfo(task_id);

    VMId_t vm = VM_per_task[task_id];
    VMInfo_t vmInf = VM_GetInfo(vm);
    unsigned reqMem = VM_MEMORY_OVERHEAD;

    for (unsigned i = 0; i < vmInf.active_tasks.size(); ++i) {
        reqMem += GetTaskInfo(vmInf.active_tasks[i]).required_memory;
    }

        for (unsigned i = 0; i < sorted.size(); ++i) {
            MachineInfo_t mchInf = Machine_GetInfo(sorted[i]);
            unsigned remainingMem = mchInf.memory_size - mchInf.memory_used;
            unsigned pending_mem = CalcPendingMem(pendingVMs[sorted[i]]);

            remainingMem -= pending_mem;

            // Note: Make code safe with VM migration AND consider Machine State!
            if (reqMem <= remainingMem && Machine_GetCPUType(sorted[i]) == tskInf.required_cpu
        && !migration[vm] && migrationCooldown[vm] == 0) {
                SimOutput("Migrating VM in HandleWarning " + to_string(vm) + " to machine " + to_string(sorted[i]), 3);
                SimOutput("Machine has " + to_string(remainingMem) + " left, while the new VM will take up " + 
                to_string(reqMem) + " amount of memory.", 3);

                migration[vm] = true;
                pendingVMs[sorted[i]].push_back(vm);
                SetTaskPriority(task_id, HIGH_PRIORITY);

                vector<VMId_t>::iterator itr = find(vms_per_machine[vmInf.machine_id].begin(), 
                vms_per_machine[vmInf.machine_id].end(), vm);

                if (itr != vms_per_machine[vmInf.machine_id].end()) {
                    vms_per_machine[vmInf.machine_id].erase(itr);
                }

                VM_Migrate(vm, sorted[i]);
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
    SimOutput("MemoryWarning(): Machine " + to_string(machine_id) + 
    " is using " + to_string(Machine_GetInfo(machine_id).memory_used) + 
    " when it has only " + to_string((Machine_GetInfo(machine_id).memory_size)), 3);
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    // The function is called on to alert you that migration is complete
    SimOutput("MigrationDone(): Migration of VM " + to_string(vm_id) + " was completed at time " + to_string(time), 4);
    Scheduler.MigrationComplete(time, vm_id);
    // migration[vm_id] = false;
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

    int completed = 0;
    for (unsigned i = 0; i < GetNumTasks(); ++i) {
        if (IsTaskCompleted(i)) {
            completed++;
        }
    }

    cout << "Total tasks completed: " << completed << "/" << GetNumTasks() << endl;
    
    Scheduler.Shutdown(time);
}

void SLAWarning(Time_t time, TaskId_t task_id) {
    SimOutput("SLAWarning(): Task " + to_string(task_id) + " experiencing SLA Warning at time " + to_string(time), 4);
    Scheduler.HandleWarning(time, task_id);
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    // Called in response to an earlier request to change the state of a machinE
    SimOutput("StateChangeComplete(): State change of " + to_string(machine_id) + " was completed at time " + to_string(time), 4);
    Scheduler.HandleStateChange(time, machine_id);
}
