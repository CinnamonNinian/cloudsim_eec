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

    
    SimOutput("Attempting to look for machine to place new task in with task id " + to_string(task_id), 3);
    auto ret = FindMachine(gpu, task_mem, cpu, vm_type);
    MachineId_t machine_id = ret.first;
    VMId_t vm_id = ret.second;

    if (machine_id == active_machines + 1) {
        SimOutput("Unable to find machine for task with id " + to_string(task_id), 3);
        return;
    }

    if (vm_id == VMId_t(-1)) {
        vm_id = VM_Create(vm_type, cpu);
        SimOutput("Initializing VM with id " + to_string(vm_id), 3);

        VM_Attach(vm_id, machine_id);
        vms.push_back(vm_id);

        migration[vm_id] = false;
    }

    SimOutput("Attached VM " + to_string(vm_id) + " to Machine", 3);

    vms_per_machine[machine_id].push_back(vm_id);
    VM_AddTask(vm_id, task_id, priority);

    SimOutput("Task with task id " + to_string(task_id) + " placed successfully on machine " + to_string(machine_id), 3);
}

/**
 * Find a machine id capable of handling the needs of a specific task.
 * @param prefer_gpu true if task prefers machines with gpus
 * @param task_mem the memory a task takes up
 * @param cpu the cpu type of the task
 * @param vm_type the vm type of a task
 * @returns the machine to place the task on, along with the vm to attach it to. 
 * If machine id is -1, failed to find a machine. 
 * If vm id is -1, no vm on that machine has the required vm type.
 */
pair<MachineId_t, VMId_t> Scheduler::FindMachine(bool prefer_gpu, unsigned int task_mem, CPUType_t cpu, VMType_t vm_type) {
    return {-1, -1};
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
            SimOutput("Turning off machine : " + to_string(machines[i]) + " at time : " + to_string(now), 3);
            Machine_SetState(machines[i], S5);
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
    // Do any bookkeeping necessary for the data structures
    // Decide if a machine is to be turned off, slowed down, or VMs to be migrated according to your policy
    // This is an opportunity to make any adjustments to optimize performance/energy
    SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) + " is complete at " + to_string(now), 4);

}

void Scheduler::HandleWarning(Time_t now, TaskId_t task_id) {
    vector<MachineId_t> sorted = machines;
    sort(sorted.begin(), sorted.end(), compareMachines);
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
    // Called in response to an earlier request to change the state of a machinE
    Scheduler.HandleStateChange(time, machine_id);
}
