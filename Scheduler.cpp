//
//  Scheduler.cpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

#include "Scheduler.hpp"
#include <assert.h>

static bool migrating = false;
static unsigned active_machines;

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
    
    std::cout << "Scheduler::Init(): Total number of machines is " + to_string(Machine_GetTotal()) << std::endl;
    SimOutput("Scheduler::Init(): Initializing scheduler", 1);

    // for(unsigned i = 0; i < 8; i++)
    //     vms.push_back(VM_Create(LINUX, X86));
    // for(unsigned i = 8; i < 16; i++)
    //     vms.push_back(VM_Create(WIN, X86));
    // for (unsigned i = 16; i < 40; ++i) {
    //     vms.push_back(VM_Create(WIN, ARM));
    // }
    // for (unsigned i = 40; i < 48; ++i) {
    //     vms.push_back(VM_Create(AIX, POWER));
    // }


    cout << "Number of tasks: " << GetNumTasks() << endl;

    // for(unsigned i = 0; i < active_machines; i++)
    //     vms.push_back(VM_Create(WIN, Machine_GetCPUType(i)));

    for(unsigned i = 0; i < active_machines; i++) {
        machines.push_back(MachineId_t(i));
    }    
    // for(unsigned i = 0; i < active_machines; i++) {
    //     VM_Attach(vms[i], machines[i]);
    // }

    // Turn off the ARM machines
    // for(unsigned i = 24; i < Machine_GetTotal(); i++)
    //     Machine_SetState(MachineId_t(i), S5);

    // SimOutput("Scheduler::Init(): VM ids are " + to_string(vms[0]) + " and " + to_string(vms[1]), 3);
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    // Update your data structure. The VM now can receive new tasks
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    // cout << "making vm for task " << task_id << endl;
    bool gpu = IsTaskGPUCapable(task_id);
    unsigned int taskMem = GetTaskMemory(task_id) + VM_MEMORY_OVERHEAD;
    VMType_t vmType = RequiredVMType(task_id);
    CPUType_t cpu = RequiredCPUType(task_id);
    SLAType_t sla = RequiredSLA(task_id);
    
    // Decide to attach the task to an existing VM, 
    //      vm.AddTask(taskid, Priority_T priority); or

    // Create a new VM, attach the VM to a machine
    //      VM vm(type of the VM)
    //      vm.Attach(machine_id);
    //      vm.AddTask(taskid, Priority_t priority) or

    // Turn on a machine, create a new VM, attach it to the VM, then add the task
    //
    // Turn on a machine, migrate an existing VM from a loaded machine....
    //
    // Other possibilities as desired
    Priority_t priority = (task_id == 0 || task_id == 64)? HIGH_PRIORITY : MID_PRIORITY;
    // if(migrating) {
    //     VM_AddTask(vms[0], task_id, priority);
    // }
    // else {
    // unsigned int rand = 0;
    // if (vmType == AIX && cpu == POWER) {
    //     rand = std::rand() % 8 + 40;
    // } else if (vmType == LINUX && cpu == X86) {
    //     rand = std::rand() % 8;
    // } else if (vmType == WIN && cpu == X86) {
    //     rand = std::rand() % 8 + 8;
    // } else if (vmType == WIN && cpu == ARM) {
    //     rand = std::rand() % 24 + 16;
    // }
    // VM_AddTask(vms[rand], task_id, priority);

    // Greedy algorithm below
    VMId_t vm = VM_Create(vmType, cpu);
    vms.push_back(vm);
    bool added = false;
    for (unsigned int i = 0; i < active_machines && !added; i++) {
        // and also make sure it has enough memory lol
        MachineInfo_t info = Machine_GetInfo(machines[i]);
        unsigned remainingMem = info.memory_size - info.memory_used;

        
        SimOutput("Memory size:" + to_string(info.memory_size) + ", memory used: " + to_string(info.memory_used), 3);
        SimOutput("Current machine is: " + to_string(i), 3);
        
        if (Machine_GetCPUType(i) == cpu && remainingMem >= taskMem) {
            VM_Attach(vms[vm], machines[i]);
            VM_AddTask(vms[vm], task_id, priority);
            added = true;
        }
    }
    if (!added) {
        assert(false);
    }
}

void Scheduler::PeriodicCheck(Time_t now) {
    // This method should be called from SchedulerCheck()
    // SchedulerCheck is called periodically by the simulator to allow you to monitor, make decisions, adjustments, etc.
    // Unlike the other invocations of the scheduler, this one doesn't report any specific event
    // Recommendation: Take advantage of this function to do some monitoring and adjustments as necessary
    
    // for (unsigned i = 0; i < active_machines; ++i) {
    //     MachineInfo_t info = Machine_GetInfo(machines[i]);
    //     if (info.memory_used == 0) {
    //         // Turn off the machine
    //         Machine_SetState(machines[i], S5);
    //     }
    // }
}

void Scheduler::Shutdown(Time_t time) {
    // Do your final reporting and bookkeeping here.
    // Report about the total energy consumed
    // Report about the SLA compliance
    // Shutdown everything to be tidy :-)
    SimOutput("SimulationComplete(): Initiating shutdown...", 3);
    for(auto & vm: vms) {
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
    migrating = false;
}

void SchedulerCheck(Time_t time) {
    // This function is called periodically by the simulator, no specific event
    SimOutput("SchedulerCheck(): SchedulerCheck() called at " + to_string(time), 4);
    Scheduler.PeriodicCheck(time);
    static unsigned counts = 0;
    counts++;
    if(counts == 10) {
        migrating = true;
        VM_Migrate(1, 9);
    }
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
    
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    // Called in response to an earlier request to change the state of a machine
}

