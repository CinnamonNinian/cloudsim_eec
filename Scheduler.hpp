//
//  Scheduler.hpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

#ifndef Scheduler_hpp
#define Scheduler_hpp

#include <vector>
#include <map>
#include <algorithm>

#include "Interfaces.h"

class Scheduler {
public:
    Scheduler()                 {}
    void Init();
    Priority_t CalculatePriority(TaskId_t task_id);
    void MigrationComplete(Time_t time, VMId_t vm_id);
    void HandleStateChange(Time_t time, MachineId_t machine_id);
    void NewTask(Time_t now, TaskId_t task_id);
    bool FindMachine(bool prefer_gpu, unsigned int task_mem, CPUType_t cpu, VMType_t vm_type, MachineId_t& machine_id, bool require_memory);
    bool FindMachineForVM(VMInfo_t vm_info, MachineId_t& machine_id, bool require_running);
    void PeriodicCheck(Time_t now);
    void Shutdown(Time_t now);
    void TaskComplete(Time_t now, TaskId_t task_id);
    void HandleWarning(Time_t now, TaskId_t task_id);
private:
    vector<VMId_t> vms;
    vector<MachineId_t> machines;
};

bool compareMachines(MachineId_t lhs, MachineId_t rhs) {
    MachineInfo_t lhsInfo = Machine_GetInfo(lhs);
    MachineInfo_t rhsInfo = Machine_GetInfo(rhs);
    return lhsInfo.memory_used < rhsInfo.memory_used;
}

bool cmpMachinesEnergy(MachineId_t lhs, MachineId_t rhs) {
    MachineInfo_t lhsInfo = Machine_GetInfo(lhs);
    MachineInfo_t rhsInfo = Machine_GetInfo(rhs);
    return lhsInfo.energy_consumed < rhsInfo.energy_consumed;
}

#endif /* Scheduler_hpp */
