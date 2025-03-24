//
//  Scheduler.hpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

#ifndef Scheduler_hpp
#define Scheduler_hpp

#include <vector>
#include <algorithm>

#include "Interfaces.h"

class Scheduler {
public:
    Scheduler()                 {}
    void Init();
    void MigrationComplete(Time_t time, VMId_t vm_id);
    void NewTask(Time_t now, TaskId_t task_id);
    MachineId_t FindMachine(AlgoType_t algo_type, bool prefer_gpu, unsigned int task_mem, CPUType_t cpu, VMType_t vm_type);
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
