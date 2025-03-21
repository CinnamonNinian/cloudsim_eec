// File that includes implementations of each algorithm. 
// These are used in Scheduler.cpp

#ifndef Algorithms_h
#define Algorithms_h

#include "SimTypes.h"
#include "Interfaces.h"

class Algorithms {
    public:

        /*
            Find machine implementations
        */
        // Greedy implementation of assigning tasks
        static MachineId_t Greedy_FindMachine(const vector<MachineId_t>& machines, bool prefer_gpu, unsigned int task_mem, CPUType_t cpu, VMType_t vm_type) {
            for (unsigned int i = 0; i < machines.size(); i++) {
                MachineInfo_t info = Machine_GetInfo(machines[i]);
                unsigned mem_left = info.memory_size - info.memory_used;
                if (Machine_GetCPUType(i) == cpu && mem_left >= task_mem) {
                    return i;
                }
            }
            return machines.size() + 1; // indicate failure to find machine
        }

        /*
            Task complete implementations
        */

        static void Greedy_TaskComplete(const vector<MachineId_t>& sorted, const vector<VMId_t>& vms) {
            for (unsigned i = sorted.size() - 1; i < sorted.size() && Machine_GetInfo(i).memory_used != 0; --i) {
                // I'm just purposely overflowing i because I know for a fact we are NOT Having integer_max machines
                MachineInfo_t inf = Machine_GetInfo(i);
                
                for (const auto& vm : vms) {
                    VMInfo_t vmInfo = VM_GetInfo(vm);
                    if (vmInfo.machine_id == i) {
                        for (unsigned j = 0; j < vmInfo.active_tasks.size(); ++j) {
                            TaskInfo_t tsk = GetTaskInfo(vmInfo.active_tasks[i]);
                            for (unsigned k = i + 1; k < sorted.size(); ++k) {
                                MachineInfo_t mchInf = Machine_GetInfo(sorted[k]);
                                unsigned remainingMem = mchInf.memory_size - mchInf.memory_used;
                                if (tsk.required_memory + VM_MEMORY_OVERHEAD <= remainingMem) {
                                    VM_Migrate(vm, k);
                                    break;
                                }
                            }
                        }
                    }
                }
        
                if (inf.active_vms == 0) {
                    Machine_SetState(i, S5);
                }
            }
        }

        /*
            SLA Violation Handing
        */

        static void Greedy_SLAViolation(const vector<MachineId_t>& sorted, const vector<VMId_t>& vms, TaskId_t task_id) {
            TaskInfo_t tskInf = GetTaskInfo(task_id);
            bool found = false;
            for (unsigned i = 0; i < sorted.size(); ++i) {
                MachineInfo_t mchInf = Machine_GetInfo(sorted[i]);
                unsigned remainingMem = mchInf.memory_size - mchInf.memory_used;
                if (tskInf.required_memory + VM_MEMORY_OVERHEAD <= remainingMem) {
                    for (const auto& vm : vms) {
                        VMInfo_t vmInf = VM_GetInfo(vm);
                        for (TaskId_t tsk : vmInf.active_tasks) {
                            if (tsk == task_id) {
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
};


#endif // Algorithms_hpp