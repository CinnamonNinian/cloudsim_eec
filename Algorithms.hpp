// File that includes implementations of each algorithm. 
// These are used in Scheduler.cpp

#ifndef Algorithms_h
#define Algorithms_h

#include <algorithm>

#include "SimTypes.h"
#include "Interfaces.h"

class Algorithms {
    public:

        /*
            Find machine implementations
        */
        // Greedy implementation of finding a machine for assigning tasks
        static pair<MachineId_t, VMId_t> Greedy_FindMachine(const vector<MachineId_t>& machines, bool prefer_gpu, unsigned int task_mem, CPUType_t cpu, VMType_t vm_type) {
            for (unsigned int i = 0; i < machines.size(); i++) {
                MachineInfo_t info = Machine_GetInfo(machines[i]);
                unsigned mem_left = info.memory_size - info.memory_used;
                if (Machine_GetCPUType(i) == cpu && mem_left >= task_mem) {
                    return {machines[i], -1};
                }
            }
            return {machines.size() + 1, -1}; // indicate failure to find machine
        }
        
        // pMapper implementation of finding a machine for assigning tasks
        // static pair<MachineId_t, VMId_t> PMapper_FindMachine(const vector<MachineId_t>& machines, bool prefer_gpu, unsigned int task_mem, CPUType_t cpu, VMType_t vm_type) {
        //     for (unsigned int i = 0; i < machines.size(); i++) {
        //         MachineInfo_t info = Machine_GetInfo(machines[i]);
        //         unsigned mem_left = info.memory_size - info.memory_used;
        //         if (Machine_GetCPUType(i) == cpu && mem_left >= task_mem) {
        //             return {i, -1};
        //         }
        //     }
        //     return {machines.size() + 1, -1};
        // }

        /*
            Find machine implementations
        */
        // Our implementation of finding a machine for assigning tasks
        // In order of importance:
        // 1. Machine has CPU that can handle task and enough memory left to accept task (non-negotiable)
        // 2. Machine has GPU capabilities
        // 3. Machine has a virtual machine of the same type
        // 4. Machine is already running
        static pair<MachineId_t, VMId_t> Our_FindMachine(const vector<MachineId_t>& machines, const vector<vector<VMId_t>>& vms_per_machine, bool prefer_gpu, unsigned int task_mem, CPUType_t cpu, VMType_t vm_type) {
            MachineId_t best_id = machines.size() + 1;
            // variables about the best machine so far
            bool best_has_gpu = !prefer_gpu;
            bool best_has_vm_type = false;
            bool best_is_running = false;

            for (unsigned int i = 0; i < machines.size(); i++) {
                if (best_has_gpu && best_has_vm_type && best_is_running) break;

                MachineInfo_t info = Machine_GetInfo(machines[i]);
                unsigned mem_left = info.memory_size - info.memory_used;
                auto vms = vms_per_machine[i];
                bool has_vm_type = find_if(vms.begin(), vms.end(), [&vm_type](VMId_t id) {
                    return VM_GetInfo(id).vm_type == vm_type;
                }) != vms.end();
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
            VMId_t vm_id = VMId_t(-1);
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

        /*
            Task complete implementations
        */

        static void Greedy_TaskComplete(const vector<MachineId_t>& machines, const vector<VMId_t>& vms) {
            for (unsigned i = machines.size() - 1; i < machines.size() && Machine_GetInfo(i).memory_used != 0; --i) {
                // I'm just purposely overflowing i because I know for a fact we are NOT Having integer_max machines
                MachineInfo_t inf = Machine_GetInfo(i);
                
                for (const auto& vm : vms) {
                    VMInfo_t vmInfo = VM_GetInfo(vm);
                    if (vmInfo.machine_id == i) {
                        for (unsigned j = 0; j < vmInfo.active_tasks.size(); ++j) {
                            TaskInfo_t tsk = GetTaskInfo(vmInfo.active_tasks[i]);
                            for (unsigned k = i + 1; k < machines.size(); ++k) {
                                MachineInfo_t mchInf = Machine_GetInfo(machines[k]);
                                unsigned remainingMem = mchInf.memory_size - mchInf.memory_used;
                                if (tsk.required_memory + VM_MEMORY_OVERHEAD <= remainingMem && tsk.required_cpu == Machine_GetCPUType(machines[k])) {
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

        static void PMapper_TaskComplete(const vector<MachineId_t>& machines, const vector<VMId_t>& vms, const vector<vector<VMId_t>>& vms_per_machine) {
            // assume already sorted from lowest util to highest

            unsigned mid = machines.size() / 2;
            TaskId_t smallest = 0;
            unsigned smallestMemReq = 4294967294;
            VMId_t smallestVM = 0;

            vector<VMId_t> listVMs = vms_per_machine[machines[0]];

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

            for (unsigned i = mid + 1; i < machines.size(); ++i) {
                MachineInfo_t mchInf = Machine_GetInfo(machines[i]);
                unsigned remainingMem = mchInf.memory_size - mchInf.memory_used;
                if (smallestMemReq <= remainingMem && mchInf.cpu == tskInfo.required_cpu) {
                    cout << "Machine: " << machines[i] << endl;
                    cout << "smallest vm: " << smallestVM << endl;
                    cout << "this is smallestmemreq: " << smallestMemReq << endl;
                    cout << "this is remainingMem: " << remainingMem << endl;
                    VM_Migrate(smallestVM, machines[i]);
                    break;
                }
            }
        }

        static void Our_TaskComplete(const vector<MachineId_t>& machines, const vector<VMId_t>& vms) {
                // ...
        }
        

        /*
            SLA Violation Handing
        */

        static void Greedy_SLAViolation(const vector<MachineId_t>& sorted, const vector<VMId_t>& vms, TaskId_t task_id, CPUType_t cpu) {
            TaskInfo_t tskInf = GetTaskInfo(task_id);
            bool found = false;
            for (unsigned i = 0; i < sorted.size(); ++i) {
                MachineInfo_t mchInf = Machine_GetInfo(sorted[i]);
                unsigned remainingMem = mchInf.memory_size - mchInf.memory_used;
                if (tskInf.required_memory + VM_MEMORY_OVERHEAD <= remainingMem && Machine_GetCPUType(sorted[i]) == cpu) {
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
