# Test case idea: two sets of machines that run the same CPU type. 
# one set consumes a lot more energy but runs tasks faster and has a GPU.

# No GPU Machines
machine class:
{
        Number of machines: 24
        Number of cores: 16
        CPU type: X86
        Memory: 32768
        S-States: [120, 100, 100, 80, 40, 10, 0]
        P-States: [12, 8, 6, 4]
        C-States: [12, 3, 1, 0]
        MIPS: [1000, 800, 600, 400]
        GPUs: no
}

# GPU Machines
machine class:
{
        Number of machines: 24
        CPU type: X86
        Number of cores: 32
        Memory: 65536
        S-States: [400, 300, 200, 80, 40, 10, 0]
        P-States: [200, 100, 50, 20]
        C-States: [100, 50, 20, 0]
        MIPS: [10000, 8000, 6000, 4000]
        GPUs: yes
}
task class:
{
        Start time: 0
        End time : 10000
        Inter arrival: 10
        Expected runtime: 100000
        Memory: 128
        VM type: WIN
        GPU enabled: yes
        SLA type: SLA1
        CPU type: X86
        Task type: CRYPTO
        Seed: 23354
}
task class:
{
        Start time: 5000
        End time : 5000000
        Inter arrival: 1000
        Expected runtime: 50000
        Memory: 32
        VM type: WIN
        GPU enabled: no
        SLA type: SLA0
        CPU type: X86
        Task type: HPC
        Seed: 453
}
