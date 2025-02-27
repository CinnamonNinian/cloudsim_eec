machine class:
{
# comment
        Number of machines: 16
        CPU type: X86
        Number of cores: 8
        Memory: 16384
        S-States: [120, 100, 100, 80, 40, 10, 0]
        P-States: [12, 8, 6, 4]
        C-States: [12, 3, 1, 0]
        MIPS: [1000, 800, 600, 400]
        GPUs: yes
}
machine class:
{
        Number of machines: 24
        Number of cores: 16
        CPU type: ARM
        Memory: 16384
        S-States: [120, 100, 100, 80, 40, 10, 0]
        P-States: [12, 8, 6, 4]
        C-States: [12, 3, 1, 0]
        MIPS: [1000, 800, 600, 400]
        GPUs: yes
}
machine class:
{
        Number of machines: 8
        Number of cores: 4
        CPU type: POWER
        Memory: 65536
        S-States: [120, 100, 100, 80, 40, 10, 0]
        P-States: [12, 8, 6, 4]
        C-States: [12, 3, 1, 0]
        MIPS: [1000, 800, 600, 400]
        GPUs: yes
}
task class:
{
        Start time: 60000
        End time : 800000
        Inter arrival: 60000
        Expected runtime: 2000000
        Memory: 8
        VM type: LINUX
        GPU enabled: no
        SLA type: SLA0
        CPU type: X86
        Task type: WEB
        Seed: 520230
}
task class:
{
        Start time: 0
        End time : 500000
        Inter arrival: 200000
        Expected runtime: 100000
        Memory: 2
        VM type: AIX
        GPU enabled: no
        SLA type: SLA1
        CPU type: POWER
        Task type: WEB
        Seed: 520288
}
task class:
{
        Start time: 200000
        End time : 1000000
        Inter arrival: 120000
        Expected runtime: 90000
        Memory: 100
        VM type: WIN
        GPU enabled: yes
        SLA type: SLA1
        CPU type: ARM
        Task type: CRYPTO
        Seed: 0
}


