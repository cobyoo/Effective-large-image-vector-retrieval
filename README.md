# Effective Retrieval Method for Feature Vector of Large Images Feature Vector


![image](https://user-images.githubusercontent.com/39446946/187832185-3ea83367-18bd-4cfd-87ec-a7ae60111c2f.png)


## Index
- [Experiment Environment](#Experiment-Environment)
- [Performance Evaluation](#Performance-Evaluation)

## Experiment Environment
    Master 1 Unit Node
       - OS : Ubuntu 20.04 LTS
       - SSD : Samsung SSD 980 PRO 2TB 
       - CPU : Intel(R) Xeon(R) Silver 4214R CPU @ 2.40GHz
       - Core : 48
       - Memory : DDR4, 128GB
       - Spark :  3.1.1
       - Hadoop : 3.2.2
        
    Worker 3 Unit Node
       - OS : Ubuntu 20.04 LTS
       - SSD : Samsung SSD 980 PRO 1TB 
       - CPU : Intel(R) Xeon(R) Silver 4210 CPU @ 2.40GHz
       - Core : 120
       - Memory : DDR4, 500GB
       - Spark :  3.1.1
       - Hadoop : 3.2.2

## Performance Evaluation
    Speed comparison according to the number of image feature vectors
