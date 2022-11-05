from typing import List

import time

import statistics as stats
from queue import PriorityQueue


### Configs
task_graph = 'fpppp' 


def stg_to_dag(filename = 'sparse'):
    import numpy as np
    no_tasks = 334
    try: 
        with open(filename, 'r') as f:
            line = f.readline() # no_tasks
            no_tasks = int(line)
    except FileNotFoundError:
        print("DataSet File Not Found... Works only with a specific dataset")
        print()
        print("Don't Worry. This error doesn't mean that file is broken,\nIt just means owner decided to not SHARE THE DATASET\nContact the owner for the dataset")
        exit()
    dag = {}
    _compcost = np.zeros(no_tasks+2, dtype=int)
    
    with open(filename, 'r') as f:
        _ = f.readline() # no_tasks
        line = f.readline()
        # while line:
        for _ in range(no_tasks):
            task, exec_time, deps_size, *deps = [int(i) for i in line.split()] # task, exec_time, deps_size, deps
            _compcost[task] = exec_time
            dag[task] = dag.get(task, ())
            if len(deps) != deps_size and task != 0:
                raise ValueError("Number of dependencies doesn't match with len(deps)! {} - {} {}".format(task, deps_size, deps))
            for d in deps:
                dag[d] = dag.get(d, ()) + (task,)
            line = f.readline()
    return dag, _compcost


# Set the computation costs of tasks and communication costs of edges with mean values.
# Compute rank_u for all tasks by traversing graph upward, starting from the exit task.
# Sort the tasks in a scheduling list by decreasing order of rank_u values.


# for running stg task graphs
low_perf_multiplier = 2
dag, _compcost = stg_to_dag(f'stg/{task_graph}')

def comm(a, b,A='a',B='b'):
    return 0

def comp(job, agent):
    return _compcost[job] * low_perf_multiplier if agent=='a' else _compcost[job]

compcost = comp
commcost = comm



class Task:
    def __init__(self, num):
        self.id = num
        self.processor = None
        self.ast = None     # Actual Start Time
        self.aft = None     # Actual Finish Time
        self.est = []       # Earliest execution Start Time
        self.eft = []       # Earliest execution Finish Time
        self.ranku = None
        self.rankd = None
        self.priority = None
        self.comp_cost = []
        self.avg_comp_cost = None
        self.successors = []
        self.predecessors = []

    def __lt__(self, other):
        return self.priority < other.priority



class Processor:
    def __init__(self, num):
        self.id = num
        self.tasks = []
        self.avail = 0      # processor ready time in a non-insertion based scheduling policy
    



def ranku(i, tasks):
# from task i to entry
    seq = [commcost(i, j) + ranku(j, tasks) for j in tasks[i].successors]
    if i==0: #first entry => last exit
        return 9999
    return tasks[i].avg_comp_cost + max(seq) if seq else tasks[i].avg_comp_cost


def rankd(i, tasks):
# From entry to task i
    if i==0:        # entry task
        return 0
    seq = [(rankd(j, tasks) + tasks[j].avg_comp_cost + commcost(j, i)) for j in tasks[i].predecessors]
    return max(seq)


def est(i: int, p: int, tasks: List[int], processors: List[int]):
    """Earliest execution Start Time Task i on Processor p"""
    if i==0:        # entry task
        return 0
    seq = [tasks[m].aft + commcost(m, i, tasks[m].processor, p) + commcost(i, m, tasks[m].processor, p) for m in tasks[i].predecessors]
    ready_time = max(seq)
    return max([ready_time, processors[p].avail])

def eft(i: int, p: int, tasks: List[int], processors: List[int]):
    """Calculate Earliest execution Finish Time for task i on processor p"""
    return compcost(i, chr(97+p)) + est(i, p, tasks, processors)


def makespan(tasks):
    seq = [t.aft for t in tasks]
    # logging.debug(seq)
    return max(seq)


def assign(i:int, p:int, tasks:List[int], processors: List[int]):
    processors[p].tasks.append(tasks[i])
    tasks[i].processor = p
    tasks[i].ast = est(tasks[i].id, p, tasks, processors)
    tasks[i].aft = eft(tasks[i].id, p, tasks, processors)
    processors[p].avail = tasks[i].aft




if __name__ == "__main__":
    # Create Processors
    P = 4
    processors = [Processor(i) for i in range(P)]
    # Create Tasks
    N = len(dag) 
    tasks = [Task(i) for i in range(N)]
    start = time.time()
    for t, succ in dag.items():
        tasks[t].successors = list(succ)
        agents = ''.join([chr(97+i) for i in range(P)]) # e.g., 'abc'
        tasks[t].comp_cost = [compcost(t, p) for p in agents]
        tasks[t].avg_comp_cost = stats.mean(tasks[t].comp_cost)
        for x in succ:
            tasks[x].predecessors.append(t)
        # setup entry task (id=0)
        tasks[0].avg_comp_cost = 0
        # if task_graph not in stags:
        #     tasks[0].successors = [1]
        #     tasks[1].predecessors = [0]

    # Calculate ranku by traversing task graph upward
    for task in reversed(tasks):
        task.ranku = round(ranku(task.id, tasks), 3)

    # Calculate Rankd by traversing task graph upward
    for task in tasks:
        task.rankd = round(rankd(task.id, tasks), 3)


    # Calculate Priority
    for task in tasks:
        task.priority = task.rankd + task.ranku

    _cp_ = tasks[1].priority
    CP = {tasks[1],}
    # Construct Critical-Path (CP)
    selected = tasks[1]
    while selected.id != N-1:
        pr = [tasks[t].priority for t in selected.successors]
        i = pr.index(max(pr))
        CP.add(tasks[selected.successors[i]])
        selected = tasks[selected.successors[i]]


    # Select the CP-Processor
    pcp = [0] * P
    for t in CP:
        for p in range(P):
            pcp[p] += compcost(t.id, chr(97+p))
    cp_processor = pcp.index(min(pcp))


    # Initialize Priority Queue
    tasks[0].ast = 0
    tasks[0].aft = 0
    q = PriorityQueue()
    q.put((-tasks[0].priority, tasks[0]))
    order = []
    while not q.empty():
        task = q.get()[1]
        order.append(task.id)
        if task in CP:
            # Assign the task to the CP-Processor
            assign(task.id, cp_processor, tasks, processors)
        else:
            seq = [eft(task.id, p, tasks, processors) for p in range(P)]
            p = seq.index(min(seq))
            assign(task.id, p, tasks, processors)
        # Update the Priority Queue with successors of task if they become ready tasks
        for s in task.successors:
            if None not in [(tasks[p].processor) for p in tasks[s].predecessors]:
                q.put((-tasks[s].priority, tasks[s]))
    end = time.time()
    print(f"Time Taken {end-start}")
    print(order)
    print('Makespan: ', makespan(tasks))