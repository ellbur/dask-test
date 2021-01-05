
def step_collatz(n):
    k = n
    for _ in range(1000*1000):
        if k == 1: return 0
        elif k % 2 == 0: k //= 2
        else: k = k*3 + 1
    else:
        return 1

def step_collatz_range(n1, n2):
    return sum(step_collatz(n) for n in range(n1, n2))

def do_work(i):
    return step_collatz_range(i*1000*1000, (i+1)*1000*1000)

if __name__ == '__main__':
    from dask_cloudprovider.aws import FargateCluster
    from distributed import Client, LocalCluster
    
    use_fargate = False
    cluster_class = FargateCluster if use_fargate else LocalCluster
    
    with cluster_class() as cluster:
        if use_fargate:
            cluster.adapt(minimum=0, maximum=30)
            
        with Client(cluster) as client:
            
            rs = sum(client.gather(client.map(do_work, range(1))))
            print(rs)

