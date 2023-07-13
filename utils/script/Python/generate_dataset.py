import sys
from sklearn.datasets import make_blobs
import pandas as pd
from sklearn.cluster import KMeans

if len(sys.argv) != 4:
    print("Usage: python script.py <n> <d> <k>")
    sys.exit(1)

samples = int(sys.argv[1])      # n: number of points into dataset
dimension = int(sys.argv[2])    # d: dimensionality of the point
centers = int(sys.argv[3])      # k: Cluster to be created

points, y = make_blobs(n_samples=samples, centers=centers, n_features=dimension, random_state=1, cluster_std=1.2,
                       shuffle=True)

df = pd.DataFrame(points)
df = df.apply(lambda x: (x - x.min()) / (x.max() - x.min()))
df.to_csv('dataset_test_100k_C4.txt', index=False, header=False)