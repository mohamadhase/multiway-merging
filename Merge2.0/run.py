import time
from Merge import Merge
if __name__ == "__main__":
    st = time.time()
    m = Merge(path="data/",blocksize=250)
    et = time.time()
    print(et-st)
