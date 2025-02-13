import os
for i in range(1,10):
    os.remove(f"./saves/database{i}.txt")
    os.remove(f"./saves/lastApp{i}.txt")
    os.remove(f"./saves/log{i}.txt")