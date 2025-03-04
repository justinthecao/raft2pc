import os
for i in range(1,10):
    try:
        os.remove(f"./saves/database{i}.txt")
    except:
        pass
    try:
        os.remove(f"./saves/database{i}.txt")
    except:
        pass
    try:
        os.remove(f"./saves/lastApp{i}.txt")
    except:
        pass
    try:
        os.remove(f"./saves/log{i}.txt")
    except:
        pass
    try:
        os.remove(f"./saves/locks{i}.txt")
    except:
        pass