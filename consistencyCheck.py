import os
import sys

import filecmp



for i in range(0,3):
    file1 = f"./saves/database{1 + 3*i}.txt"
    file2 = f"./saves/database{2 + 3*i}.txt"
    file3 = f"./saves/database{3 + 3*i}.txt"

    try:
        if filecmp.cmp(file1, file2, shallow=False):
            print(f"{file1} and {file2} are identical")
        else:
            print(f"{file1} and {file2} are different")
    except:
        ...
    try:
        if filecmp.cmp(file2, file3, shallow=False):
            print(f"{file2} and {file3} are identical")
        else:
            print(f"{file2} and {file3} are different")
    except:
        ...
    try:
            
        if filecmp.cmp(file1, file3, shallow=False):
            print(f"{file1} and {file3} are identical")
        else:
            print(f"{file1} and {file3} are different")
    except:
        ...


for i in range(0,3):
    file1 = f"./saves/log{1 + 3*i}.txt"
    file2 = f"./saves/log{2 + 3*i}.txt"
    file3 = f"./saves/log{3 + 3*i}.txt"

    try:
        if filecmp.cmp(file1, file2, shallow=False):
            print(f"{file1} and {file2} are identical")
        else:
            print(f"{file1} and {file2} are different")
    except:
        ...
    try:
        if filecmp.cmp(file2, file3, shallow=False):
            print(f"{file2} and {file3} are identical")
        else:
            print(f"{file2} and {file3} are different")
    except:
        ...
    try:
            
        if filecmp.cmp(file1, file3, shallow=False):
            print(f"{file1} and {file3} are identical")
        else:
            print(f"{file1} and {file3} are different")
    except:
        ...


for i in range(0,3):
    file1 = f"./saves/lastApp{1 + 3*i}.txt"
    file2 = f"./saves/lastApp{2 + 3*i}.txt"
    file3 = f"./saves/lastApp{3 + 3*i}.txt"

    try:
        if filecmp.cmp(file1, file2, shallow=False):
            print(f"{file1} and {file2} are identical")
        else:
            print(f"{file1} and {file2} are different")
    except:
        ...
    try:
        if filecmp.cmp(file2, file3, shallow=False):
            print(f"{file2} and {file3} are identical")
        else:
            print(f"{file2} and {file3} are different")
    except:
        ...
    try:
            
        if filecmp.cmp(file1, file3, shallow=False):
            print(f"{file1} and {file3} are identical")
        else:
            print(f"{file1} and {file3} are different")
    except:
        ...