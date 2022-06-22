import sys
counter = {}
inputfile = sys.stdin
for line in inputfile:
    line = line.strip()
    if line in counter:
        counter[line] += 1
    else:
        counter[line] = 1
for key, val in counter.items():
    print(f'слово {key} встретилось {val} раз')
