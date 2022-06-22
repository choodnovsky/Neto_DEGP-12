import sys

counter = {}  # заводим пустой словарь
inputfile = sys.stdin  # принимаем данные из консоли
for line in inputfile:
    line = line.strip()  # отбрасываем пробельные символы с краев
    if line in counter:  # если такой ключ в словаре уже есть, прибавляем 1
        counter[line] += 1
    else:  # иначе создаем новый ключ
        counter[line] = 1
for key, val in counter.items():
    print(f'слово {key} встретилось {val} раз')  # выводим подсчеты слов на печать